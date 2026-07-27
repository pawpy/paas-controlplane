package controller

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	paasv1 "github.com/pawpy/paas-controlplane/api/v1alpha1"
)

// cnpgClusterGVK is the CloudNativePG Cluster type. Referenced as unstructured so
// this operator does not take a build dependency on the CNPG module.
var cnpgClusterGVK = schema.GroupVersionKind{Group: "postgresql.cnpg.io", Version: "v1", Kind: "Cluster"}

// obcGVK is the ObjectBucketClaim type (Rook's bucket provisioner). Unstructured
// for the same reason: no build dependency on the lib-bucket-provisioner module.
var obcGVK = schema.GroupVersionKind{Group: "objectbucket.io", Version: "v1alpha1", Kind: "ObjectBucketClaim"}

// scheduledBackupGVK is the CNPG ScheduledBackup type (drives periodic base
// backups; unstructured, no module dependency like cnpgClusterGVK).
var scheduledBackupGVK = schema.GroupVersionKind{Group: "postgresql.cnpg.io", Version: "v1", Kind: "ScheduledBackup"}

// pgImageRepo is the mirrored CloudNativePG postgres image (barman-cloud built in),
// pinned in registry.local by apps/image-mirror. pgDefaultVersion is used when a
// postgres backing does not pin a version. Keep in lockstep with the mirror list.
const (
	pgImageRepo      = "registry.local/cloudnative-pg/postgresql"
	pgDefaultVersion = "17.2"
)

// StackReconciler drives a Stack toward: backing data services (self-hosted via
// operators), then for each service a release Job (if it has a release hook) run
// to completion, then a Deployment per process (+ a Service for ported
// processes). Typed connections resolve to an internal URL (intra-stack) or to
// credentials injected from the backing service's Secret.
type StackReconciler struct {
	client.Client
	Scheme               *runtime.Scheme
	TolerateControlPlane bool
	// Builtins is the compiled-in template-tier catalog (see servicedef.go).
	Builtins *serviceCatalog
	// SystemNamespace is where the optional paas-servicedefs ConfigMap lives.
	SystemNamespace string
	// Tier is the overcommit pool for tenant workloads (dev/prod/build). Requests
	// are derived from limits by its ratios. See overcommit.go.
	Tier overcommit
	// SchedulerName routes tenant pods to a named scheduler (Trimaran usage-aware
	// bin-packer); empty uses the default kube-scheduler.
	SchedulerName string
	// ObjectStorageClass is the bucket-provisioner StorageClass for `object`
	// backing (Ceph RGW via Rook); ObjectEndpoint is the in-cluster S3 URL.
	ObjectStorageClass string
	ObjectEndpoint     string
}

// backing is a provisioned data service: whether it is ready to serve, and how a
// connection to it turns into container env for a consumer.
type backing struct {
	ready bool
	// env returns the env vars to inject for a connection `as` this name.
	env func(as string) []corev1.EnvVar
}

// +kubebuilder:rbac:groups=paas.sh,resources=stacks,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=paas.sh,resources=stacks/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=apps,resources=deployments;statefulsets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=services;secrets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=postgresql.cnpg.io,resources=clusters;scheduledbackups;backups,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=objectbucket.io,resources=objectbucketclaims,verbs=get;list;watch;create;update;patch;delete

func (r *StackReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	l := log.FromContext(ctx)

	var stack paasv1.Stack
	if err := r.Get(ctx, req.NamespacedName, &stack); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// 1) Provision backing services and learn how to bind connections to them.
	backings, backupPending, err := r.reconcileBacking(ctx, &stack)
	if err != nil {
		return ctrl.Result{}, err
	}

	internalURLs := serviceURLs(&stack)
	releasePending, releaseFailed, backingPending := false, false, false

	for i := range stack.Spec.Services {
		svc := &stack.Spec.Services[i]
		if svc.Image == "" {
			l.Info("service has no image, skipping until build is wired", "service", svc.Name)
			continue
		}
		image := normalizeImage(svc.Image)

		// Resolve connections into env. Intra-stack service edges become an
		// internal URL; backing edges become credentials from the backing
		// Secret. A service whose backing is not ready yet is held back.
		svcEnv := envFromSpec(svc.Env)
		waitBacking := false
		for _, c := range svc.Connections {
			if url, ok := internalURLs[c.To]; ok {
				if c.As != "" {
					svcEnv = append(svcEnv, corev1.EnvVar{Name: c.As, Value: url})
				}
				continue
			}
			if b, ok := backings[c.To]; ok {
				if !b.ready {
					waitBacking = true
					continue
				}
				if c.As != "" {
					svcEnv = append(svcEnv, b.env(c.As)...)
				}
				continue
			}
			l.Info("connection target not found", "service", svc.Name, "to", c.To)
		}
		if waitBacking {
			backingPending = true
			continue // hold the service (and its migration) until the DB is up
		}

		// Release hook gates the rollout: run it to completion first. It gets the
		// resolved connection env, so migrations reach the (now-ready) database.
		if svc.Hooks != nil && svc.Hooks.Release != "" {
			done, failed, err := r.reconcileReleaseJob(ctx, &stack, svc, image, svcEnv)
			if err != nil {
				return ctrl.Result{}, err
			}
			if failed {
				releaseFailed = true
				continue
			}
			if !done {
				releasePending = true
				continue
			}
		}

		for j := range svc.Processes {
			p := &svc.Processes[j]
			if err := r.reconcileProcess(ctx, &stack, svc, p, image, svcEnv); err != nil {
				return ctrl.Result{}, fmt.Errorf("service %s process %s: %w", svc.Name, p.Name, err)
			}
		}
	}

	return r.updateStatus(ctx, &stack, backingPending, releasePending, releaseFailed, backupPending)
}

// reconcileBacking ensures each backing service exists and reports readiness +
// how to bind it. It dispatches across the three catalog tiers:
//   - OPERATOR: a small hand-written adapter drives a best-in-class operator
//     (postgres -> CloudNativePG). Reserved for data-heavy/HA engines.
//   - TEMPLATE: a data-only ServiceDefinition interpreted by reconcileTemplated.
//     Adding one is a descriptor, not code (see servicedef.go).
//   - FALLBACK: an uncataloged type with a user-supplied image runs as a bare
//     StatefulSet so the long tail still works, without managed features.
// reconcileBacking also reports backupPending: a backing that has asked for
// backups but whose object-storage bucket has not bound yet. The controller does
// not watch the OBC/CNPG Cluster, so without a requeue the barman wiring (set only
// once the bucket is Bound) would never land after the first pass. The caller
// requeues while this is true.
func (r *StackReconciler) reconcileBacking(ctx context.Context, stack *paasv1.Stack) (map[string]backing, bool, error) {
	l := log.FromContext(ctx)
	catalog := r.catalog(ctx)
	out := map[string]backing{}
	backupPending := false
	for i := range stack.Spec.Backing {
		b := &stack.Spec.Backing[i]
		switch {
		case strings.EqualFold(b.Type, "postgres") || strings.EqualFold(b.Type, "postgresql"):
			ready, secret, bp, err := r.reconcilePostgres(ctx, stack, b)
			if err != nil {
				return nil, false, err
			}
			backupPending = backupPending || bp
			s := secret
			out[b.Name] = backing{ready: ready, env: func(as string) []corev1.EnvVar { return postgresEnv(s, as) }}
		case strings.EqualFold(b.Type, "object") || strings.EqualFold(b.Type, "s3"):
			ready, name, err := r.reconcileObject(ctx, stack, b)
			if err != nil {
				return nil, false, err
			}
			n := name
			out[b.Name] = backing{ready: ready, env: func(as string) []corev1.EnvVar { return r.objectEnv(n, as) }}
		default:
			if def := catalog.lookup(b.Type); def != nil {
				ready, secret, err := r.reconcileTemplated(ctx, stack, b, def)
				if err != nil {
					return nil, false, err
				}
				d, s := def, secret
				out[b.Name] = backing{ready: ready, env: func(as string) []corev1.EnvVar { return templatedEnv(d, s, as) }}
				continue
			}
			if b.Image != "" {
				ready, secret, err := r.reconcileFallback(ctx, stack, b)
				if err != nil {
					return nil, false, err
				}
				s := secret
				out[b.Name] = backing{ready: ready, env: func(as string) []corev1.EnvVar { return fallbackEnv(s, as) }}
				continue
			}
			l.Info("backing type not in catalog and no image for fallback (see backing-services catalog)", "name", b.Name, "type", b.Type)
		}
	}
	return out, backupPending, nil
}

// reconcilePostgres ensures a CloudNativePG Cluster for a postgres backing and
// returns readiness + the app Secret name. The image is the mirrored CNPG postgres
// (registry.local, via apps/image-mirror). When the backing opts into backups it
// also provisions a dedicated object-storage bucket and wires continuous WAL +
// scheduled base backups to it (barman-cloud, in-tree in CNPG 1.30) — the same
// backup that a PR-preview environment clones from (M5c).
func (r *StackReconciler) reconcilePostgres(ctx context.Context, stack *paasv1.Stack, b *paasv1.BackingSpec) (ready bool, secretName string, backupPending bool, err error) {
	l := log.FromContext(ctx)
	name := fmt.Sprintf("%s-%s", stack.Name, b.Name)
	disk := b.Disk
	if disk == "" {
		disk = "1Gi"
	}
	version := b.Version
	if version == "" {
		version = pgDefaultVersion
	}

	// Clone (M5c-2): a preview environment seeds this postgres from a parent
	// environment's barman backup via CNPG bootstrap.recovery. The Environment
	// controller stamps the parent coordinates as annotations on the Stack. Only a
	// backed-up parent can be cloned (recovery needs a base backup + WAL).
	parentNs, parentCluster, isClone := cloneSource(stack, b)

	// Bootstrap (initdb vs recovery) is honoured only at creation and is immutable
	// afterward, so decide it once. A pre-Get tells us whether this is the first
	// creation; inside the mutate the empty UID is the authoritative signal.
	clExisting := &unstructured.Unstructured{}
	clExisting.SetGroupVersionKind(cnpgClusterGVK)
	clusterExists := r.Get(ctx, types.NamespacedName{Namespace: stack.Namespace, Name: name}, clExisting) == nil

	// Before first creation of a clone, wait until the parent has a recoverable
	// base backup, then copy its read creds into this namespace. Requeue (via
	// backupPending) until then so the clone actually seeds rather than racing to
	// an empty database.
	cloneBucket, cloneCreds := "", ""
	if isClone && !clusterExists {
		recoverable, bucket, gerr := r.parentRecoverable(ctx, parentNs, parentCluster)
		if gerr != nil {
			return false, "", false, gerr
		}
		if !recoverable {
			l.Info("clone source not yet recoverable, requeuing",
				"cluster", name, "parent", parentNs+"/"+parentCluster)
			return false, name + "-app", true, nil
		}
		cloneCreds = name + "-clone-src"
		if err = r.copyBackupCreds(ctx, stack, parentNs, parentCluster+"-backup", cloneCreds); err != nil {
			return false, "", false, fmt.Errorf("copy clone creds: %w", err)
		}
		cloneBucket = bucket
	}

	// Own backups: a persistent backed-up postgres gets its own bucket + WAL +
	// scheduled base backup (the clone source for previews). A preview clone is
	// ephemeral: it does not archive, it only reads the parent bucket.
	backupBucket, backupReady := "", false
	wantOwnBackup := b.Backup && !isClone
	if wantOwnBackup {
		// WAL archiving needs its creds Secret to exist before the cluster
		// references it, so hold off wiring barmanObjectStore until the OBC is
		// Bound (Secret + ConfigMap present). The cluster comes up meanwhile.
		var berr error
		backupReady, backupBucket, berr = r.ensureBucket(ctx, stack, name+"-backup")
		if berr != nil {
			return false, "", false, berr
		}
	}

	cl := &unstructured.Unstructured{}
	cl.SetGroupVersionKind(cnpgClusterGVK)
	cl.SetNamespace(stack.Namespace)
	cl.SetName(name)
	if _, err = controllerutil.CreateOrUpdate(ctx, r.Client, cl, func() error {
		// Set only the fields we own so CNPG's spec defaulting is preserved
		// (wholesale spec replacement would fight the operator and thrash).
		_ = unstructured.SetNestedField(cl.Object, int64(1), "spec", "instances")
		_ = unstructured.SetNestedField(cl.Object, disk, "spec", "storage", "size")
		_ = unstructured.SetNestedField(cl.Object, "ceph-block", "spec", "storage", "storageClass")
		_ = unstructured.SetNestedField(cl.Object, pgImageRepo+":"+version, "spec", "imageName")

		// Bootstrap is immutable after creation, so set it only on first create
		// (empty UID). Clone -> physical recovery from the parent's external barman
		// store; otherwise a fresh initdb database.
		if cl.GetUID() == "" {
			if isClone && cloneBucket != "" {
				_ = unstructured.SetNestedField(cl.Object, parentCluster, "spec", "bootstrap", "recovery", "source")
				_ = unstructured.SetNestedSlice(cl.Object, []interface{}{
					map[string]interface{}{
						"name": parentCluster,
						"barmanObjectStore": map[string]interface{}{
							"destinationPath": "s3://" + cloneBucket,
							"endpointURL":     r.ObjectEndpoint,
							// serverName is the barman folder the parent wrote under
							// (defaults to the parent cluster name); must match or
							// recovery finds no backup.
							"serverName": parentCluster,
							"s3Credentials": map[string]interface{}{
								"accessKeyId":     map[string]interface{}{"name": cloneCreds, "key": "AWS_ACCESS_KEY_ID"},
								"secretAccessKey": map[string]interface{}{"name": cloneCreds, "key": "AWS_SECRET_ACCESS_KEY"},
							},
							"wal": map[string]interface{}{"compression": "gzip"},
						},
					},
				}, "spec", "externalClusters")
			} else {
				_ = unstructured.SetNestedField(cl.Object, "app", "spec", "bootstrap", "initdb", "database")
				_ = unstructured.SetNestedField(cl.Object, "app", "spec", "bootstrap", "initdb", "owner")
			}
		}

		if backupReady {
			_ = unstructured.SetNestedMap(cl.Object, map[string]interface{}{
				"destinationPath": "s3://" + backupBucket,
				"endpointURL":     r.ObjectEndpoint,
				"s3Credentials": map[string]interface{}{
					"accessKeyId":     map[string]interface{}{"name": name + "-backup", "key": "AWS_ACCESS_KEY_ID"},
					"secretAccessKey": map[string]interface{}{"name": name + "-backup", "key": "AWS_SECRET_ACCESS_KEY"},
				},
				"wal": map[string]interface{}{"compression": "gzip"},
				// immediateCheckpoint forces a fast checkpoint at backup start instead
				// of the default "spread" one, which on an idle DB waits up to
				// checkpoint_timeout (5m) before the base backup makes progress.
				"data": map[string]interface{}{"compression": "gzip", "immediateCheckpoint": true},
			}, "spec", "backup", "barmanObjectStore")
			_ = unstructured.SetNestedField(cl.Object, "30d", "spec", "backup", "retentionPolicy")
		}
		// Single converged box: CNPG's Postgres pods must tolerate the
		// control-plane taint (CNPG places tolerations under spec.affinity).
		if r.TolerateControlPlane {
			_ = unstructured.SetNestedSlice(cl.Object, []interface{}{
				map[string]interface{}{
					"key":      "node-role.kubernetes.io/control-plane",
					"operator": "Exists",
					"effect":   "NoSchedule",
				},
			}, "spec", "affinity", "tolerations")
		}
		return controllerutil.SetControllerReference(stack, cl, r.Scheme)
	}); err != nil {
		return false, "", false, fmt.Errorf("cnpg cluster %s: %w", name, err)
	}

	// A ScheduledBackup (immediate + daily) gives a base backup to archive against
	// and to clone previews from. Only once the bucket is wired.
	if backupReady {
		if err = r.ensureScheduledBackup(ctx, stack, name); err != nil {
			return false, "", false, err
		}
	}

	readyInstances, _, _ := unstructured.NestedInt64(cl.Object, "status", "readyInstances")
	// backupPending: own backups were requested but the bucket has not bound yet,
	// so barman is not wired. Requeue until it binds (nothing else re-triggers us).
	return readyInstances >= 1, name + "-app", wantOwnBackup && !backupReady, nil
}

// cloneSource reads the clone coordinates the Environment controller stamps on a
// preview's Stack. Clone applies only to postgres backings that opt into backups
// (recovery needs a parent base backup + WAL). Returns the parent namespace and
// the parent cluster name for this backing (parentStack-<backing>).
func cloneSource(stack *paasv1.Stack, b *paasv1.BackingSpec) (parentNs, parentCluster string, ok bool) {
	if !b.Backup {
		return "", "", false
	}
	ns := stack.Annotations["paas.sh/clone-from-namespace"]
	parentStack := stack.Annotations["paas.sh/clone-from-stack"]
	if ns == "" || parentStack == "" {
		return "", "", false
	}
	return ns, fmt.Sprintf("%s-%s", parentStack, b.Name), true
}

// parentRecoverable reports whether the parent CNPG cluster has a base backup that
// can seed a clone, and returns the parent backup bucket name. Recoverability is
// signalled by status.firstRecoverabilityPoint (set once a PITR-usable base backup
// + WAL exist); the bucket name comes from the OBC-generated ConfigMap. A missing
// parent cluster/ConfigMap is "not yet recoverable" (requeue), not an error.
func (r *StackReconciler) parentRecoverable(ctx context.Context, ns, cluster string) (recoverable bool, bucket string, err error) {
	pc := &unstructured.Unstructured{}
	pc.SetGroupVersionKind(cnpgClusterGVK)
	if e := r.Get(ctx, types.NamespacedName{Namespace: ns, Name: cluster}, pc); e != nil {
		return false, "", client.IgnoreNotFound(e)
	}
	frp, _, _ := unstructured.NestedString(pc.Object, "status", "firstRecoverabilityPoint")
	byMethod, _, _ := unstructured.NestedMap(pc.Object, "status", "firstRecoverabilityPointByMethod")
	if frp == "" && len(byMethod) == 0 {
		return false, "", nil
	}
	var cm corev1.ConfigMap
	if e := r.Get(ctx, types.NamespacedName{Namespace: ns, Name: cluster + "-backup"}, &cm); e != nil {
		return false, "", client.IgnoreNotFound(e)
	}
	return cm.Data["BUCKET_NAME"] != "", cm.Data["BUCKET_NAME"], nil
}

// copyBackupCreds mirrors the parent's backup-creds Secret (the OBC-generated
// AWS_* keys) into this Stack's namespace so a clone can read the parent bucket.
// Owned by the Stack, so it is GC'd with the preview namespace at teardown.
func (r *StackReconciler) copyBackupCreds(ctx context.Context, stack *paasv1.Stack, srcNs, srcName, dstName string) error {
	var src corev1.Secret
	if err := r.Get(ctx, types.NamespacedName{Namespace: srcNs, Name: srcName}, &src); err != nil {
		return err
	}
	dst := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: dstName, Namespace: stack.Namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, dst, func() error {
		if dst.Data == nil {
			dst.Data = map[string][]byte{}
		}
		dst.Data["AWS_ACCESS_KEY_ID"] = src.Data["AWS_ACCESS_KEY_ID"]
		dst.Data["AWS_SECRET_ACCESS_KEY"] = src.Data["AWS_SECRET_ACCESS_KEY"]
		return controllerutil.SetControllerReference(stack, dst, r.Scheme)
	})
	return err
}

// ensureScheduledBackup drives a daily base backup for a CNPG cluster (immediate:
// true so the first one runs right away — the recovery/clone base). Named after
// the cluster, owned by the Stack.
func (r *StackReconciler) ensureScheduledBackup(ctx context.Context, stack *paasv1.Stack, clusterName string) error {
	sb := &unstructured.Unstructured{}
	sb.SetGroupVersionKind(scheduledBackupGVK)
	sb.SetNamespace(stack.Namespace)
	sb.SetName(clusterName)
	if _, err := controllerutil.CreateOrUpdate(ctx, r.Client, sb, func() error {
		// CNPG cron is 6-field (leading seconds): 3am daily.
		_ = unstructured.SetNestedField(sb.Object, "0 0 3 * * *", "spec", "schedule")
		_ = unstructured.SetNestedField(sb.Object, true, "spec", "immediate")
		_ = unstructured.SetNestedField(sb.Object, "self", "spec", "backupOwnerReference")
		_ = unstructured.SetNestedField(sb.Object, clusterName, "spec", "cluster", "name")
		return controllerutil.SetControllerReference(stack, sb, r.Scheme)
	}); err != nil {
		return fmt.Errorf("scheduledbackup %s: %w", clusterName, err)
	}
	return nil
}

// postgresEnv binds a postgres connection: `as` gets the full connection URI, and
// the discrete PG* vars are injected too (both sourced from the CNPG app Secret,
// by reference, so credentials never land in the Deployment spec).
func postgresEnv(secret, as string) []corev1.EnvVar {
	return []corev1.EnvVar{
		{Name: as, ValueFrom: secretRef(secret, "uri")},
		{Name: "PGHOST", ValueFrom: secretRef(secret, "host")},
		{Name: "PGPORT", ValueFrom: secretRef(secret, "port")},
		{Name: "PGDATABASE", ValueFrom: secretRef(secret, "dbname")},
		{Name: "PGUSER", ValueFrom: secretRef(secret, "user")},
		{Name: "PGPASSWORD", ValueFrom: secretRef(secret, "password")},
	}
}

// reconcileObject provisions S3 object storage: an ObjectBucketClaim that Rook's
// bucket provisioner turns into a bucket in the platform Ceph RGW plus a Secret
// (access/secret keys) and ConfigMap (bucket name/region), both named after the
// OBC in the tenant namespace. This is the platform tier: we run the S3 backend
// (Ceph RGW), not a managed cloud bucket. Ready = OBC phase Bound.
func (r *StackReconciler) reconcileObject(ctx context.Context, stack *paasv1.Stack, b *paasv1.BackingSpec) (ready bool, name string, err error) {
	name = fmt.Sprintf("%s-%s", stack.Name, b.Name)
	ready, _, err = r.ensureBucket(ctx, stack, name)
	return ready, name, err
}

// ensureBucket creates/updates an ObjectBucketClaim and reports whether it is
// Bound plus the generated bucket name. Rook names the claim's creds Secret and
// its ConfigMap (which carries BUCKET_NAME) after the OBC, in the stack namespace.
// Used both for `object` backings (the app reads creds by ref) and for CNPG backup
// buckets (the controller needs the concrete bucket name for the barman path).
func (r *StackReconciler) ensureBucket(ctx context.Context, stack *paasv1.Stack, obcName string) (bound bool, bucketName string, err error) {
	obc := &unstructured.Unstructured{}
	obc.SetGroupVersionKind(obcGVK)
	obc.SetNamespace(stack.Namespace)
	obc.SetName(obcName)
	if _, err = controllerutil.CreateOrUpdate(ctx, r.Client, obc, func() error {
		// generateBucketName gives a unique bucket per claim; the real name lands
		// in the generated ConfigMap's BUCKET_NAME.
		_ = unstructured.SetNestedField(obc.Object, obcName, "spec", "generateBucketName")
		_ = unstructured.SetNestedField(obc.Object, r.ObjectStorageClass, "spec", "storageClassName")
		return controllerutil.SetControllerReference(stack, obc, r.Scheme)
	}); err != nil {
		return false, "", fmt.Errorf("objectbucketclaim %s: %w", obcName, err)
	}
	if phase, _, _ := unstructured.NestedString(obc.Object, "status", "phase"); phase != "Bound" {
		return false, "", nil
	}
	// The generated ConfigMap (same name as the OBC) carries BUCKET_NAME; it lands
	// right after the OBC binds, so a not-found here is just a race — requeue.
	var cm corev1.ConfigMap
	if err = r.Get(ctx, types.NamespacedName{Namespace: stack.Namespace, Name: obcName}, &cm); err != nil {
		return false, "", client.IgnoreNotFound(err)
	}
	return true, cm.Data["BUCKET_NAME"], nil
}

// objectEnv binds an object connection. Credentials come by Secret reference and
// the bucket/region by ConfigMap reference (both generated by the OBC, same name);
// the endpoint is the in-cluster RGW URL. AWS_* are also set so S3 SDKs that read
// the standard env pick them up with no config.
func (r *StackReconciler) objectEnv(name, as string) []corev1.EnvVar {
	cmRef := func(key string) *corev1.EnvVarSource {
		return &corev1.EnvVarSource{ConfigMapKeyRef: &corev1.ConfigMapKeySelector{
			LocalObjectReference: corev1.LocalObjectReference{Name: name}, Key: key,
		}}
	}
	// RGW is region-agnostic, and Rook leaves the OBC's BUCKET_REGION blank, but
	// many S3 SDKs error without a region set, so pin a conventional default.
	const region = "us-east-1"
	out := []corev1.EnvVar{}
	if as != "" && as != "S3_ENDPOINT" {
		out = append(out, corev1.EnvVar{Name: as, Value: r.ObjectEndpoint})
	}
	return append(out,
		corev1.EnvVar{Name: "S3_ENDPOINT", Value: r.ObjectEndpoint},
		corev1.EnvVar{Name: "S3_BUCKET", ValueFrom: cmRef("BUCKET_NAME")},
		corev1.EnvVar{Name: "S3_REGION", Value: region},
		corev1.EnvVar{Name: "AWS_DEFAULT_REGION", Value: region},
		corev1.EnvVar{Name: "S3_ACCESS_KEY_ID", ValueFrom: secretRef(name, "AWS_ACCESS_KEY_ID")},
		corev1.EnvVar{Name: "S3_SECRET_ACCESS_KEY", ValueFrom: secretRef(name, "AWS_SECRET_ACCESS_KEY")},
		corev1.EnvVar{Name: "AWS_ACCESS_KEY_ID", ValueFrom: secretRef(name, "AWS_ACCESS_KEY_ID")},
		corev1.EnvVar{Name: "AWS_SECRET_ACCESS_KEY", ValueFrom: secretRef(name, "AWS_SECRET_ACCESS_KEY")},
	)
}

// reconcileFallback runs an uncataloged backing service from a user-supplied image
// as a bare StatefulSet + headless Service + a Secret carrying host/port/endpoint.
// This is the long-tail tier-0: the container runs and is reachable, but it gets no
// generated credentials and no managed features. A service earns those by graduating
// to a TEMPLATE descriptor (servicedef.go) and then, if it needs HA/backups, to an
// OPERATOR adapter.
func (r *StackReconciler) reconcileFallback(ctx context.Context, stack *paasv1.Stack, b *paasv1.BackingSpec) (ready bool, secretName string, err error) {
	name := fmt.Sprintf("%s-%s", stack.Name, b.Name)
	labels := map[string]string{"paas.sh/stack": stack.Name, "paas.sh/backing": b.Name}
	host := fmt.Sprintf("%s.%s.svc.cluster.local", name, stack.Namespace)
	port := b.Port
	if port == 0 {
		return false, "", fmt.Errorf("fallback backing %q needs a port", b.Name)
	}

	sec := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: stack.Namespace}}
	if _, err = controllerutil.CreateOrUpdate(ctx, r.Client, sec, func() error {
		if sec.Data == nil {
			sec.Data = map[string][]byte{}
		}
		sec.Data["host"] = []byte(host)
		sec.Data["port"] = []byte(strconv.Itoa(int(port)))
		sec.Data["endpoint"] = []byte(fmt.Sprintf("%s:%d", host, port))
		return controllerutil.SetControllerReference(stack, sec, r.Scheme)
	}); err != nil {
		return false, "", fmt.Errorf("fallback secret: %w", err)
	}

	svc := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: stack.Namespace}}
	if _, err = controllerutil.CreateOrUpdate(ctx, r.Client, svc, func() error {
		svc.Labels = labels
		svc.Spec.ClusterIP = corev1.ClusterIPNone
		svc.Spec.Selector = labels
		svc.Spec.Ports = []corev1.ServicePort{{Name: "svc", Port: port, TargetPort: intstr.FromInt32(port)}}
		return controllerutil.SetControllerReference(stack, svc, r.Scheme)
	}); err != nil {
		return false, "", fmt.Errorf("fallback service: %w", err)
	}

	disk := b.Disk
	if disk == "" {
		disk = "1Gi"
	}
	replicas := int32(1)
	ss := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: stack.Namespace}}
	if _, err = controllerutil.CreateOrUpdate(ctx, r.Client, ss, func() error {
		ss.Labels = labels
		ss.Spec.ServiceName = name
		ss.Spec.Replicas = &replicas
		ss.Spec.Selector = &metav1.LabelSelector{MatchLabels: labels}
		ss.Spec.PersistentVolumeClaimRetentionPolicy = &appsv1.StatefulSetPersistentVolumeClaimRetentionPolicy{
			WhenDeleted: appsv1.DeletePersistentVolumeClaimRetentionPolicyType,
		}
		ss.Spec.Template.ObjectMeta.Labels = labels
		ss.Spec.Template.Spec.AutomountServiceAccountToken = ptr(false)
		ss.Spec.Template.Spec.Tolerations = r.tolerations()
		applyScheduler(&ss.Spec.Template.Spec, r.SchedulerName)
		ss.Spec.Template.Spec.Containers = []corev1.Container{{
			Name:            "backing",
			Image:           normalizeImage(b.Image),
			ImagePullPolicy: corev1.PullIfNotPresent,
			Ports:           []corev1.ContainerPort{{ContainerPort: port, Name: "svc"}},
			Resources:       fixedResources("500m", "512Mi", r.Tier),
			VolumeMounts:    []corev1.VolumeMount{{Name: "data", MountPath: "/data"}},
			SecurityContext: hardenedSecurityContext(),
		}}
		ss.Spec.VolumeClaimTemplates = []corev1.PersistentVolumeClaim{{
			ObjectMeta: metav1.ObjectMeta{Name: "data"},
			Spec: corev1.PersistentVolumeClaimSpec{
				AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
				StorageClassName: ptr("ceph-block"),
				Resources:        corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceStorage: resourceQty(disk)}},
			},
		}}
		return controllerutil.SetControllerReference(stack, ss, r.Scheme)
	}); err != nil {
		return false, "", fmt.Errorf("fallback statefulset: %w", err)
	}

	return ss.Status.ReadyReplicas >= 1, name, nil
}

// fallbackEnv binds a fallback backing: the `as` env gets "host:port"; discrete
// FALLBACK_HOST/FALLBACK_PORT are provided too. The consumer wires the rest.
func fallbackEnv(secret, as string) []corev1.EnvVar {
	out := []corev1.EnvVar{}
	if as != "" {
		out = append(out, corev1.EnvVar{Name: as, ValueFrom: secretRef(secret, "endpoint")})
	}
	return append(out,
		corev1.EnvVar{Name: "FALLBACK_HOST", ValueFrom: secretRef(secret, "host")},
		corev1.EnvVar{Name: "FALLBACK_PORT", ValueFrom: secretRef(secret, "port")},
	)
}

func secretRef(name, key string) *corev1.EnvVarSource {
	return &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{
		LocalObjectReference: corev1.LocalObjectReference{Name: name}, Key: key,
	}}
}

// randomPassword returns hex (not base64url): hex is safe inside a redis:// URL
// with no special/leading-dash characters that trip URL parsers.
func randomPassword() string {
	b := make([]byte, 24)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func resourceQty(s string) resource.Quantity { return resource.MustParse(s) }

// fixedResources is the ceiling for a backing service; requests are tier-derived
// (limit/overcommit) so the pool overcommits. See overcommit.go.
func fixedResources(cpuLimit, memLimit string, t overcommit) corev1.ResourceRequirements {
	return t.limitedResources(resource.MustParse(cpuLimit), resource.MustParse(memLimit))
}

// reconcileReleaseJob ensures the per-generation release Job exists and reports
// whether it is done/failed. A new generation re-runs the migration.
func (r *StackReconciler) reconcileReleaseJob(ctx context.Context, stack *paasv1.Stack, svc *paasv1.ServiceSpec, image string, env []corev1.EnvVar) (done, failed bool, err error) {
	name := fmt.Sprintf("%s-%s-release-g%d", stack.Name, svc.Name, stack.Generation)
	var job batchv1.Job
	err = r.Get(ctx, types.NamespacedName{Namespace: stack.Namespace, Name: name}, &job)
	if apierrors.IsNotFound(err) {
		job = r.buildReleaseJob(stack, svc, image, name, env)
		if err := controllerutil.SetControllerReference(stack, &job, r.Scheme); err != nil {
			return false, false, err
		}
		if err := r.Create(ctx, &job); err != nil {
			return false, false, err
		}
		return false, false, nil
	}
	if err != nil {
		return false, false, err
	}
	for _, c := range job.Status.Conditions {
		if c.Status != corev1.ConditionTrue {
			continue
		}
		switch c.Type {
		case batchv1.JobComplete:
			return true, false, nil
		case batchv1.JobFailed:
			return false, true, nil
		}
	}
	return false, false, nil
}

func (r *StackReconciler) buildReleaseJob(stack *paasv1.Stack, svc *paasv1.ServiceSpec, image, name string, env []corev1.EnvVar) batchv1.Job {
	backoff := int32(2)
	return batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: stack.Namespace,
			Labels:    map[string]string{"paas.sh/stack": stack.Name, "paas.sh/service": svc.Name, "paas.sh/hook": "release"},
		},
		Spec: batchv1.JobSpec{
			BackoffLimit: &backoff,
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					RestartPolicy:                corev1.RestartPolicyNever,
					AutomountServiceAccountToken: ptr(false),
					Tolerations:                  r.tolerations(),
					SchedulerName:                r.SchedulerName,
					Containers: []corev1.Container{{
						Name:            "release",
						Image:           image,
						ImagePullPolicy: corev1.PullIfNotPresent,
						Command:         shCommand(svc.Hooks.Release),
						Env:             env,
						SecurityContext: hardenedSecurityContext(),
					}},
				},
			},
		},
	}
}

// reconcileProcess creates/updates the Deployment (+ Service, if the process has
// a port) for one process of a service.
func (r *StackReconciler) reconcileProcess(ctx context.Context, stack *paasv1.Stack, svc *paasv1.ServiceSpec, p *paasv1.ProcessSpec, image string, svcEnv []corev1.EnvVar) error {
	name := fmt.Sprintf("%s-%s-%s", stack.Name, svc.Name, p.Name)
	labels := map[string]string{"paas.sh/stack": stack.Name, "paas.sh/service": svc.Name, "paas.sh/process": p.Name}
	replicas := p.Replicas
	if replicas == 0 {
		replicas = 1
	}

	dep := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: stack.Namespace}}
	if _, err := controllerutil.CreateOrUpdate(ctx, r.Client, dep, func() error {
		dep.Labels = labels
		dep.Spec.Replicas = &replicas
		dep.Spec.Selector = &metav1.LabelSelector{MatchLabels: labels}
		dep.Spec.Template.ObjectMeta.Labels = labels
		dep.Spec.Template.Spec.AutomountServiceAccountToken = ptr(false)
		dep.Spec.Template.Spec.Tolerations = r.tolerations()
		applyScheduler(&dep.Spec.Template.Spec, r.SchedulerName)
		c := corev1.Container{
			Name:            p.Name,
			Image:           image,
			ImagePullPolicy: corev1.PullIfNotPresent,
			Env:             processEnv(svcEnv, p.Port),
			Resources:       resources(p.Resources, r.Tier),
			SecurityContext: hardenedSecurityContext(),
		}
		if p.Command != "" {
			c.Command = shCommand(p.Command)
		}
		if p.Port > 0 {
			c.Ports = []corev1.ContainerPort{{ContainerPort: p.Port, Name: "http"}}
		}
		dep.Spec.Template.Spec.Containers = []corev1.Container{c}
		return controllerutil.SetControllerReference(stack, dep, r.Scheme)
	}); err != nil {
		return fmt.Errorf("deployment: %w", err)
	}

	if p.Port <= 0 {
		return nil
	}
	svcObj := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: stack.Namespace}}
	if _, err := controllerutil.CreateOrUpdate(ctx, r.Client, svcObj, func() error {
		svcObj.Labels = labels
		svcObj.Spec.Selector = labels
		svcObj.Spec.Ports = []corev1.ServicePort{{Name: "http", Port: 80, TargetPort: intstr.FromInt32(p.Port)}}
		return controllerutil.SetControllerReference(stack, svcObj, r.Scheme)
	}); err != nil {
		return fmt.Errorf("service: %w", err)
	}
	return nil
}

func (r *StackReconciler) updateStatus(ctx context.Context, stack *paasv1.Stack, backingPending, releasePending, releaseFailed, backupPending bool) (ctrl.Result, error) {
	var deps appsv1.DeploymentList
	if err := r.List(ctx, &deps, client.InNamespace(stack.Namespace), client.MatchingLabels{"paas.sh/stack": stack.Name}); err != nil {
		return ctrl.Result{}, err
	}
	var total, ready int32
	for i := range deps.Items {
		total++
		if deps.Items[i].Status.ReadyReplicas >= 1 {
			ready++
		}
	}

	stack.Status.Services = int32(len(stack.Spec.Services))
	stack.Status.ReadyServices = ready
	stack.Status.ObservedGeneration = stack.Generation
	switch {
	case releaseFailed:
		stack.Status.ReleaseHook = "Failed"
		stack.Status.Phase = "ReleaseFailed"
	case backingPending:
		stack.Status.Phase = "ProvisioningBacking"
	case releasePending:
		stack.Status.ReleaseHook = "Running"
		stack.Status.Phase = "ReleaseHook"
	default:
		if hasReleaseHook(stack) {
			stack.Status.ReleaseHook = "Complete"
		} else {
			stack.Status.ReleaseHook = "None"
		}
		if total > 0 && ready == total {
			stack.Status.Phase = "Running"
		} else {
			stack.Status.Phase = "Progressing"
		}
	}
	if err := r.Status().Update(ctx, stack); err != nil {
		return ctrl.Result{}, err
	}
	// Poll while backing is provisioning, or while a backup bucket is still binding
	// (neither the CNPG Cluster nor the OBC is Owns-watched, so nothing else wakes
	// us to finish wiring barman once the bucket is Bound).
	if backingPending || backupPending {
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}
	return ctrl.Result{}, nil
}

func (r *StackReconciler) tolerations() []corev1.Toleration {
	if !r.TolerateControlPlane {
		return nil
	}
	return []corev1.Toleration{{
		Key:      "node-role.kubernetes.io/control-plane",
		Operator: corev1.TolerationOpExists,
		Effect:   corev1.TaintEffectNoSchedule,
	}}
}

func (r *StackReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&paasv1.Stack{}).
		Owns(&appsv1.Deployment{}).
		Owns(&corev1.Service{}).
		Owns(&batchv1.Job{}).
		Complete(r)
}

// --- helpers ---

// serviceURLs maps each service that exposes a port to its internal URL, so
// intra-stack connections resolve to a cluster address.
func serviceURLs(stack *paasv1.Stack) map[string]string {
	out := map[string]string{}
	for i := range stack.Spec.Services {
		svc := &stack.Spec.Services[i]
		for j := range svc.Processes {
			p := &svc.Processes[j]
			if p.Port > 0 {
				name := fmt.Sprintf("%s-%s-%s", stack.Name, svc.Name, p.Name)
				out[svc.Name] = fmt.Sprintf("http://%s.%s.svc.cluster.local", name, stack.Namespace)
				break
			}
		}
	}
	return out
}

func hasReleaseHook(stack *paasv1.Stack) bool {
	for i := range stack.Spec.Services {
		if stack.Spec.Services[i].Hooks != nil && stack.Spec.Services[i].Hooks.Release != "" {
			return true
		}
	}
	return false
}

func shCommand(cmd string) []string { return []string{"/bin/sh", "-c", cmd} }

func hardenedSecurityContext() *corev1.SecurityContext {
	return &corev1.SecurityContext{
		AllowPrivilegeEscalation: ptr(false),
		Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
	}
}

// processEnv prepends PORT (when the process has one) to the service env.
func processEnv(svcEnv []corev1.EnvVar, port int32) []corev1.EnvVar {
	if port <= 0 {
		return svcEnv
	}
	out := make([]corev1.EnvVar, 0, len(svcEnv)+1)
	out = append(out, corev1.EnvVar{Name: "PORT", Value: fmt.Sprintf("%d", port)})
	return append(out, svcEnv...)
}

// envFromSpec converts the Stack's plain name/value env into container env.
func envFromSpec(in []paasv1.EnvVar) []corev1.EnvVar {
	out := make([]corev1.EnvVar, 0, len(in))
	for _, e := range in {
		out = append(out, corev1.EnvVar{Name: e.Name, Value: e.Value})
	}
	return out
}
