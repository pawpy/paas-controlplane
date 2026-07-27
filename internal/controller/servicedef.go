package controller

import (
	"bytes"
	"context"
	"embed"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"text/template"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/yaml"

	paasv1 "github.com/pawpy/paas-controlplane/api/v1alpha1"
)

// This file implements the TEMPLATE tier of the backing-services catalog as DATA,
// not code. A template-backed service is described by a ServiceDefinition (a YAML
// document), and one generic reconciler (reconcileTemplated) interprets it. Adding
// a new template service means adding a descriptor, never writing Go.
//
// Descriptors come from two places, merged (ConfigMap wins):
//  1. builtin/*.yaml, compiled in (the shipped catalog);
//  2. an optional ConfigMap `paas-servicedefs` in the control-plane namespace, so
//     the platform team can add/override services by GitOps with no controller
//     rebuild.
//
// If a service needs anything this schema cannot express (clustering, failover,
// bespoke bootstrap, sidecars), it does not belong in the template tier: it graduates
// to an OPERATOR adapter (a small hand-written reconcile<Type>, e.g. reconcilePostgres).
// Uncataloged types with a user-supplied image fall to the generic FALLBACK path.

//go:embed builtin/*.yaml
var builtinServiceDefs embed.FS

// ServiceDefinition is the declarative description of a template-backed service.
type ServiceDefinition struct {
	// Type is the primary backing[].type; Aliases are accepted synonyms.
	Type    string   `json:"type"`
	Aliases []string `json:"aliases,omitempty"`

	// Image is a Go template over {{.Version}} (e.g. "valkey/valkey:{{.Version}}-alpine").
	Image          string `json:"image"`
	DefaultVersion string `json:"defaultVersion,omitempty"`

	// Port is the server port (container port + Service port).
	Port     int32  `json:"port"`
	PortName string `json:"portName,omitempty"`

	// Command/Args for the container. Each element is $(VAR)-expanded by the kubelet
	// from ContainerEnv, so args like "--requirepass" "$(VALKEY_PASSWORD)" work.
	Command []string `json:"command,omitempty"`
	Args    []string `json:"args,omitempty"`

	// GeneratePassword mints a stable random password, exposed to templates as
	// {{.Password}} and stored in the generated Secret under key "password".
	GeneratePassword bool `json:"generatePassword,omitempty"`

	// ContainerEnv injects env into the server container from the generated Secret
	// (envName -> secretKey). Typically used to feed the password to the process.
	ContainerEnv map[string]string `json:"containerEnv,omitempty"`

	// Persistence, if set, mounts a Ceph RBD data PVC at MountPath.
	Persistence *PersistenceSpec `json:"persistence,omitempty"`

	// Security overrides the pod SecurityContext. Some images need a specific uid so
	// their entrypoint skips a privileged step (chown/su-exec) that drop-ALL-caps blocks.
	Security *PodSecuritySpec `json:"security,omitempty"`

	// Resources ceiling for the server container.
	Resources ResourceSpec `json:"resources,omitempty"`

	// SecretValues are templated over {{.Host}} {{.Port}} {{.Password}} and written
	// into the generated Secret. This is where the connection URL/DSN is composed.
	// The reconciler always also writes "host" and "port" (and "password" if generated).
	SecretValues map[string]string `json:"secretValues,omitempty"`

	// Binding maps a connection to consumer env. PrimaryKey is the secret key the
	// edge's `as` env receives (the URL/DSN); Extra are discrete vars (envName -> secretKey).
	Binding BindingSpec `json:"binding"`
}

type PersistenceSpec struct {
	MountPath   string `json:"mountPath"`
	DefaultDisk string `json:"defaultDisk,omitempty"`
}

type PodSecuritySpec struct {
	RunAsUser    *int64 `json:"runAsUser,omitempty"`
	RunAsGroup   *int64 `json:"runAsGroup,omitempty"`
	FSGroup      *int64 `json:"fsGroup,omitempty"`
	RunAsNonRoot *bool  `json:"runAsNonRoot,omitempty"`
}

type ResourceSpec struct {
	CPULimit    string `json:"cpuLimit,omitempty"`
	MemoryLimit string `json:"memoryLimit,omitempty"`
}

type BindingSpec struct {
	// PrimaryKey is the secret key delivered under the connection's `as` env (URL/DSN).
	PrimaryKey string `json:"primaryKey"`
	// Extra are discrete vars: consumer envName -> secret key.
	Extra map[string]string `json:"extra,omitempty"`
}

// serviceCatalog is a type->definition index (aliases point at the same def).
type serviceCatalog struct {
	byType map[string]*ServiceDefinition
}

func (c *serviceCatalog) register(def *ServiceDefinition) {
	c.byType[strings.ToLower(def.Type)] = def
	for _, a := range def.Aliases {
		c.byType[strings.ToLower(a)] = def
	}
}

func (c *serviceCatalog) lookup(t string) *ServiceDefinition {
	if c == nil {
		return nil
	}
	return c.byType[strings.ToLower(t)]
}

// LoadBuiltinCatalog parses the compiled-in descriptors. A parse error here is a
// programming error (bad shipped descriptor), so it is fatal at startup.
func LoadBuiltinCatalog() (*serviceCatalog, error) {
	c := &serviceCatalog{byType: map[string]*ServiceDefinition{}}
	entries, err := builtinServiceDefs.ReadDir("builtin")
	if err != nil {
		return nil, err
	}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".yaml") {
			continue
		}
		data, err := builtinServiceDefs.ReadFile("builtin/" + e.Name())
		if err != nil {
			return nil, err
		}
		def, err := parseServiceDef(data)
		if err != nil {
			return nil, fmt.Errorf("builtin servicedef %s: %w", e.Name(), err)
		}
		c.register(def)
	}
	return c, nil
}

func parseServiceDef(data []byte) (*ServiceDefinition, error) {
	var def ServiceDefinition
	if err := yaml.UnmarshalStrict(data, &def); err != nil {
		return nil, err
	}
	if def.Type == "" || def.Image == "" || def.Port == 0 {
		return nil, fmt.Errorf("descriptor needs type, image and port")
	}
	if def.Binding.PrimaryKey == "" {
		return nil, fmt.Errorf("descriptor %s needs binding.primaryKey", def.Type)
	}
	return &def, nil
}

// catalog returns the builtin catalog overlaid with the paas-servicedefs ConfigMap
// (each data entry is one descriptor YAML). The overlay lets the platform team add
// or override template services by GitOps without rebuilding the controller. A
// missing ConfigMap is normal; a malformed entry is logged and skipped (it must not
// take down provisioning for every tenant).
func (r *StackReconciler) catalog(ctx context.Context) *serviceCatalog {
	l := log.FromContext(ctx)
	merged := &serviceCatalog{byType: map[string]*ServiceDefinition{}}
	for k, v := range r.Builtins.byType {
		merged.byType[k] = v
	}

	var cm corev1.ConfigMap
	err := r.Get(ctx, types.NamespacedName{Namespace: r.SystemNamespace, Name: "paas-servicedefs"}, &cm)
	if apierrors.IsNotFound(err) {
		return merged
	}
	if err != nil {
		l.Info("could not read paas-servicedefs ConfigMap, using builtin catalog only", "err", err.Error())
		return merged
	}
	for name, doc := range cm.Data {
		def, perr := parseServiceDef([]byte(doc))
		if perr != nil {
			l.Info("skipping malformed servicedef in ConfigMap", "entry", name, "err", perr.Error())
			continue
		}
		merged.register(def)
	}
	return merged
}

// reconcileTemplated provisions a template-backed service from its descriptor: a
// generated-password Secret (holding host/port/url/...), a headless Service, and a
// StatefulSet. It is the single interpreter for the whole template tier.
func (r *StackReconciler) reconcileTemplated(ctx context.Context, stack *paasv1.Stack, b *paasv1.BackingSpec, def *ServiceDefinition) (ready bool, secretName string, pending bool, err error) {
	name := fmt.Sprintf("%s-%s", stack.Name, b.Name)
	labels := map[string]string{"paas.sh/stack": stack.Name, "paas.sh/backing": b.Name}
	host := fmt.Sprintf("%s.%s.svc.cluster.local", name, stack.Namespace)
	port := def.Port
	portName := orDefault(def.PortName, def.Type)

	version := orDefault(b.Version, def.DefaultVersion)
	image, err := renderTemplate(def.Image, map[string]string{"Version": version})
	if err != nil {
		return false, "", false, fmt.Errorf("%s image template: %w", def.Type, err)
	}

	// 1) Secret: password minted once (if any), then host/port and the templated
	//    SecretValues (url/dsn/servers/...) re-derived every reconcile so format
	//    fixes propagate to existing secrets.
	sec := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: stack.Namespace}}
	if _, err = controllerutil.CreateOrUpdate(ctx, r.Client, sec, func() error {
		if sec.Data == nil {
			sec.Data = map[string][]byte{}
		}
		if def.GeneratePassword && len(sec.Data["password"]) == 0 {
			sec.Data["password"] = []byte(randomPassword())
		}
		pw := string(sec.Data["password"])
		sec.Data["host"] = []byte(host)
		sec.Data["port"] = []byte(strconv.Itoa(int(port)))
		tv := map[string]string{"Host": host, "Port": strconv.Itoa(int(port)), "Password": pw}
		for k, tmpl := range def.SecretValues {
			v, rerr := renderTemplate(tmpl, tv)
			if rerr != nil {
				return fmt.Errorf("secretValues[%s]: %w", k, rerr)
			}
			sec.Data[k] = []byte(v)
		}
		return controllerutil.SetControllerReference(stack, sec, r.Scheme)
	}); err != nil {
		return false, "", false, fmt.Errorf("%s secret: %w", def.Type, err)
	}

	// 2) Headless Service.
	svc := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: stack.Namespace}}
	if _, err = controllerutil.CreateOrUpdate(ctx, r.Client, svc, func() error {
		svc.Labels = labels
		svc.Spec.ClusterIP = corev1.ClusterIPNone
		svc.Spec.Selector = labels
		svc.Spec.Ports = []corev1.ServicePort{{Name: portName, Port: port, TargetPort: intstr.FromInt32(port)}}
		return controllerutil.SetControllerReference(stack, svc, r.Scheme)
	}); err != nil {
		return false, "", false, fmt.Errorf("%s service: %w", def.Type, err)
	}

	// 3) StatefulSet.
	cpuLimit := orDefault(def.Resources.CPULimit, "100m")
	memLimit := orDefault(def.Resources.MemoryLimit, "256Mi")
	replicas := int32(1)
	container := corev1.Container{
		Name:            def.Type,
		Image:           image,
		ImagePullPolicy: corev1.PullIfNotPresent,
		Command:         def.Command,
		Args:            def.Args,
		Env:             containerEnvFrom(def.ContainerEnv, name),
		Ports:           []corev1.ContainerPort{{ContainerPort: port, Name: portName}},
		Resources:       fixedResources(cpuLimit, memLimit, r.Tier),
		SecurityContext: hardenedSecurityContext(),
	}
	if def.Persistence != nil {
		container.VolumeMounts = []corev1.VolumeMount{{Name: "data", MountPath: def.Persistence.MountPath}}
	}

	// Clone (M5c-3): a preview seeds a persistent (PVC-backed) template backing from
	// the parent via CSI snapshot restore. Only on first creation (the PVC
	// dataSource is immutable); until the restore snapshot is ReadyToUse, requeue
	// without creating the StatefulSet. Stateless template backings (no persistence,
	// e.g. memcached/nats) have nothing to clone and just come up fresh.
	var cloneDataSource *corev1.TypedLocalObjectReference
	if def.Persistence != nil && isPreviewClone(stack) && !r.statefulSetExists(ctx, stack.Namespace, name) {
		vs, vready, cerr := r.prepareCloneRestore(ctx, stack, b.Name)
		if cerr != nil {
			return false, "", false, cerr
		}
		if !vready {
			return false, name, true, nil
		}
		cloneDataSource = snapshotDataSource(vs)
	}

	ss := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: stack.Namespace}}
	if _, err = controllerutil.CreateOrUpdate(ctx, r.Client, ss, func() error {
		ss.Labels = labels
		ss.Spec.ServiceName = name
		ss.Spec.Replicas = &replicas
		ss.Spec.Selector = &metav1.LabelSelector{MatchLabels: labels}
		ss.Spec.Template.ObjectMeta.Labels = labels
		ss.Spec.Template.Spec.AutomountServiceAccountToken = ptr(false)
		ss.Spec.Template.Spec.Tolerations = r.tolerations()
		applyScheduler(&ss.Spec.Template.Spec, r.SchedulerName)
		ss.Spec.Template.Spec.SecurityContext = podSecurityContext(def.Security)
		ss.Spec.Template.Spec.Containers = []corev1.Container{container}
		// VolumeClaimTemplates are immutable after creation, so build them only on
		// create (empty UID); the clone dataSource likewise applies once.
		if def.Persistence != nil && ss.UID == "" {
			disk := orDefault(b.Disk, orDefault(def.Persistence.DefaultDisk, "1Gi"))
			ss.Spec.PersistentVolumeClaimRetentionPolicy = &appsv1.StatefulSetPersistentVolumeClaimRetentionPolicy{
				WhenDeleted: appsv1.DeletePersistentVolumeClaimRetentionPolicyType,
			}
			ss.Spec.VolumeClaimTemplates = []corev1.PersistentVolumeClaim{{
				ObjectMeta: metav1.ObjectMeta{Name: "data"},
				Spec: corev1.PersistentVolumeClaimSpec{
					AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
					StorageClassName: ptr("ceph-block"),
					DataSource:       cloneDataSource,
					Resources:        corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceStorage: resourceQty(disk)}},
				},
			}}
		}
		return controllerutil.SetControllerReference(stack, ss, r.Scheme)
	}); err != nil {
		return false, "", false, fmt.Errorf("%s statefulset: %w", def.Type, err)
	}

	// Backup (M5c-3): a persistent, backed-up, non-preview backing gets its PVC
	// labelled so the snapscheduler SnapshotSchedule snapshots it. Best-effort: a
	// snapshot-infra hiccup must not fail app provisioning.
	if def.Persistence != nil && b.Backup && !isPreviewClone(stack) {
		if lerr := r.labelBackupPVC(ctx, stack, b.Name); lerr != nil {
			log.FromContext(ctx).Error(lerr, "label backup pvc", "backing", b.Name)
		}
	}

	return ss.Status.ReadyReplicas >= 1, name, false, nil
}

// templatedEnv binds a connection to a template-backed service: the `as` env gets
// the descriptor's primary key (URL/DSN), plus the discrete Extra vars, all by
// Secret reference (deterministic order so we do not thrash the consumer spec).
func templatedEnv(def *ServiceDefinition, secret, as string) []corev1.EnvVar {
	out := []corev1.EnvVar{}
	if as != "" {
		out = append(out, corev1.EnvVar{Name: as, ValueFrom: secretRef(secret, def.Binding.PrimaryKey)})
	}
	for _, envName := range sortedKeys(def.Binding.Extra) {
		out = append(out, corev1.EnvVar{Name: envName, ValueFrom: secretRef(secret, def.Binding.Extra[envName])})
	}
	return out
}

// containerEnvFrom turns a descriptor's ContainerEnv (envName -> secret key) into
// container env sourced from the generated Secret, in deterministic order.
func containerEnvFrom(m map[string]string, secret string) []corev1.EnvVar {
	out := make([]corev1.EnvVar, 0, len(m))
	for _, envName := range sortedKeys(m) {
		out = append(out, corev1.EnvVar{Name: envName, ValueFrom: secretRef(secret, m[envName])})
	}
	return out
}

func renderTemplate(tmpl string, data map[string]string) (string, error) {
	t, err := template.New("d").Option("missingkey=error").Parse(tmpl)
	if err != nil {
		return "", err
	}
	var buf bytes.Buffer
	if err := t.Execute(&buf, data); err != nil {
		return "", err
	}
	return buf.String(), nil
}

func sortedKeys(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func orDefault(v, def string) string {
	if v == "" {
		return def
	}
	return v
}

func podSecurityContext(s *PodSecuritySpec) *corev1.PodSecurityContext {
	if s == nil {
		return nil
	}
	return &corev1.PodSecurityContext{
		RunAsUser:    s.RunAsUser,
		RunAsGroup:   s.RunAsGroup,
		FSGroup:      s.FSGroup,
		RunAsNonRoot: s.RunAsNonRoot,
	}
}
