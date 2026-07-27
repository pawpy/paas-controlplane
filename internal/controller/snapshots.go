package controller

import (
	"context"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	paasv1 "github.com/pawpy/paas-controlplane/api/v1alpha1"
)

// CSI snapshot + snapscheduler types, referenced as unstructured so this operator
// takes no build dependency on the external-snapshotter / snapscheduler modules
// (same convention as the CNPG and OBC types).
var (
	volumeSnapshotGVK        = schema.GroupVersionKind{Group: "snapshot.storage.k8s.io", Version: "v1", Kind: "VolumeSnapshot"}
	volumeSnapshotContentGVK = schema.GroupVersionKind{Group: "snapshot.storage.k8s.io", Version: "v1", Kind: "VolumeSnapshotContent"}
	snapshotScheduleGVK      = schema.GroupVersionKind{Group: "snapscheduler.backube", Version: "v1", Kind: "SnapshotSchedule"}
)

const (
	// snapshotClass is the Ceph RBD VolumeSnapshotClass (apps/snapshot-controller).
	snapshotClass = "ceph-block-snap"
	// backupLabel marks the PVCs the snapscheduler SnapshotSchedule selects.
	backupLabel = "paas.sh/backup"
	// cloneOwnerLabel keys the cross-namespace / cluster-scoped clone artifacts to
	// the preview namespace, so environment teardown can GC them (they cannot be
	// owned by a namespaced object across namespaces / at cluster scope).
	cloneOwnerLabel = "paas.sh/clone-owner"
)

// pvcNameFor is the deterministic PVC of a 1-replica template/fallback backing's
// StatefulSet (VolumeClaimTemplate "data", ordinal 0).
func pvcNameFor(stackName, backingName string) string {
	return fmt.Sprintf("data-%s-%s-0", stackName, backingName)
}

// isPreviewClone reports whether this Stack is a preview seeded from a parent
// (the Environment controller stamps the clone annotations). Preview clones are
// ephemeral: they restore from the parent but do not run their own backups.
func isPreviewClone(stack *paasv1.Stack) bool {
	return stack.Annotations["paas.sh/clone-from-namespace"] != "" &&
		stack.Annotations["paas.sh/clone-from-stack"] != ""
}

// ensureBackupSchedule stamps one snapscheduler SnapshotSchedule per stack that
// snapshots every PVC labelled paas.sh/backup=true in the namespace on a daily
// cadence and prunes to a retention count. Owned by the Stack (GC'd at teardown).
func (r *StackReconciler) ensureBackupSchedule(ctx context.Context, stack *paasv1.Stack) error {
	sched := &unstructured.Unstructured{}
	sched.SetGroupVersionKind(snapshotScheduleGVK)
	sched.SetNamespace(stack.Namespace)
	sched.SetName(stack.Name + "-backup")
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, sched, func() error {
		_ = unstructured.SetNestedStringMap(sched.Object,
			map[string]string{backupLabel: "true"}, "spec", "claimSelector", "matchLabels")
		_ = unstructured.SetNestedField(sched.Object, "0 3 * * *", "spec", "schedule")
		_ = unstructured.SetNestedField(sched.Object, int64(7), "spec", "retention", "maxCount")
		_ = unstructured.SetNestedField(sched.Object, snapshotClass,
			"spec", "snapshotTemplate", "volumeSnapshotClassName")
		return controllerutil.SetControllerReference(stack, sched, r.Scheme)
	})
	if err != nil {
		return fmt.Errorf("snapshotschedule %s: %w", stack.Name, err)
	}
	return nil
}

// labelBackupPVC tags a backing's PVC so the SnapshotSchedule selects it. The PVC
// is created asynchronously by the StatefulSet, so a not-found here is just a race
// (it gets labelled on the next reconcile).
func (r *StackReconciler) labelBackupPVC(ctx context.Context, stack *paasv1.Stack, backingName string) error {
	var pvc corev1.PersistentVolumeClaim
	key := types.NamespacedName{Namespace: stack.Namespace, Name: pvcNameFor(stack.Name, backingName)}
	if err := r.Get(ctx, key, &pvc); err != nil {
		return client.IgnoreNotFound(err)
	}
	if pvc.Labels[backupLabel] == "true" {
		return nil
	}
	if pvc.Labels == nil {
		pvc.Labels = map[string]string{}
	}
	pvc.Labels[backupLabel] = "true"
	pvc.Labels["paas.sh/backing"] = backingName
	return r.Update(ctx, &pvc)
}

// prepareCloneRestore drives the cross-namespace clone of a PVC-backed backing:
// snapshot the parent PVC, share the underlying Ceph snapshot handle into this
// namespace via a static (pre-provisioned) VolumeSnapshotContent, and expose a
// preview-namespace VolumeSnapshot to restore from. It returns the restore
// VolumeSnapshot name once ReadyToUse (ready=true). Until then ready=false and the
// caller must requeue WITHOUT creating the StatefulSet: a PVC's dataSource is set
// once, at creation, so the restore source must exist first.
//
// Lifecycle: the parent-namespace source snapshot and the cluster-scoped static
// content are labelled cloneOwnerLabel=<previewNs> and GC'd by environment
// teardown (they cannot be owned across namespaces / at cluster scope). The static
// content is Retain so deleting the preview snapshot never deletes the shared Ceph
// snapshot out from under the source.
func (r *StackReconciler) prepareCloneRestore(ctx context.Context, stack *paasv1.Stack, backingName string) (restoreVS string, ready bool, err error) {
	l := log.FromContext(ctx)
	parentNs := stack.Annotations["paas.sh/clone-from-namespace"]
	parentStack := stack.Annotations["paas.sh/clone-from-stack"]
	if parentNs == "" || parentStack == "" {
		return "", false, nil
	}
	previewNs := stack.Namespace
	parentPVC := pvcNameFor(parentStack, backingName)
	srcName := fmt.Sprintf("paas-clone-%s-%s", previewNs, backingName)

	// 1) Source snapshot in the parent namespace (labelled for teardown GC; no
	//    owner ref, it lives in another namespace).
	src := &unstructured.Unstructured{}
	src.SetGroupVersionKind(volumeSnapshotGVK)
	src.SetNamespace(parentNs)
	src.SetName(srcName)
	if _, err = controllerutil.CreateOrUpdate(ctx, r.Client, src, func() error {
		src.SetLabels(map[string]string{cloneOwnerLabel: previewNs})
		_ = unstructured.SetNestedField(src.Object, snapshotClass, "spec", "volumeSnapshotClassName")
		_ = unstructured.SetNestedField(src.Object, parentPVC, "spec", "source", "persistentVolumeClaimName")
		return nil
	}); err != nil {
		return "", false, fmt.Errorf("clone source snapshot %s/%s: %w", parentNs, srcName, err)
	}
	srcReady, _, _ := unstructured.NestedBool(src.Object, "status", "readyToUse")
	if !srcReady {
		l.Info("clone source snapshot not ready", "parent", parentNs+"/"+parentPVC)
		return "", false, nil
	}
	boundContent, _, _ := unstructured.NestedString(src.Object, "status", "boundVolumeSnapshotContentName")
	if boundContent == "" {
		return "", false, nil
	}
	srcContent := &unstructured.Unstructured{}
	srcContent.SetGroupVersionKind(volumeSnapshotContentGVK)
	if err = r.Get(ctx, types.NamespacedName{Name: boundContent}, srcContent); err != nil {
		return "", false, client.IgnoreNotFound(err)
	}
	handle, _, _ := unstructured.NestedString(srcContent.Object, "status", "snapshotHandle")
	driver, _, _ := unstructured.NestedString(srcContent.Object, "spec", "driver")
	if handle == "" || driver == "" {
		return "", false, nil
	}

	// 2) Static content (cluster-scoped) sharing the Ceph handle into the preview
	//    namespace. Retain: its lifecycle is independent of the source snapshot.
	restoreVS = "clone-" + backingName
	contentName := fmt.Sprintf("paas-clone-%s-%s", previewNs, backingName)
	content := &unstructured.Unstructured{}
	content.SetGroupVersionKind(volumeSnapshotContentGVK)
	content.SetName(contentName)
	if _, err = controllerutil.CreateOrUpdate(ctx, r.Client, content, func() error {
		content.SetLabels(map[string]string{cloneOwnerLabel: previewNs})
		_ = unstructured.SetNestedField(content.Object, "Retain", "spec", "deletionPolicy")
		_ = unstructured.SetNestedField(content.Object, driver, "spec", "driver")
		_ = unstructured.SetNestedField(content.Object, snapshotClass, "spec", "volumeSnapshotClassName")
		_ = unstructured.SetNestedField(content.Object, handle, "spec", "source", "snapshotHandle")
		_ = unstructured.SetNestedField(content.Object, restoreVS, "spec", "volumeSnapshotRef", "name")
		_ = unstructured.SetNestedField(content.Object, previewNs, "spec", "volumeSnapshotRef", "namespace")
		return nil
	}); err != nil {
		return "", false, fmt.Errorf("clone static content %s: %w", contentName, err)
	}

	// 3) Restore snapshot in the preview namespace, bound to the static content.
	rvs := &unstructured.Unstructured{}
	rvs.SetGroupVersionKind(volumeSnapshotGVK)
	rvs.SetNamespace(previewNs)
	rvs.SetName(restoreVS)
	if _, err = controllerutil.CreateOrUpdate(ctx, r.Client, rvs, func() error {
		rvs.SetLabels(map[string]string{cloneOwnerLabel: previewNs})
		_ = unstructured.SetNestedField(rvs.Object, snapshotClass, "spec", "volumeSnapshotClassName")
		_ = unstructured.SetNestedField(rvs.Object, contentName, "spec", "source", "volumeSnapshotContentName")
		return nil
	}); err != nil {
		return "", false, fmt.Errorf("clone restore snapshot %s/%s: %w", previewNs, restoreVS, err)
	}
	rReady, _, _ := unstructured.NestedBool(rvs.Object, "status", "readyToUse")
	return restoreVS, rReady, nil
}

// statefulSetExists reports whether a StatefulSet already exists. Used to gate
// clone bootstrap to first creation (the PVC dataSource is immutable afterward).
func (r *StackReconciler) statefulSetExists(ctx context.Context, ns, name string) bool {
	var ss appsv1.StatefulSet
	return r.Get(ctx, types.NamespacedName{Namespace: ns, Name: name}, &ss) == nil
}

// snapshotDataSource is the PVC dataSource that restores from a VolumeSnapshot.
func snapshotDataSource(vsName string) *corev1.TypedLocalObjectReference {
	api := "snapshot.storage.k8s.io"
	return &corev1.TypedLocalObjectReference{APIGroup: &api, Kind: "VolumeSnapshot", Name: vsName}
}

// gcCloneArtifacts deletes the cross-namespace source snapshots and cluster-scoped
// static contents a preview created (labelled cloneOwnerLabel=<previewNs>). Called
// by environment teardown; the in-namespace restore snapshot goes with the deleted
// namespace. Best-effort: snapshot CRDs may be absent on a cluster without CSI
// snapshots installed.
func gcCloneArtifacts(ctx context.Context, c client.Client, previewNs string) error {
	sel := client.MatchingLabels{cloneOwnerLabel: previewNs}
	for _, gvk := range []schema.GroupVersionKind{volumeSnapshotGVK, volumeSnapshotContentGVK} {
		list := &unstructured.UnstructuredList{}
		list.SetGroupVersionKind(gvk)
		if err := c.List(ctx, list, sel); err != nil {
			// A cluster without CSI snapshots installed has no such kind; teardown
			// must not fail on that.
			if apierrors.IsNotFound(err) || meta.IsNoMatchError(err) {
				continue
			}
			return err
		}
		for i := range list.Items {
			if err := c.Delete(ctx, &list.Items[i]); err != nil && !apierrors.IsNotFound(err) {
				return err
			}
		}
	}
	return nil
}
