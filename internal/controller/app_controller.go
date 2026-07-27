package controller

import (
	"context"
	"fmt"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	paasv1 "github.com/pawpy/paas-controlplane/api/v1alpha1"
)

// NodeIP is the address the sslip.io wildcard resolves to. Single-box for now;
// becomes a Gateway address / real domain later.
const NodeIP = "65.109.119.211"

// AppReconciler drives an App toward: a Deployment + Service running the image
// from its newest Release. Owns() the children so they are garbage-collected
// with the App and so their status changes re-trigger reconcile.
type AppReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	// TolerateControlPlane adds the control-plane NoSchedule toleration to tenant
	// pods so they can run on a single converged box. Set false once dedicated
	// tenant/worker nodes exist (tenants should not land on control-plane then).
	TolerateControlPlane bool
	// Tier is the overcommit pool for tenant workloads (dev/prod/build). Requests
	// are derived from limits by its ratios. See overcommit.go.
	Tier overcommit
	// SchedulerName routes tenant pods to a named scheduler (Trimaran usage-aware
	// bin-packer); empty uses the default kube-scheduler.
	SchedulerName string
}

// +kubebuilder:rbac:groups=paas.sh,resources=apps;releases,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=paas.sh,resources=apps/status;releases/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch;create;update;patch;delete

func (r *AppReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	l := log.FromContext(ctx)

	var app paasv1.App
	if err := r.Get(ctx, req.NamespacedName, &app); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Pick the newest Release that targets this app.
	var releases paasv1.ReleaseList
	if err := r.List(ctx, &releases, client.InNamespace(app.Namespace)); err != nil {
		return ctrl.Result{}, err
	}
	var newest *paasv1.Release
	for i := range releases.Items {
		rel := &releases.Items[i]
		if rel.Spec.App != app.Name {
			continue
		}
		if newest == nil || rel.CreationTimestamp.After(newest.CreationTimestamp.Time) {
			newest = rel
		}
	}
	if newest == nil {
		return r.setPhase(ctx, &app, "NoRelease")
	}
	image := normalizeImage(newest.Spec.Image)

	replicas := app.Spec.Replicas
	if replicas == 0 {
		replicas = 1
	}
	port := app.Spec.Port
	if port == 0 {
		port = 8080
	}
	labels := map[string]string{"paas.sh/app": app.Name}

	dep := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: app.Name, Namespace: app.Namespace}}
	if _, err := controllerutil.CreateOrUpdate(ctx, r.Client, dep, func() error {
		dep.Labels = labels
		dep.Spec.Replicas = &replicas
		dep.Spec.Selector = &metav1.LabelSelector{MatchLabels: labels}
		dep.Spec.Template.ObjectMeta.Labels = labels
		if r.TolerateControlPlane {
			dep.Spec.Template.Spec.Tolerations = []corev1.Toleration{{
				Key:      "node-role.kubernetes.io/control-plane",
				Operator: corev1.TolerationOpExists,
				Effect:   corev1.TaintEffectNoSchedule,
			}}
		}
		applyScheduler(&dep.Spec.Template.Spec, r.SchedulerName)
		dep.Spec.Template.Spec.Containers = []corev1.Container{{
			Name:            "app",
			Image:           image,
			ImagePullPolicy: corev1.PullIfNotPresent,
			Ports:           []corev1.ContainerPort{{ContainerPort: port, Name: "http"}},
			Env:             toEnv(app.Spec.Env, port),
			Resources:       resources(app.Spec.Resources, r.Tier),
			SecurityContext: &corev1.SecurityContext{
				AllowPrivilegeEscalation: ptr(false),
				Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
			},
		}}
		return controllerutil.SetControllerReference(&app, dep, r.Scheme)
	}); err != nil {
		return ctrl.Result{}, fmt.Errorf("deployment: %w", err)
	}

	svc := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: app.Name, Namespace: app.Namespace}}
	if _, err := controllerutil.CreateOrUpdate(ctx, r.Client, svc, func() error {
		svc.Labels = labels
		svc.Spec.Selector = labels
		svc.Spec.Ports = []corev1.ServicePort{{Name: "http", Port: 80, TargetPort: intstr.FromInt32(port)}}
		return controllerutil.SetControllerReference(&app, svc, r.Scheme)
	}); err != nil {
		return ctrl.Result{}, fmt.Errorf("service: %w", err)
	}

	app.Status.Image = image
	app.Status.URL = fmt.Sprintf("http://%s.%s.sslip.io", app.Name, strings.ReplaceAll(NodeIP, ".", "-"))
	app.Status.ReadyReplicas = dep.Status.ReadyReplicas
	app.Status.ObservedGeneration = app.Generation
	if dep.Status.ReadyReplicas >= 1 {
		app.Status.Phase = "Running"
	} else {
		app.Status.Phase = "Progressing"
	}
	if err := r.Status().Update(ctx, &app); err != nil {
		return ctrl.Result{}, err
	}
	l.Info("reconciled", "app", app.Name, "image", image, "phase", app.Status.Phase, "ready", app.Status.ReadyReplicas)
	return ctrl.Result{}, nil
}

func (r *AppReconciler) setPhase(ctx context.Context, app *paasv1.App, phase string) (ctrl.Result, error) {
	if app.Status.Phase == phase {
		return ctrl.Result{}, nil
	}
	app.Status.Phase = phase
	return ctrl.Result{}, r.Status().Update(ctx, app)
}

func (r *AppReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&paasv1.App{}).
		Owns(&appsv1.Deployment{}).
		Owns(&corev1.Service{}).
		Watches(&paasv1.Release{}, handler.EnqueueRequestsFromMapFunc(releaseToApp)).
		Complete(r)
}

// releaseToApp enqueues the App a Release belongs to.
func releaseToApp(_ context.Context, obj client.Object) []reconcile.Request {
	rel, ok := obj.(*paasv1.Release)
	if !ok {
		return nil
	}
	return []reconcile.Request{{NamespacedName: types.NamespacedName{Namespace: rel.Namespace, Name: rel.Spec.App}}}
}

// normalizeImage rewrites the build's registry.local:5000 push ref to the bare
// registry.local host, which is what the node's containerd mirror is keyed on.
func normalizeImage(img string) string {
	return strings.Replace(img, "registry.local:5000/", "registry.local/", 1)
}

func toEnv(in []paasv1.EnvVar, port int32) []corev1.EnvVar {
	out := []corev1.EnvVar{{Name: "PORT", Value: fmt.Sprintf("%d", port)}}
	for _, e := range in {
		out = append(out, corev1.EnvVar{Name: e.Name, Value: e.Value})
	}
	return out
}

// resources builds the container resources for a tenant workload: the spec's
// limits as the ceiling, and tier-derived requests (limit/overcommit) so the pool
// overcommits (dev 15x CPU, 2x mem). See overcommit.go.
func resources(rr paasv1.Resources, t overcommit) corev1.ResourceRequirements {
	cpu := rr.CPU
	if cpu == "" {
		cpu = "500m"
	}
	mem := rr.Memory
	if mem == "" {
		mem = "512Mi"
	}
	return t.limitedResources(resource.MustParse(cpu), resource.MustParse(mem))
}

func ptr[T any](v T) *T { return &v }
