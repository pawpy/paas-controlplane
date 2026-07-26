package controller

import (
	"context"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	paasv1 "github.com/pawpy/paas-controlplane/api/v1alpha1"
)

// StackReconciler drives a Stack toward: for each service, a release Job run to
// completion (if it has a release hook) and then a Deployment per process (+ a
// Service for processes with a port). Backing services and cross-namespace
// connection wiring are M5b; here, intra-stack service connections are resolved
// to internal URLs.
type StackReconciler struct {
	client.Client
	Scheme               *runtime.Scheme
	TolerateControlPlane bool
}

// +kubebuilder:rbac:groups=paas.sh,resources=stacks,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=paas.sh,resources=stacks/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;update;patch;delete

func (r *StackReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	l := log.FromContext(ctx)

	var stack paasv1.Stack
	if err := r.Get(ctx, req.NamespacedName, &stack); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	internalURLs := serviceURLs(&stack)
	releasePending, releaseFailed := false, false
	var deployed int32

	for i := range stack.Spec.Services {
		svc := &stack.Spec.Services[i]
		if svc.Image == "" {
			// Build (repo -> image) is not wired yet; skip imageless services.
			l.Info("service has no image, skipping until build is wired", "service", svc.Name)
			continue
		}
		image := normalizeImage(svc.Image)

		// Env shared by this service's processes: service env + resolved
		// intra-stack connection URLs. Backing-service connections are M5b.
		baseEnv := append([]paasv1.EnvVar{}, svc.Env...)
		for _, c := range svc.Connections {
			if url, ok := internalURLs[c.To]; ok && c.As != "" {
				baseEnv = append(baseEnv, paasv1.EnvVar{Name: c.As, Value: url})
			}
		}

		// Release hook gates the rollout: run it to completion first.
		if svc.Hooks != nil && svc.Hooks.Release != "" {
			done, failed, err := r.reconcileReleaseJob(ctx, &stack, svc, image, baseEnv)
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
			if err := r.reconcileProcess(ctx, &stack, svc, p, image, baseEnv); err != nil {
				return ctrl.Result{}, fmt.Errorf("service %s process %s: %w", svc.Name, p.Name, err)
			}
			deployed++
		}
	}

	return r.updateStatus(ctx, &stack, releasePending, releaseFailed)
}

// reconcileReleaseJob ensures the per-generation release Job exists and reports
// whether it is done/failed. A new generation re-runs the migration.
func (r *StackReconciler) reconcileReleaseJob(ctx context.Context, stack *paasv1.Stack, svc *paasv1.ServiceSpec, image string, env []paasv1.EnvVar) (done, failed bool, err error) {
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

func (r *StackReconciler) buildReleaseJob(stack *paasv1.Stack, svc *paasv1.ServiceSpec, image, name string, env []paasv1.EnvVar) batchv1.Job {
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
					Containers: []corev1.Container{{
						Name:            "release",
						Image:           image,
						ImagePullPolicy: corev1.PullIfNotPresent,
						Command:         shCommand(svc.Hooks.Release),
						Env:             plainEnv(env),
						SecurityContext: hardenedSecurityContext(),
					}},
				},
			},
		},
	}
}

// reconcileProcess creates/updates the Deployment (+ Service, if the process has
// a port) for one process of a service.
func (r *StackReconciler) reconcileProcess(ctx context.Context, stack *paasv1.Stack, svc *paasv1.ServiceSpec, p *paasv1.ProcessSpec, image string, baseEnv []paasv1.EnvVar) error {
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
		c := corev1.Container{
			Name:            p.Name,
			Image:           image,
			ImagePullPolicy: corev1.PullIfNotPresent,
			Env:             processEnv(baseEnv, p.Port),
			Resources:       resources(p.Resources),
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

func (r *StackReconciler) updateStatus(ctx context.Context, stack *paasv1.Stack, releasePending, releaseFailed bool) (ctrl.Result, error) {
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
				break // first ported process is the service's entrypoint
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

// processEnv is the container env for a running process: PORT (when it has one) +
// the service/connection env.
func processEnv(in []paasv1.EnvVar, port int32) []corev1.EnvVar {
	var out []corev1.EnvVar
	if port > 0 {
		out = append(out, corev1.EnvVar{Name: "PORT", Value: fmt.Sprintf("%d", port)})
	}
	return append(out, plainEnv(in)...)
}

func plainEnv(in []paasv1.EnvVar) []corev1.EnvVar {
	out := make([]corev1.EnvVar, 0, len(in))
	for _, e := range in {
		out = append(out, corev1.EnvVar{Name: e.Name, Value: e.Value})
	}
	return out
}
