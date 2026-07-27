package controller

import (
	"context"
	_ "embed"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/yaml"

	ctrl "sigs.k8s.io/controller-runtime"

	paasv1 "github.com/pawpy/paas-controlplane/api/v1alpha1"
)

// tenantBaseline is the security baseline stamped into every project/environment
// namespace (default-deny NetworkPolicies + apiserver/operator/object allows +
// LimitRange). ${NAMESPACE} is substituted per environment. See baseline/.
//
//go:embed baseline/tenant-baseline.yaml
var tenantBaseline string

const envFinalizer = "paas.sh/environment-teardown"

// EnvironmentReconciler turns an Environment into an isolated namespace: it
// creates `proj-<project>-<name>`, stamps the tenant security baseline, and (if a
// Stack is given) deploys the app graph into it. This replaces the manual
// kubectl+sed namespace bring-up. Teardown (delete Environment) deletes the
// namespace, taking the whole graph with it — the PR-preview lifecycle.
type EnvironmentReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=paas.sh,resources=environments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=paas.sh,resources=environments/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=paas.sh,resources=environments/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=namespaces,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=limitranges,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=networking.k8s.io,resources=networkpolicies,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=cilium.io,resources=ciliumnetworkpolicies,verbs=get;list;watch;create;update;patch;delete

func (r *EnvironmentReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	l := log.FromContext(ctx)

	var env paasv1.Environment
	if err := r.Get(ctx, req.NamespacedName, &env); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	ns := namespaceFor(&env)

	// Teardown: on deletion, delete the namespace (cascades the whole graph), then
	// drop the finalizer. The namespace is cluster-scoped so it cannot be an owned
	// child of the (namespaced) Environment; the finalizer is how we GC it.
	if !env.DeletionTimestamp.IsZero() {
		if controllerutil.ContainsFinalizer(&env, envFinalizer) {
			if err := r.deleteNamespace(ctx, ns); err != nil {
				return ctrl.Result{}, err
			}
			controllerutil.RemoveFinalizer(&env, envFinalizer)
			if err := r.Update(ctx, &env); err != nil {
				return ctrl.Result{}, err
			}
		}
		return ctrl.Result{}, nil
	}
	if controllerutil.AddFinalizer(&env, envFinalizer) {
		if err := r.Update(ctx, &env); err != nil {
			return ctrl.Result{}, err
		}
	}

	// 1) Namespace with project/PSA labels.
	if err := r.ensureNamespace(ctx, &env, ns); err != nil {
		return ctrl.Result{}, err
	}
	// 2) Security baseline (rendered from the embedded template, server-side applied).
	if err := r.stampBaseline(ctx, ns); err != nil {
		return ctrl.Result{}, fmt.Errorf("baseline: %w", err)
	}
	// 3) The app graph, if any.
	if env.Spec.Stack != nil {
		if err := r.ensureStack(ctx, &env, ns); err != nil {
			return ctrl.Result{}, fmt.Errorf("stack: %w", err)
		}
	}

	l.Info("environment reconciled", "namespace", ns, "type", env.Spec.Type)
	return r.updateEnvStatus(ctx, &env, ns)
}

func namespaceFor(env *paasv1.Environment) string {
	return fmt.Sprintf("proj-%s-%s", env.Spec.Project, env.Name)
}

func (r *EnvironmentReconciler) ensureNamespace(ctx context.Context, env *paasv1.Environment, ns string) error {
	n := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, n, func() error {
		if n.Labels == nil {
			n.Labels = map[string]string{}
		}
		n.Labels["paas.sh/project"] = env.Spec.Project
		n.Labels["paas.sh/environment"] = env.Name
		n.Labels["paas.sh/env-type"] = orDefault(env.Spec.Type, "persistent")
		// Match the tenants-namespace PSA convention: enforce baseline (railpack
		// images run as root, so restricted would break them), warn on restricted.
		n.Labels["pod-security.kubernetes.io/enforce"] = "baseline"
		n.Labels["pod-security.kubernetes.io/warn"] = "restricted"
		return nil
	})
	return err
}

// stampBaseline renders the embedded baseline for this namespace and server-side
// applies each document. SSA (not CreateOrUpdate) so we can apply heterogeneous
// objects — including the CiliumNetworkPolicy — without typed clients, and so
// re-stamping converges idempotently under a single field owner.
func (r *EnvironmentReconciler) stampBaseline(ctx context.Context, ns string) error {
	rendered := strings.ReplaceAll(tenantBaseline, "${NAMESPACE}", ns)
	for _, doc := range strings.Split(rendered, "\n---") {
		if strings.TrimSpace(stripComments(doc)) == "" {
			continue
		}
		obj := map[string]interface{}{}
		if err := yaml.Unmarshal([]byte(doc), &obj); err != nil {
			return fmt.Errorf("parse baseline doc: %w", err)
		}
		if len(obj) == 0 {
			continue
		}
		u := &unstructured.Unstructured{Object: obj}
		u.SetNamespace(ns)
		if err := r.Patch(ctx, u, client.Apply,
			client.FieldOwner("paas-environment"), client.ForceOwnership); err != nil {
			return fmt.Errorf("apply %s/%s: %w", u.GetKind(), u.GetName(), err)
		}
	}
	return nil
}

// ensureStack deploys the environment's app graph as a Stack named after the
// environment, in its namespace. No owner reference: the Stack lives in a
// different namespace from the (control-plane) Environment, so it is GC'd by the
// namespace deletion at teardown, not by ownership.
func (r *EnvironmentReconciler) ensureStack(ctx context.Context, env *paasv1.Environment, ns string) error {
	st := &paasv1.Stack{ObjectMeta: metav1.ObjectMeta{Name: env.Name, Namespace: ns}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, st, func() error {
		if st.Labels == nil {
			st.Labels = map[string]string{}
		}
		st.Labels["paas.sh/project"] = env.Spec.Project
		st.Labels["paas.sh/environment"] = env.Name
		// Clone (M5c-2): a preview seeds its stateful backings from a sibling
		// environment's data. Record the parent coordinates as annotations; the
		// Stack controller uses them to bootstrap postgres via CNPG recovery from
		// the parent's barman backup. Non-postgres backings start fresh.
		if env.Spec.Type == "preview" && env.Spec.CloneFrom != "" {
			if st.Annotations == nil {
				st.Annotations = map[string]string{}
			}
			st.Annotations["paas.sh/clone-from-stack"] = env.Spec.CloneFrom
			st.Annotations["paas.sh/clone-from-namespace"] =
				fmt.Sprintf("proj-%s-%s", env.Spec.Project, env.Spec.CloneFrom)
		}
		st.Spec = *env.Spec.Stack
		return nil
	})
	return err
}

func (r *EnvironmentReconciler) deleteNamespace(ctx context.Context, ns string) error {
	n := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}
	if err := r.Delete(ctx, n); err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	return nil
}

func (r *EnvironmentReconciler) updateEnvStatus(ctx context.Context, env *paasv1.Environment, ns string) (ctrl.Result, error) {
	// Ready once the namespace exists (baseline + stack are applied above).
	var got corev1.Namespace
	nsReady := r.Get(ctx, types.NamespacedName{Name: ns}, &got) == nil
	env.Status.Namespace = ns
	env.Status.BaselineStamped = nsReady
	if nsReady {
		env.Status.Phase = "Ready"
	} else {
		env.Status.Phase = "Provisioning"
	}
	env.Status.ObservedGeneration = env.Generation
	if err := r.Status().Update(ctx, env); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

// stripComments drops full-line comments so an all-comment split chunk counts as
// empty (the embedded file's header/section comments).
func stripComments(doc string) string {
	var b strings.Builder
	for _, line := range strings.Split(doc, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "#") {
			continue
		}
		b.WriteString(line)
		b.WriteString("\n")
	}
	return b.String()
}

func (r *EnvironmentReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&paasv1.Environment{}).
		Complete(r)
}
