package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// EnvironmentSpec is one namespace-isolated instance of a project: a persistent
// environment (production/staging) or an ephemeral preview (a PR environment,
// torn down with the Environment). The controller creates the namespace
// `proj-<project>-<name>`, stamps the tenant security baseline (default-deny
// NetworkPolicies, apiserver/operator/object-storage allows, LimitRange), and, if
// a Stack is given, deploys the whole app graph into it.
type EnvironmentSpec struct {
	// Project groups environments; the namespace is `proj-<project>-<name>`.
	Project string `json:"project"`
	// Type is "persistent" (long-lived) or "preview" (ephemeral PR env). Governs
	// teardown expectations and data-clone behaviour.
	// +kubebuilder:default=persistent
	// +kubebuilder:validation:Enum=persistent;preview
	Type string `json:"type,omitempty"`
	// CloneFrom names a sibling Environment (same project) whose data a preview
	// seeds from. Per-engine clone (postgres via CNPG bootstrap-from-backup, others
	// via volume snapshot) is layered on top; recorded here as the source of record.
	CloneFrom string `json:"cloneFrom,omitempty"`
	// Stack is the application graph deployed into this environment's namespace.
	// Optional: an Environment may just be a prepared, isolated namespace.
	Stack *StackSpec `json:"stack,omitempty"`
}

// EnvironmentStatus is the observed state.
type EnvironmentStatus struct {
	// Namespace is the project/environment namespace the controller manages.
	Namespace string `json:"namespace,omitempty"`
	// Phase: Provisioning -> Ready (namespace + baseline stamped, Stack applied).
	Phase string `json:"phase,omitempty"`
	// BaselineStamped records that the security baseline was applied.
	BaselineStamped    bool  `json:"baselineStamped,omitempty"`
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Project",type=string,JSONPath=`.spec.project`
// +kubebuilder:printcolumn:name="Type",type=string,JSONPath=`.spec.type`
// +kubebuilder:printcolumn:name="Namespace",type=string,JSONPath=`.status.namespace`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`

// Environment is a namespace-isolated instance of a project (persistent or a
// preview), with its security baseline and optional app graph.
type Environment struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   EnvironmentSpec   `json:"spec,omitempty"`
	Status EnvironmentStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// EnvironmentList is a list of Environment.
type EnvironmentList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Environment `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Environment{}, &EnvironmentList{})
}
