package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// EnvVar is a plain name/value environment variable for the app process.
type EnvVar struct {
	Name  string `json:"name"`
	Value string `json:"value,omitempty"`
}

// Resources is the per-replica limit. Requests are set low by the operator so
// the tenant pool can overcommit; these are the ceilings.
type Resources struct {
	// +kubebuilder:default="500m"
	CPU string `json:"cpu,omitempty"`
	// +kubebuilder:default="512Mi"
	Memory string `json:"memory,omitempty"`
}

// AppSpec is the desired state of a tenant application. It carries the
// long-lived config; the image to run comes from the newest Release.
type AppSpec struct {
	// Owner is the user that owns the app (multi-user comes later).
	Owner string `json:"owner,omitempty"`
	// Port the app process listens on inside the container.
	// +kubebuilder:default=8080
	Port int32 `json:"port,omitempty"`
	// +kubebuilder:default=1
	Replicas int32 `json:"replicas,omitempty"`
	// Env is injected into the container (PORT is always set by the operator).
	Env []EnvVar `json:"env,omitempty"`
	// Resources is the per-replica CPU/memory ceiling.
	Resources Resources `json:"resources,omitempty"`
	// Domains are extra hostnames to route (the sslip.io host is always added).
	Domains []string `json:"domains,omitempty"`
}

// AppStatus is the observed state.
type AppStatus struct {
	Phase              string `json:"phase,omitempty"`
	Image              string `json:"image,omitempty"`
	URL                string `json:"url,omitempty"`
	ReadyReplicas      int32  `json:"readyReplicas,omitempty"`
	ObservedGeneration int64  `json:"observedGeneration,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Image",type=string,JSONPath=`.status.image`
// +kubebuilder:printcolumn:name="URL",type=string,JSONPath=`.status.url`
// +kubebuilder:printcolumn:name="Ready",type=integer,JSONPath=`.status.readyReplicas`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`

// App is a tenant application: a stable name + config, deployed to whatever
// image the newest Release for it points at.
type App struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   AppSpec   `json:"spec,omitempty"`
	Status AppStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// AppList is a list of App.
type AppList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []App `json:"items"`
}

func init() {
	SchemeBuilder.Register(&App{}, &AppList{})
}
