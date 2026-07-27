package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// StackSpec is a multi-service application graph: services (each built once and
// run as one or more processes), the backing data services they need, and the
// volumes and connections that wire them together. M5a reconciles the services
// half (processes -> Deployments/Services, release hooks -> Jobs); backing
// services and connection provisioning land in M5b.
type StackSpec struct {
	// Services are the application services built from the repo.
	Services []ServiceSpec `json:"services,omitempty"`
	// Backing are the self-hosted data services (postgres, valkey, ...).
	// Recorded now; provisioned by the catalog in M5b.
	Backing []BackingSpec `json:"backing,omitempty"`
	// Volumes are named storage claims (block|file|object). Recorded now.
	Volumes []VolumeSpec `json:"volumes,omitempty"`
}

// ServiceSpec is one buildable unit that runs as one or more processes.
type ServiceSpec struct {
	Name string `json:"name"`
	// Image is the OCI ref to run. M5a uses this directly; Build (repo -> image
	// via the fleet) is wired in a later milestone.
	Image string `json:"image,omitempty"`
	// Build describes how to produce the image from the repo (not yet wired).
	Build *BuildSpec `json:"build,omitempty"`
	// Processes are the deployables from this one image (web, worker, cron).
	Processes []ProcessSpec `json:"processes,omitempty"`
	// Env is injected into every process of this service.
	Env []EnvVar `json:"env,omitempty"`
	// Hooks.release runs to completion (as a Job) before the processes roll.
	Hooks *Hooks `json:"hooks,omitempty"`
	// Connections are typed edges to other services or backing services.
	// M5a resolves intra-stack service edges (injects an internal URL);
	// backing-service edges are recorded for M5b.
	Connections []ConnectionSpec `json:"connections,omitempty"`
}

// BuildSpec is where the image comes from when not given directly.
type BuildSpec struct {
	RepoSubdir string `json:"repoSubdir,omitempty"`
	Dockerfile string `json:"dockerfile,omitempty"`
}

// ProcessSpec is one deployable of a service.
type ProcessSpec struct {
	Name string `json:"name"`
	// Command is run via `sh -c`. Empty uses the image entrypoint.
	Command string `json:"command,omitempty"`
	// Port, if >0, exposes a Service and (later) an edge domain.
	Port int32 `json:"port,omitempty"`
	// +kubebuilder:default=1
	Replicas int32 `json:"replicas,omitempty"`
	// Kind is web|worker|cron (informational for M5a; cron -> CronJob later).
	// +kubebuilder:default=web
	Kind      string    `json:"kind,omitempty"`
	Resources Resources `json:"resources,omitempty"`
}

// Hooks are lifecycle commands.
type Hooks struct {
	// Release runs to completion before the new processes roll (migrations).
	Release string `json:"release,omitempty"`
}

// ConnectionSpec is a typed edge. `To` names a service or backing service in the
// stack; `As` is the env var the consumer expects the connection under.
type ConnectionSpec struct {
	To string `json:"to"`
	As string `json:"as,omitempty"`
}

// BackingSpec references a catalog data service. `Type` selects the catalog tier:
// an operator adapter (postgres), a template descriptor (valkey, memcached, ...),
// or, for an uncataloged type, the generic fallback (requires Image + Port).
type BackingSpec struct {
	Name    string `json:"name"`
	Type    string `json:"type"`
	Version string `json:"version,omitempty"`
	Disk    string `json:"disk,omitempty"`
	// +kubebuilder:default=small
	Plan string `json:"plan,omitempty"`
	// Image is the OCI ref for the generic FALLBACK path (uncataloged type). Ignored
	// for operator- and template-backed types, which pick their own image.
	Image string `json:"image,omitempty"`
	// Port is the server port for the FALLBACK path. Required when Image is set.
	Port int32 `json:"port,omitempty"`
}

// VolumeSpec is a named storage claim.
type VolumeSpec struct {
	Name string `json:"name"`
	// +kubebuilder:default=block
	Kind string `json:"kind,omitempty"` // block|file|object
}

// StackStatus is the observed state.
type StackStatus struct {
	Phase              string `json:"phase,omitempty"`
	ObservedGeneration int64  `json:"observedGeneration,omitempty"`
	Services           int32  `json:"services,omitempty"`
	ReadyServices      int32  `json:"readyServices,omitempty"`
	// ReleaseHook is the aggregate phase of release Jobs this generation.
	ReleaseHook string `json:"releaseHook,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Services",type=integer,JSONPath=`.status.services`
// +kubebuilder:printcolumn:name="Ready",type=integer,JSONPath=`.status.readyServices`
// +kubebuilder:printcolumn:name="Release",type=string,JSONPath=`.status.releaseHook`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`

// Stack is a multi-service application graph.
type Stack struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   StackSpec   `json:"spec,omitempty"`
	Status StackStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// StackList is a list of Stack.
type StackList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Stack `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Stack{}, &StackList{})
}
