package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ReleaseSpec is an immutable record of one deploy: an app + the image built
// for a given git sha. Creating a Release triggers a rollout of its App.
type ReleaseSpec struct {
	// App is the name of the App this release belongs to (same namespace).
	App string `json:"app"`
	// Image is the OCI ref produced by the build (e.g. registry.local/<app>:<sha>).
	Image string `json:"image"`
	// GitSHA is the commit this release was built from.
	GitSHA string `json:"gitSHA,omitempty"`
}

// ReleaseStatus is the observed state.
type ReleaseStatus struct {
	Phase string `json:"phase,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="App",type=string,JSONPath=`.spec.app`
// +kubebuilder:printcolumn:name="Image",type=string,JSONPath=`.spec.image`
// +kubebuilder:printcolumn:name="SHA",type=string,JSONPath=`.spec.gitSHA`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`

// Release is one immutable deploy of an App.
type Release struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ReleaseSpec   `json:"spec,omitempty"`
	Status ReleaseStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// ReleaseList is a list of Release.
type ReleaseList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Release `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Release{}, &ReleaseList{})
}
