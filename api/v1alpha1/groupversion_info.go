// Package v1alpha1 contains the paas.sh/v1alpha1 API: the App and Release
// custom resources that the control-plane operator reconciles into running,
// routed tenant workloads.
// +kubebuilder:object:generate=true
// +groupName=paas.sh
package v1alpha1

import (
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/scheme"
)

// GroupVersion is the API group/version for these types.
var GroupVersion = schema.GroupVersion{Group: "paas.sh", Version: "v1alpha1"}

// SchemeBuilder registers the types into a scheme.
var SchemeBuilder = &scheme.Builder{GroupVersion: GroupVersion}

// AddToScheme adds the types to a runtime scheme.
var AddToScheme = SchemeBuilder.AddToScheme
