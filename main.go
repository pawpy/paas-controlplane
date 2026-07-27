// Command paas-controlplane runs the control-plane operator: it reconciles
// paas.sh App/Release custom resources into running, routed tenant workloads.
package main

import (
	"os"

	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	paasv1 "github.com/pawpy/paas-controlplane/api/v1alpha1"
	"github.com/pawpy/paas-controlplane/internal/controller"
)

var scheme = runtime.NewScheme()

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(paasv1.AddToScheme(scheme))
}

func main() {
	ctrl.SetLogger(zap.New(zap.UseDevMode(true)))
	setupLog := ctrl.Log.WithName("setup")

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsserver.Options{BindAddress: "0"},
		HealthProbeBindAddress: ":8081",
	})
	if err != nil {
		setupLog.Error(err, "unable to start manager")
		os.Exit(1)
	}

	tolerate := os.Getenv("TENANT_SCHEDULE_ON_CONTROL_PLANE") != "false"

	// The template-tier catalog (servicedef.go): compiled-in descriptors, later
	// overlaid at reconcile time by the optional paas-servicedefs ConfigMap.
	builtins, err := controller.LoadBuiltinCatalog()
	if err != nil {
		setupLog.Error(err, "unable to load builtin service catalog")
		os.Exit(1)
	}
	systemNamespace := os.Getenv("PAAS_SYSTEM_NAMESPACE")
	if systemNamespace == "" {
		systemNamespace = "paas-system"
	}

	// Overcommit pool for tenant workloads (this box = dev, 15x CPU / 2x mem) and
	// the scheduler tenant pods opt into (Trimaran usage-aware bin-packer).
	tier := controller.ResolveTier(os.Getenv("PAAS_OVERCOMMIT_TIER"))
	schedulerName := os.Getenv("PAAS_SCHEDULER_NAME")

	// S3 object-storage backing (Ceph RGW via Rook).
	objectStorageClass := os.Getenv("PAAS_OBJECT_STORAGECLASS")
	if objectStorageClass == "" {
		objectStorageClass = "ceph-bucket"
	}
	objectEndpoint := os.Getenv("PAAS_OBJECT_ENDPOINT")
	if objectEndpoint == "" {
		objectEndpoint = "http://rook-ceph-rgw-paas-s3.rook-ceph.svc"
	}

	if err := (&controller.AppReconciler{
		Client:               mgr.GetClient(),
		Scheme:               mgr.GetScheme(),
		TolerateControlPlane: tolerate,
		Tier:                 tier,
		SchedulerName:        schedulerName,
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to set up App controller")
		os.Exit(1)
	}

	if err := (&controller.StackReconciler{
		Client:               mgr.GetClient(),
		Scheme:               mgr.GetScheme(),
		TolerateControlPlane: tolerate,
		Builtins:             builtins,
		SystemNamespace:      systemNamespace,
		Tier:                 tier,
		SchedulerName:        schedulerName,
		ObjectStorageClass:   objectStorageClass,
		ObjectEndpoint:       objectEndpoint,
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to set up Stack controller")
		os.Exit(1)
	}

	_ = mgr.AddHealthzCheck("healthz", healthz.Ping)
	_ = mgr.AddReadyzCheck("readyz", healthz.Ping)

	setupLog.Info("starting control-plane operator")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "manager exited")
		os.Exit(1)
	}
}
