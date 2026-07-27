package controller

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

// overcommit is a pool's CPU/memory overcommit ratios. Kubernetes has no capacity
// multiplier: the scheduler admits a pod only if Σrequests ≤ node allocatable. So
// we encode a pool's overcommit by deriving each container's REQUEST from its LIMIT
// as limit/ratio. Then Σrequests ≤ allocatable ⟺ Σlimits ≤ allocatable×ratio, i.e.
// the node admits ratio-x as many pods as it has real capacity for. cpu.weight
// (derived from the request) still governs fair sharing under contention, and the
// limit is the hard cgroup ceiling (cpu.max / memory.max).
//
// CPU is COMPRESSIBLE (contention just throttles), so it takes the aggressive ratio
// (dev 15x, prod 7x). MEMORY is INCOMPRESSIBLE (overcommit -> OOM kill), so it stays
// conservative at 2:1 and leans on MemoryQoS: memory.min = request (protected from
// reclaim) and memory.high = soft-reclaim watermark below the memory.max ceiling
// (kubelet MemoryQoS feature gate + memoryThrottlingFactor).
type overcommit struct {
	name string
	cpu  int64
	mem  int64
}

// overcommitTiers are the pools. This converged prototype box runs as "dev"
// (PAAS_OVERCOMMIT_TIER); dedicated prod hosts set "prod", build hosts "build".
var overcommitTiers = map[string]overcommit{
	"dev":        {name: "dev", cpu: 15, mem: 2},
	"prod":       {name: "prod", cpu: 7, mem: 2},
	"build":      {name: "build", cpu: 2, mem: 2},
	"enterprise": {name: "enterprise", cpu: 1, mem: 1},
}

// ResolveTier maps a tier name to its ratios, defaulting to dev for unknown/empty.
func ResolveTier(name string) overcommit {
	if t, ok := overcommitTiers[name]; ok {
		return t
	}
	return overcommitTiers["dev"]
}

// limitedResources builds a ResourceRequirements: the given limits as the hard
// ceiling, and tier-derived requests (limit/ratio) as the scheduler reservation.
func (t overcommit) limitedResources(cpuLimit, memLimit resource.Quantity) corev1.ResourceRequirements {
	return corev1.ResourceRequirements{
		Requests: t.requestsFromLimits(cpuLimit, memLimit),
		Limits: corev1.ResourceList{
			corev1.ResourceCPU:    cpuLimit,
			corev1.ResourceMemory: memLimit,
		},
	}
}

// requestsFromLimits derives requests = limit/ratio, with small floors (1m CPU,
// 4Mi memory) so tiny limits still round to a nonzero request. A nonzero request
// keeps the pod QoS Burstable (not BestEffort, first to be OOM-killed) and keeps
// cpu.weight proportional to the limit.
func (t overcommit) requestsFromLimits(cpuLimit, memLimit resource.Quantity) corev1.ResourceList {
	reqCPU := cpuLimit.MilliValue() / t.cpu
	if reqCPU < 1 {
		reqCPU = 1
	}
	reqMem := memLimit.Value() / t.mem
	const minMem = 4 * 1024 * 1024 // 4Mi
	if reqMem < minMem {
		reqMem = minMem
	}
	return corev1.ResourceList{
		corev1.ResourceCPU:    *resource.NewMilliQuantity(reqCPU, resource.DecimalSI),
		corev1.ResourceMemory: *resource.NewQuantity(reqMem, resource.BinarySI),
	}
}

// applyScheduler routes a pod to a named scheduler (e.g. the Trimaran usage-aware
// bin-packer) when one is configured; empty leaves the default kube-scheduler.
func applyScheduler(spec *corev1.PodSpec, name string) {
	if name != "" {
		spec.SchedulerName = name
	}
}
