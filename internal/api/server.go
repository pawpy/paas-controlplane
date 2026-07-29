// Package api serves the control plane's HTTP surface for the canvas UI: Stacks
// and Environments as JSON, the backing-service catalog, and a server-sent-event
// stream of live workload status for the digital-twin overlay.
//
// It runs inside the operator process as a manager Runnable so reads come off the
// same informer cache the reconcilers use: listing stacks costs no apiserver
// round trip, and the SSE stream is driven by the cache's own watch events rather
// than by polling.
//
// Scope note: there is deliberately no authn/authz here. The Service is
// ClusterIP-only and is not routed through the edge; reaching it means being
// inside the cluster or holding a kubeconfig (`kubectl port-forward`), which is
// already full control-plane access. Exposing this publicly needs the auth story
// first.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/cache"
	ctrlcache "sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"

	paasv1 "github.com/pawpy/paas-controlplane/api/v1alpha1"
	"github.com/pawpy/paas-controlplane/internal/controller"
)

// Server is the UI-facing HTTP API. It satisfies manager.Runnable.
type Server struct {
	Addr    string
	Client  client.Client
	Cache   ctrlcache.Cache
	Catalog []controller.CatalogEntry
	Log     logr.Logger

	hub *hub
}

// NeedLeaderElection reports false: the API is read-mostly and should serve from
// every replica, not just the leader.
func (s *Server) NeedLeaderElection() bool { return false }

// Start runs the HTTP server until ctx is cancelled. Called by the manager after
// the caches have synced, so informer registration below will not block.
func (s *Server) Start(ctx context.Context) error {
	s.hub = newHub(s.Log)

	if err := s.watchForTwinUpdates(ctx); err != nil {
		return fmt.Errorf("register twin informers: %w", err)
	}
	go s.hub.run(ctx, func() []StackTwin { return s.twins(ctx) })

	srv := &http.Server{
		Addr:              s.Addr,
		Handler:           s.routes(),
		ReadHeaderTimeout: 10 * time.Second,
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	s.Log.Info("serving canvas API", "addr", s.Addr)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

func (s *Server) routes() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /api/v1/catalog", s.handleCatalog)
	mux.HandleFunc("GET /api/v1/stacks", s.handleListStacks)
	mux.HandleFunc("GET /api/v1/stacks/{ns}/{name}", s.handleGetStack)
	mux.HandleFunc("PUT /api/v1/stacks/{ns}/{name}", s.handlePutStack)
	mux.HandleFunc("DELETE /api/v1/stacks/{ns}/{name}", s.handleDeleteStack)
	mux.HandleFunc("GET /api/v1/environments", s.handleListEnvironments)
	mux.HandleFunc("GET /api/v1/twin", s.handleTwin)
	mux.HandleFunc("GET /api/v1/events", s.handleEvents)

	return mux
}

// ---------------------------------------------------------------- wire formats

// StackDoc is a Stack as the UI edits it: identity plus the spec, with the
// controller's own status alongside for convenience. This is the document the
// two panes (canvas and YAML) are both projections of.
type StackDoc struct {
	Namespace string             `json:"namespace"`
	Name      string             `json:"name"`
	Spec      paasv1.StackSpec   `json:"spec"`
	Status    paasv1.StackStatus `json:"status,omitempty"`
	// Generation/ResourceVersion let the UI detect that the object moved under it.
	Generation      int64  `json:"generation,omitempty"`
	ResourceVersion string `json:"resourceVersion,omitempty"`
	// ManagedByEnvironment names the Environment that owns this Stack's spec, if any.
	//
	// This matters to an editor: ensureStack assigns `st.Spec = *env.Spec.Stack` on
	// every Environment reconcile, so writing to such a Stack directly is reverted
	// within seconds. The editable document for these is the Environment's embedded
	// stack, not the Stack. Surfaced here so a UI can refuse the write instead of
	// letting it silently disappear.
	ManagedByEnvironment string `json:"managedByEnvironment,omitempty"`
	Project              string `json:"project,omitempty"`
}

// EnvironmentDoc is an Environment: the namespace-isolated instance of a project.
type EnvironmentDoc struct {
	Namespace string                   `json:"namespace"`
	Name      string                   `json:"name"`
	Spec      paasv1.EnvironmentSpec   `json:"spec"`
	Status    paasv1.EnvironmentStatus `json:"status,omitempty"`
}

// ProcessStatus is one process's live replica count, keyed the way the canvas
// draws it (a badge on the owning service node).
type ProcessStatus struct {
	Service string `json:"service"`
	Process string `json:"process"`
	Ready   int32  `json:"ready"`
	Desired int32  `json:"desired"`
}

// BackingStatus is one backing service's live readiness.
type BackingStatus struct {
	Name  string `json:"name"`
	Type  string `json:"type"`
	Ready bool   `json:"ready"`
}

// StackTwin is the live overlay for one Stack: what the canvas paints on top of
// the declared graph.
type StackTwin struct {
	Namespace          string          `json:"namespace"`
	Name               string          `json:"name"`
	Phase              string          `json:"phase,omitempty"`
	ReleaseHook        string          `json:"releaseHook,omitempty"`
	Services           int32           `json:"services"`
	ReadyServices      int32           `json:"readyServices"`
	Generation         int64           `json:"generation,omitempty"`
	ObservedGeneration int64           `json:"observedGeneration,omitempty"`
	Processes          []ProcessStatus `json:"processes"`
	Backing            []BackingStatus `json:"backing"`
}

// ------------------------------------------------------------------- handlers

func (s *Server) handleCatalog(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.Catalog)
}

func (s *Server) handleListStacks(w http.ResponseWriter, r *http.Request) {
	var list paasv1.StackList
	if err := s.Client.List(r.Context(), &list); err != nil {
		writeErr(w, err)
		return
	}
	out := make([]StackDoc, 0, len(list.Items))
	for i := range list.Items {
		out = append(out, stackDoc(&list.Items[i]))
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Namespace != out[j].Namespace {
			return out[i].Namespace < out[j].Namespace
		}
		return out[i].Name < out[j].Name
	})
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) handleGetStack(w http.ResponseWriter, r *http.Request) {
	var stack paasv1.Stack
	key := types.NamespacedName{Namespace: r.PathValue("ns"), Name: r.PathValue("name")}
	if err := s.Client.Get(r.Context(), key, &stack); err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, stackDoc(&stack))
}

// handlePutStack is the canvas's save: create the Stack or replace its spec. The
// UI owns the whole spec document, so this is a replace rather than a merge --
// dropping a service on the canvas has to be able to delete it here.
func (s *Server) handlePutStack(w http.ResponseWriter, r *http.Request) {
	ns, name := r.PathValue("ns"), r.PathValue("name")

	var body struct {
		Spec paasv1.StackSpec `json:"spec"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON: " + err.Error()})
		return
	}

	var existing paasv1.Stack
	err := s.Client.Get(r.Context(), types.NamespacedName{Namespace: ns, Name: name}, &existing)
	switch {
	case apierrors.IsNotFound(err):
		stack := &paasv1.Stack{}
		stack.Namespace, stack.Name = ns, name
		stack.Spec = body.Spec
		if cerr := s.Client.Create(r.Context(), stack); cerr != nil {
			writeErr(w, cerr)
			return
		}
		writeJSON(w, http.StatusCreated, stackDoc(stack))
	case err != nil:
		writeErr(w, err)
	default:
		existing.Spec = body.Spec
		if uerr := s.Client.Update(r.Context(), &existing); uerr != nil {
			writeErr(w, uerr)
			return
		}
		writeJSON(w, http.StatusOK, stackDoc(&existing))
	}
}

func (s *Server) handleDeleteStack(w http.ResponseWriter, r *http.Request) {
	stack := &paasv1.Stack{}
	stack.Namespace, stack.Name = r.PathValue("ns"), r.PathValue("name")
	if err := s.Client.Delete(r.Context(), stack); err != nil {
		writeErr(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleListEnvironments(w http.ResponseWriter, r *http.Request) {
	var list paasv1.EnvironmentList
	if err := s.Client.List(r.Context(), &list); err != nil {
		writeErr(w, err)
		return
	}
	out := make([]EnvironmentDoc, 0, len(list.Items))
	for i := range list.Items {
		e := &list.Items[i]
		out = append(out, EnvironmentDoc{Namespace: e.Namespace, Name: e.Name, Spec: e.Spec, Status: e.Status})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Spec.Project != out[j].Spec.Project {
			return out[i].Spec.Project < out[j].Spec.Project
		}
		return out[i].Name < out[j].Name
	})
	writeJSON(w, http.StatusOK, out)
}

// handleTwin is the one-shot form of the SSE stream: the current live overlay for
// every Stack. The UI fetches this on load, then follows /events.
func (s *Server) handleTwin(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.twins(r.Context()))
}

// handleEvents streams the twin overlay as server-sent events. Each message is
// the full twin list: it is small (one entry per Stack), and a full snapshot per
// message means a reconnecting client needs no replay protocol.
func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "streaming unsupported"})
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	// Proxy buffering would defeat the point of a stream.
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)

	ch, unsubscribe := s.hub.subscribe()
	defer unsubscribe()

	// Prime the connection so the UI paints live state without waiting for the
	// first cluster change.
	if b, err := json.Marshal(s.twins(r.Context())); err == nil {
		fmt.Fprintf(w, "event: twin\ndata: %s\n\n", b)
		flusher.Flush()
	}

	for {
		select {
		case <-r.Context().Done():
			return
		case payload, open := <-ch:
			if !open {
				return
			}
			fmt.Fprintf(w, "event: twin\ndata: %s\n\n", payload)
			flusher.Flush()
		}
	}
}

// ------------------------------------------------------------- twin computation

// twins builds the live overlay for every Stack: per-process replica counts from
// the owning Deployments, and per-backing readiness from whatever that engine's
// tier actually produces.
func (s *Server) twins(ctx context.Context) []StackTwin {
	var list paasv1.StackList
	if err := s.Client.List(ctx, &list); err != nil {
		s.Log.Error(err, "twin: list stacks")
		return []StackTwin{}
	}

	out := make([]StackTwin, 0, len(list.Items))
	for i := range list.Items {
		stack := &list.Items[i]
		t := StackTwin{
			Namespace:          stack.Namespace,
			Name:               stack.Name,
			Phase:              stack.Status.Phase,
			ReleaseHook:        stack.Status.ReleaseHook,
			Services:           stack.Status.Services,
			ReadyServices:      stack.Status.ReadyServices,
			Generation:         stack.Generation,
			ObservedGeneration: stack.Status.ObservedGeneration,
			Processes:          s.processStatuses(ctx, stack),
			Backing:            s.backingStatuses(ctx, stack),
		}
		out = append(out, t)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Namespace != out[j].Namespace {
			return out[i].Namespace < out[j].Namespace
		}
		return out[i].Name < out[j].Name
	})
	return out
}

// processStatuses reads the process Deployments the Stack reconciler stamps with
// paas.sh/{stack,service,process}.
func (s *Server) processStatuses(ctx context.Context, stack *paasv1.Stack) []ProcessStatus {
	var deps appsv1.DeploymentList
	if err := s.Client.List(ctx, &deps,
		client.InNamespace(stack.Namespace),
		client.MatchingLabels{"paas.sh/stack": stack.Name},
	); err != nil {
		s.Log.Error(err, "twin: list deployments", "stack", stack.Name)
		return []ProcessStatus{}
	}

	out := make([]ProcessStatus, 0, len(deps.Items))
	for i := range deps.Items {
		d := &deps.Items[i]
		proc := d.Labels["paas.sh/process"]
		if proc == "" {
			continue // backing StatefulSets and release Jobs are not processes
		}
		desired := int32(1)
		if d.Spec.Replicas != nil {
			desired = *d.Spec.Replicas
		}
		out = append(out, ProcessStatus{
			Service: d.Labels["paas.sh/service"],
			Process: proc,
			Ready:   d.Status.ReadyReplicas,
			Desired: desired,
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Service != out[j].Service {
			return out[i].Service < out[j].Service
		}
		return out[i].Process < out[j].Process
	})
	return out
}

var (
	cnpgClusterGVK = schema.GroupVersionKind{Group: "postgresql.cnpg.io", Version: "v1", Kind: "Cluster"}
	obcGVK         = schema.GroupVersionKind{Group: "objectbucket.io", Version: "v1alpha1", Kind: "ObjectBucketClaim"}
)

// backingStatuses reports readiness per backing service using the same signal the
// reconciler gates on, which differs by tier: CNPG readyInstances for postgres, an
// ObjectBucketClaim phase for object, ReadyReplicas for the StatefulSet tiers.
func (s *Server) backingStatuses(ctx context.Context, stack *paasv1.Stack) []BackingStatus {
	out := make([]BackingStatus, 0, len(stack.Spec.Backing))
	for i := range stack.Spec.Backing {
		b := &stack.Spec.Backing[i]
		name := fmt.Sprintf("%s-%s", stack.Name, b.Name)
		st := BackingStatus{Name: b.Name, Type: b.Type}

		switch {
		case strings.EqualFold(b.Type, "postgres") || strings.EqualFold(b.Type, "postgresql"):
			u := &unstructured.Unstructured{}
			u.SetGroupVersionKind(cnpgClusterGVK)
			if err := s.Client.Get(ctx, types.NamespacedName{Namespace: stack.Namespace, Name: name}, u); err == nil {
				ready, _, _ := unstructured.NestedInt64(u.Object, "status", "readyInstances")
				st.Ready = ready >= 1
			}
		case strings.EqualFold(b.Type, "object") || strings.EqualFold(b.Type, "s3"):
			u := &unstructured.Unstructured{}
			u.SetGroupVersionKind(obcGVK)
			if err := s.Client.Get(ctx, types.NamespacedName{Namespace: stack.Namespace, Name: name}, u); err == nil {
				phase, _, _ := unstructured.NestedString(u.Object, "status", "phase")
				st.Ready = phase == "Bound"
			}
		default:
			var ss appsv1.StatefulSet
			if err := s.Client.Get(ctx, types.NamespacedName{Namespace: stack.Namespace, Name: name}, &ss); err == nil {
				st.Ready = ss.Status.ReadyReplicas >= 1
			}
		}
		out = append(out, st)
	}
	return out
}

// watchForTwinUpdates marks the hub dirty whenever anything the overlay reflects
// changes. Stacks carry the phase and release-hook state; Deployments carry the
// per-process replica counts. Both informers already exist because the
// reconcilers watch them, so this adds watches, not API load.
func (s *Server) watchForTwinUpdates(ctx context.Context) error {
	handler := cache.ResourceEventHandlerFuncs{
		AddFunc:    func(any) { s.hub.markDirty() },
		UpdateFunc: func(any, any) { s.hub.markDirty() },
		DeleteFunc: func(any) { s.hub.markDirty() },
	}
	for _, obj := range []client.Object{&paasv1.Stack{}, &appsv1.Deployment{}} {
		informer, err := s.Cache.GetInformer(ctx, obj)
		if err != nil {
			return err
		}
		if _, err := informer.AddEventHandler(handler); err != nil {
			return err
		}
	}
	return nil
}

// ------------------------------------------------------------------------- hub

// hub fans one coalesced twin snapshot out to every SSE subscriber. Cluster
// churn is bursty (a rollout touches many Deployments in a second), so events
// are debounced into at most one recompute per window rather than one per event.
type hub struct {
	log   logr.Logger
	dirty chan struct{}

	mu   sync.Mutex
	subs map[chan []byte]struct{}
}

const (
	// debounce is how long to wait for a burst of cluster events to settle.
	debounce = 300 * time.Millisecond
	// heartbeat re-sends the snapshot even when nothing changed, so a client that
	// missed an event (or a proxy that dropped one) self-heals.
	heartbeat = 15 * time.Second
)

func newHub(log logr.Logger) *hub {
	return &hub{
		log:   log,
		dirty: make(chan struct{}, 1),
		subs:  map[chan []byte]struct{}{},
	}
}

func (h *hub) markDirty() {
	select {
	case h.dirty <- struct{}{}:
	default: // already pending; the coalesced recompute will pick this up
	}
}

func (h *hub) subscribe() (<-chan []byte, func()) {
	// Buffered: a slow reader drops stale snapshots instead of stalling the hub.
	ch := make(chan []byte, 1)
	h.mu.Lock()
	h.subs[ch] = struct{}{}
	h.mu.Unlock()

	return ch, func() {
		h.mu.Lock()
		delete(h.subs, ch)
		close(ch)
		h.mu.Unlock()
	}
}

func (h *hub) run(ctx context.Context, snapshot func() []StackTwin) {
	ticker := time.NewTicker(heartbeat)
	defer ticker.Stop()

	var timer *time.Timer
	var timerC <-chan time.Time

	broadcast := func() {
		h.mu.Lock()
		n := len(h.subs)
		h.mu.Unlock()
		if n == 0 {
			return // nobody watching: skip the recompute entirely
		}

		payload, err := json.Marshal(snapshot())
		if err != nil {
			h.log.Error(err, "twin: marshal snapshot")
			return
		}

		h.mu.Lock()
		defer h.mu.Unlock()
		for ch := range h.subs {
			select {
			case ch <- payload:
			default:
				// Subscriber has an unread snapshot; replace it with the fresher one.
				select {
				case <-ch:
				default:
				}
				select {
				case ch <- payload:
				default:
				}
			}
		}
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-h.dirty:
			if timer == nil {
				timer = time.NewTimer(debounce)
				timerC = timer.C
			}
		case <-timerC:
			timer, timerC = nil, nil
			broadcast()
		case <-ticker.C:
			broadcast()
		}
	}
}

// ---------------------------------------------------------------------- helpers

func stackDoc(s *paasv1.Stack) StackDoc {
	return StackDoc{
		Namespace:       s.Namespace,
		Name:            s.Name,
		Spec:            s.Spec,
		Status:          s.Status,
		Generation:      s.Generation,
		ResourceVersion: s.ResourceVersion,
		// Stamped by ensureStack. There is no owner reference to read instead: the
		// Environment lives in the control-plane namespace and the Stack in the
		// tenant one, so teardown is by namespace deletion, not ownership.
		ManagedByEnvironment: s.Labels["paas.sh/environment"],
		Project:              s.Labels["paas.sh/project"],
	}
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		// Header is already sent; nothing useful left to do but stop.
		return
	}
}

// writeErr maps apimachinery errors onto their HTTP equivalents so the UI can
// tell "you renamed a stack that no longer exists" from "the apiserver is down".
func writeErr(w http.ResponseWriter, err error) {
	code := http.StatusInternalServerError
	switch {
	case apierrors.IsNotFound(err):
		code = http.StatusNotFound
	case apierrors.IsConflict(err):
		code = http.StatusConflict
	case apierrors.IsInvalid(err), apierrors.IsBadRequest(err):
		code = http.StatusBadRequest
	case apierrors.IsForbidden(err), apierrors.IsUnauthorized(err):
		code = http.StatusForbidden
	case apierrors.IsAlreadyExists(err):
		code = http.StatusConflict
	}
	writeJSON(w, code, map[string]string{"error": err.Error()})
}
