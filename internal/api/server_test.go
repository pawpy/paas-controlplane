package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	paasv1 "github.com/pawpy/paas-controlplane/api/v1alpha1"
	"github.com/pawpy/paas-controlplane/internal/controller"
)

func testScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(s); err != nil {
		t.Fatalf("clientgo scheme: %v", err)
	}
	if err := paasv1.AddToScheme(s); err != nil {
		t.Fatalf("paas scheme: %v", err)
	}
	return s
}

// chatwootStack mirrors the shape the design doc's example Stack has: one service
// with a web + worker process, a release hook, and edges to postgres/valkey/object.
func chatwootStack() *paasv1.Stack {
	return &paasv1.Stack{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "chatwoot",
			Namespace:  "proj-chatwoot-prod",
			Generation: 3,
			// As ensureStack stamps them: this Stack's spec is owned by an Environment.
			Labels: map[string]string{"paas.sh/project": "chatwoot", "paas.sh/environment": "prod"},
		},
		Spec: paasv1.StackSpec{
			Services: []paasv1.ServiceSpec{{
				Name:  "web",
				Image: "registry.local/chatwoot:v1",
				Processes: []paasv1.ProcessSpec{
					{Name: "web", Kind: "web", Port: 3000, Replicas: 2},
					{Name: "worker", Kind: "worker", Replicas: 1},
				},
				Hooks: &paasv1.Hooks{Release: "bundle exec rails db:prepare"},
				Connections: []paasv1.ConnectionSpec{
					{To: "postgres", As: "DATABASE_URL"},
					{To: "redis", As: "REDIS_URL"},
					{To: "uploads", As: "S3"},
				},
			}},
			Backing: []paasv1.BackingSpec{
				{Name: "postgres", Type: "postgres", Version: "16", Disk: "20Gi"},
				{Name: "redis", Type: "valkey", Version: "8"},
				{Name: "uploads", Type: "object"},
			},
		},
		Status: paasv1.StackStatus{Phase: "Running", Services: 1, ReadyServices: 1, ObservedGeneration: 3},
	}
}

func newTestServer(t *testing.T, objs ...runtime.Object) *Server {
	t.Helper()
	cat, err := controller.LoadBuiltinCatalog()
	if err != nil {
		t.Fatalf("catalog: %v", err)
	}
	c := fake.NewClientBuilder().WithScheme(testScheme(t)).WithRuntimeObjects(objs...).Build()
	return &Server{Client: c, Catalog: cat.Entries(), Log: logr.Discard()}
}

func do(t *testing.T, s *Server, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	var r *http.Request
	if body == "" {
		r = httptest.NewRequest(method, path, nil)
	} else {
		r = httptest.NewRequest(method, path, strings.NewReader(body))
	}
	w := httptest.NewRecorder()
	s.routes().ServeHTTP(w, r)
	return w
}

func TestCatalogCoversEveryTier(t *testing.T) {
	s := newTestServer(t)
	w := do(t, s, "GET", "/api/v1/catalog", "")
	if w.Code != http.StatusOK {
		t.Fatalf("code = %d, want 200", w.Code)
	}

	var entries []controller.CatalogEntry
	if err := json.Unmarshal(w.Body.Bytes(), &entries); err != nil {
		t.Fatalf("decode: %v", err)
	}

	byType := map[string]controller.CatalogEntry{}
	for _, e := range entries {
		byType[e.Type] = e
	}

	// The operator tier, the object special case and the template descriptors must
	// all be offerable, or the canvas palette silently loses engines.
	for _, want := range []string{"postgres", "object", "valkey", "memcached", "nats"} {
		if _, ok := byType[want]; !ok {
			t.Errorf("catalog missing %q (have %v)", want, keys(byType))
		}
	}

	if got := byType["postgres"]; got.Tier != "operator" || got.DefaultEnv != "DATABASE_URL" || !got.ManagedBackup {
		t.Errorf("postgres = %+v, want operator tier / DATABASE_URL / managed backup", got)
	}
	// Valkey must advertise REDIS_URL: apps expect that name regardless of the fork.
	if got := byType["valkey"]; got.DefaultEnv != "REDIS_URL" {
		t.Errorf("valkey defaultEnv = %q, want REDIS_URL", got.DefaultEnv)
	}
	// redis is an alias, not a separate entry, so the palette shows one cache engine.
	if _, ok := byType["redis"]; ok {
		t.Error("redis should be an alias of valkey, not its own entry")
	}
	if got := byType["valkey"]; len(got.Aliases) == 0 || got.Aliases[0] != "redis" {
		t.Errorf("valkey aliases = %v, want [redis]", got.Aliases)
	}
	if got := byType["object"]; got.Tier != "object" || got.DefaultEnv != "S3_ENDPOINT" {
		t.Errorf("object = %+v, want object tier / S3_ENDPOINT", got)
	}
}

func TestListAndGetStack(t *testing.T) {
	s := newTestServer(t, chatwootStack())

	w := do(t, s, "GET", "/api/v1/stacks", "")
	if w.Code != http.StatusOK {
		t.Fatalf("list code = %d, want 200", w.Code)
	}
	var docs []StackDoc
	if err := json.Unmarshal(w.Body.Bytes(), &docs); err != nil {
		t.Fatalf("decode list: %v", err)
	}
	if len(docs) != 1 || docs[0].Name != "chatwoot" {
		t.Fatalf("list = %+v, want one chatwoot", docs)
	}
	if len(docs[0].Spec.Services[0].Processes) != 2 {
		t.Errorf("processes = %d, want 2 (web + worker)", len(docs[0].Spec.Services[0].Processes))
	}

	w = do(t, s, "GET", "/api/v1/stacks/proj-chatwoot-prod/chatwoot", "")
	if w.Code != http.StatusOK {
		t.Fatalf("get code = %d, want 200", w.Code)
	}
	var doc StackDoc
	if err := json.Unmarshal(w.Body.Bytes(), &doc); err != nil {
		t.Fatalf("decode get: %v", err)
	}
	if doc.Status.Phase != "Running" || doc.Generation != 3 {
		t.Errorf("doc = %+v, want phase Running / generation 3", doc)
	}
	if len(doc.Spec.Backing) != 3 {
		t.Errorf("backing = %d, want 3", len(doc.Spec.Backing))
	}
	// An editor has to know this: writing to an Environment-owned Stack is reverted
	// on the next Environment reconcile.
	if doc.ManagedByEnvironment != "prod" || doc.Project != "chatwoot" {
		t.Errorf("ownership = %q/%q, want prod/chatwoot", doc.ManagedByEnvironment, doc.Project)
	}

	// A standalone Stack must NOT claim an owner, or the UI blocks edits it should allow.
	standalone := &paasv1.Stack{}
	standalone.Name, standalone.Namespace = "solo", "tenants"
	s2 := newTestServer(t, standalone)
	w2 := do(t, s2, "GET", "/api/v1/stacks/tenants/solo", "")
	var solo StackDoc
	if err := json.Unmarshal(w2.Body.Bytes(), &solo); err != nil {
		t.Fatalf("decode solo: %v", err)
	}
	if solo.ManagedByEnvironment != "" {
		t.Errorf("standalone stack claims owner %q", solo.ManagedByEnvironment)
	}

	if w := do(t, s, "GET", "/api/v1/stacks/proj-chatwoot-prod/nope", ""); w.Code != http.StatusNotFound {
		t.Errorf("missing stack code = %d, want 404", w.Code)
	}
}

func TestPutStackCreatesThenReplacesSpec(t *testing.T) {
	s := newTestServer(t)

	body := `{"spec":{"services":[{"name":"web","image":"registry.local/demo:v1",
	          "processes":[{"name":"web","port":8080,"replicas":1}]}],
	          "backing":[{"name":"db","type":"postgres"}]}}`
	w := do(t, s, "PUT", "/api/v1/stacks/proj-demo-prod/demo", body)
	if w.Code != http.StatusCreated {
		t.Fatalf("create code = %d, want 201: %s", w.Code, w.Body)
	}

	// A canvas save is a replace: deleting the backing node must delete it here too,
	// which a merge patch would not do.
	w = do(t, s, "PUT", "/api/v1/stacks/proj-demo-prod/demo",
		`{"spec":{"services":[{"name":"web","image":"registry.local/demo:v2"}]}}`)
	if w.Code != http.StatusOK {
		t.Fatalf("update code = %d, want 200: %s", w.Code, w.Body)
	}
	var doc StackDoc
	if err := json.Unmarshal(w.Body.Bytes(), &doc); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(doc.Spec.Backing) != 0 {
		t.Errorf("backing = %+v, want removed by the replace", doc.Spec.Backing)
	}
	if doc.Spec.Services[0].Image != "registry.local/demo:v2" {
		t.Errorf("image = %q, want v2", doc.Spec.Services[0].Image)
	}

	if w := do(t, s, "PUT", "/api/v1/stacks/proj-demo-prod/demo", `{"spec":`); w.Code != http.StatusBadRequest {
		t.Errorf("malformed body code = %d, want 400", w.Code)
	}
	if w := do(t, s, "DELETE", "/api/v1/stacks/proj-demo-prod/demo", ""); w.Code != http.StatusNoContent {
		t.Errorf("delete code = %d, want 204", w.Code)
	}
}

func TestTwinReportsProcessRepliasAndBackingReadiness(t *testing.T) {
	two := int32(2)
	webDep := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "chatwoot-web-web",
			Namespace: "proj-chatwoot-prod",
			Labels: map[string]string{
				"paas.sh/stack": "chatwoot", "paas.sh/service": "web", "paas.sh/process": "web",
			},
		},
		Spec:   appsv1.DeploymentSpec{Replicas: &two},
		Status: appsv1.DeploymentStatus{ReadyReplicas: 1},
	}
	// A backing StatefulSet carries no paas.sh/process label and must not appear as
	// a process, or the canvas draws replica badges on database nodes.
	valkeySts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "chatwoot-redis",
			Namespace: "proj-chatwoot-prod",
			Labels:    map[string]string{"paas.sh/stack": "chatwoot", "paas.sh/backing": "redis"},
		},
		Status: appsv1.StatefulSetStatus{ReadyReplicas: 1},
	}

	s := newTestServer(t, chatwootStack(), webDep, valkeySts)
	w := do(t, s, "GET", "/api/v1/twin", "")
	if w.Code != http.StatusOK {
		t.Fatalf("code = %d, want 200", w.Code)
	}

	var twins []StackTwin
	if err := json.Unmarshal(w.Body.Bytes(), &twins); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(twins) != 1 {
		t.Fatalf("twins = %d, want 1", len(twins))
	}
	tw := twins[0]

	if len(tw.Processes) != 1 {
		t.Fatalf("processes = %+v, want only the web Deployment", tw.Processes)
	}
	if p := tw.Processes[0]; p.Service != "web" || p.Process != "web" || p.Ready != 1 || p.Desired != 2 {
		t.Errorf("process = %+v, want web/web 1 of 2", p)
	}

	byName := map[string]BackingStatus{}
	for _, b := range tw.Backing {
		byName[b.Name] = b
	}
	if len(byName) != 3 {
		t.Fatalf("backing = %+v, want 3 entries", tw.Backing)
	}
	// Only the StatefulSet-backed one exists in this fixture; postgres (CNPG) and
	// uploads (OBC) are absent, so they must report not-ready rather than error.
	if !byName["redis"].Ready {
		t.Error("redis should be ready (StatefulSet has a ready replica)")
	}
	if byName["postgres"].Ready {
		t.Error("postgres should not be ready (no CNPG Cluster in the fixture)")
	}
	if byName["uploads"].Ready {
		t.Error("uploads should not be ready (no ObjectBucketClaim in the fixture)")
	}
	if tw.Phase != "Running" || tw.Generation != 3 || tw.ObservedGeneration != 3 {
		t.Errorf("twin meta = %+v, want Running / gen 3 observed 3", tw)
	}
}

func keys(m map[string]controller.CatalogEntry) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
