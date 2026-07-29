package controller

import "sort"

// The API-facing view of the backing-service catalog.
//
// The Stack CRD keeps connection types *implicit*: a ConnectionSpec is only
// {to, as}, and reconcileBacking derives the engine from the target's
// backing[].type. Anything that has to draw the graph (the canvas UI) needs the
// same type->binding knowledge, so it is projected here rather than duplicated
// in the frontend.
//
// Tiers mirror reconcileBacking's dispatch order exactly: operator adapters
// first, the object/S3 special case, then the template descriptors, with the
// generic image+port fallback for anything uncataloged.

// CatalogEntry describes one offerable backing-service type.
type CatalogEntry struct {
	Type    string   `json:"type"`
	Aliases []string `json:"aliases,omitempty"`
	// Tier is operator|object|template: how reconcileBacking provisions this type.
	Tier           string `json:"tier"`
	DefaultVersion string `json:"defaultVersion,omitempty"`
	// Port is the server port. Zero for the object tier (no listener of ours).
	Port int32 `json:"port,omitempty"`
	// DefaultEnv is the conventional `as` for a connection to this type: the env
	// var the consumer's connection URL/DSN lands in when the edge omits `as`.
	DefaultEnv string `json:"defaultEnv,omitempty"`
	// Extra are the discrete env vars a consumer also receives alongside DefaultEnv.
	Extra []string `json:"extra,omitempty"`
	// Persistent reports whether provisioning claims a data volume.
	Persistent bool `json:"persistent"`
	// ManagedBackup reports engine-aware backups (backing[].backup). Types without
	// it fall back to CSI volume snapshots when they are persistent.
	ManagedBackup bool `json:"managedBackup"`
}

// operatorTierEntries are the hand-written adapters: types whose HA, failover,
// backup and upgrade behaviour needs more than a template descriptor can say.
// Keep in sync with reconcileBacking's leading switch cases.
func operatorTierEntries() []CatalogEntry {
	return []CatalogEntry{
		{
			Type:    "postgres",
			Aliases: []string{"postgresql"},
			Tier:    "operator",
			// From the adapter's own pin, not a literal: the palette must only ever
			// offer a version that apps/image-mirror actually mirrors, and the two
			// have to move together.
			DefaultVersion: pgDefaultVersion,
			Port:           5432,
			DefaultEnv:     "DATABASE_URL",
			Extra:          []string{"PGHOST", "PGPORT", "PGDATABASE", "PGUSER", "PGPASSWORD"},
			Persistent:     true,
			ManagedBackup:  true,
		},
		{
			Type:       "object",
			Aliases:    []string{"s3"},
			Tier:       "object",
			DefaultEnv: "S3_ENDPOINT",
			Extra:      []string{"S3_ENDPOINT", "S3_BUCKET", "S3_REGION", "S3_ACCESS_KEY_ID", "S3_SECRET_ACCESS_KEY"},
		},
	}
}

// Entries returns the whole catalog as API projections, sorted by type. Aliases
// are collapsed back onto their primary definition so each engine appears once.
func (c *serviceCatalog) Entries() []CatalogEntry {
	out := operatorTierEntries()

	// byType indexes aliases at the same *ServiceDefinition, so dedupe by pointer.
	seen := map[*ServiceDefinition]bool{}
	for _, def := range c.byType {
		if seen[def] {
			continue
		}
		seen[def] = true
		e := CatalogEntry{
			Type:           def.Type,
			Aliases:        def.Aliases,
			Tier:           "template",
			DefaultVersion: def.DefaultVersion,
			Port:           def.Port,
			DefaultEnv:     templateDefaultEnv(def),
			Persistent:     def.Persistence != nil,
		}
		for env := range def.Binding.Extra {
			e.Extra = append(e.Extra, env)
		}
		sort.Strings(e.Extra)
		out = append(out, e)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Type < out[j].Type })
	return out
}

// templateDefaultEnv is the conventional consumer env for a template-backed type.
// Descriptors name the *secret key* the URL lives under (binding.primaryKey), not
// the env var, because the env var is the edge's `as`. For the canvas we still
// want a sensible pre-fill, so map the well-known engines and otherwise derive
// <TYPE>_URL.
func templateDefaultEnv(def *ServiceDefinition) string {
	switch def.Type {
	case "valkey":
		// Kept as REDIS_URL for app compatibility: every client library and every
		// upstream app config expects that name, Valkey fork or not.
		return "REDIS_URL"
	case "memcached":
		return "MEMCACHED_SERVERS"
	case "nats":
		return "NATS_URL"
	}
	return upperSnake(def.Type) + "_URL"
}

func upperSnake(s string) string {
	b := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		ch := s[i]
		switch {
		case ch >= 'a' && ch <= 'z':
			b = append(b, ch-32)
		case ch >= 'A' && ch <= 'Z' || ch >= '0' && ch <= '9':
			b = append(b, ch)
		default:
			b = append(b, '_')
		}
	}
	return string(b)
}
