// Package migrations embeds the SQL migration files for the saga-orchestrator.
package migrations

import "embed"

// FS holds all SQL migration files embedded into the binary.
//
//go:embed *.sql
var FS embed.FS
