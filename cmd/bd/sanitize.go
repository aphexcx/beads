package main

import "strings"

// sanitizeDBName replaces hyphens and dots with underscores for
// SQL-idiomatic embedded Dolt database names (GH#2142, GH#3231).
//
// Lives in its own file (no build tag) so callers outside the
// cgo-only store_factory.go can reach it under CGO_ENABLED=0 lint.
func sanitizeDBName(name string) string {
	name = strings.ReplaceAll(name, "-", "_")
	name = strings.ReplaceAll(name, ".", "_")
	return name
}
