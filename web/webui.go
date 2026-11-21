// Package webui embeds the built KeyShift dashboard (web/dist).
//
// Run `make web` (or `npm ci && npm run build` in web/) before building the
// controller to include the dashboard. Without a build, the controller
// serves its API and reports that the dashboard assets are missing.
package webui

import (
	"embed"
	"io/fs"
)

//go:embed all:dist
var dist embed.FS

// FS returns the dashboard file system rooted at dist.
func FS() fs.FS {
	sub, err := fs.Sub(dist, "dist")
	if err != nil {
		panic(err)
	}
	return sub
}

// Built reports whether the dashboard was built into the binary.
func Built() bool {
	_, err := fs.Stat(FS(), "index.html")
	return err == nil
}
