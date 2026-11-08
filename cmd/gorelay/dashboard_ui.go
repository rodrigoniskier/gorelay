package main

import (
	"embed"
	"net/http"
)

//go:embed dashboard_assets/*
var dashboardAssets embed.FS

func registerDashboardUI(mux *http.ServeMux) {
	for _, asset := range []struct{ path, file, media string }{
		{"/dashboard/", "index.html", "text/html; charset=utf-8"},
		{"/dashboard/app.css", "app.css", "text/css; charset=utf-8"},
		{"/dashboard/app.js", "app.js", "text/javascript; charset=utf-8"},
	} {
		mux.HandleFunc("GET "+asset.path, func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != asset.path {
				http.NotFound(w, r)
				return
			}
			content, err := dashboardAssets.ReadFile("dashboard_assets/" + asset.file)
			if err != nil {
				http.Error(w, "asset unavailable", 500)
				return
			}
			w.Header().Set("Content-Type", asset.media)
			w.Header().Set("Content-Security-Policy", "default-src 'none'; script-src 'self'; style-src 'self'; connect-src 'self'; img-src 'self'; base-uri 'none'; frame-ancestors 'none'; form-action 'self'")
			w.Header().Set("X-Frame-Options", "DENY")
			w.Header().Set("Referrer-Policy", "no-referrer")
			w.Header().Set("Cache-Control", "no-store")
			w.Header().Set("X-Content-Type-Options", "nosniff")
			w.Write(content)
		})
	}
}
