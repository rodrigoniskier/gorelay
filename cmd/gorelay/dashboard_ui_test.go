package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestDashboardUIAssets(t *testing.T) {
	mux := http.NewServeMux()
	registerDashboardUI(mux)
	for _, path := range []string{"/dashboard/", "/dashboard/app.css", "/dashboard/app.js"} {
		t.Run(path, func(t *testing.T) {
			w := httptest.NewRecorder()
			mux.ServeHTTP(w, httptest.NewRequest("GET", path, nil))
			if w.Code != 200 {
				t.Fatalf("status %d", w.Code)
			}
			if w.Body.Len() == 0 {
				t.Fatal("empty asset")
			}
			if !strings.Contains(w.Header().Get("Content-Security-Policy"), "frame-ancestors 'none'") {
				t.Fatal("missing restrictive CSP")
			}
			if w.Header().Get("Cache-Control") != "no-store" {
				t.Fatal("missing no-store")
			}
		})
	}
	for _, path := range []string{"/dashboard/not-found", "/dashboard/dashboard_assets/index.html"} {
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, httptest.NewRequest("GET", path, nil))
		if w.Code != 404 {
			t.Fatalf("%s: %d", path, w.Code)
		}
	}
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, httptest.NewRequest("POST", "/dashboard/", nil))
	if w.Code != 405 {
		t.Fatalf("POST: %d", w.Code)
	}
}

func TestDashboardHTMLSemantics(t *testing.T) {
	page, err := dashboardAssets.ReadFile("dashboard_assets/index.html")
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{"lang=\"en\"", "Skip to dashboard", "aria-live=\"polite\"", "type=\"number\" min=\"1\" max=\"100\"", "does not guarantee successful receipt", "not automatically re-driven", "payloads hidden"} {
		if !strings.Contains(string(page), required) {
			t.Errorf("missing %s", required)
		}
	}
	for _, forbidden := range []string{"https://", "http://", "<script>", "onclick="} {
		if strings.Contains(string(page), forbidden) {
			t.Errorf("unexpected remote or inline content %s", forbidden)
		}
	}
	script, _ := dashboardAssets.ReadFile("dashboard_assets/app.js")
	if strings.Contains(string(script), "innerHTML") {
		t.Fatal("untrusted data must not render as HTML")
	}
}
