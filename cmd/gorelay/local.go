package main

import (
	"errors"
	"net"
	"net/http"
	"strconv"
	"strings"
)

// The unauthenticated stage-2 service is restricted to literal loopback binds.
// Hostnames are deliberately excluded from listener configuration so DNS cannot
// widen the listener's address without a configuration change.
func validateLocalAddr(addr string) error {
	host, port, err := net.SplitHostPort(addr)
	if err != nil || !validLocalPort(port) {
		return errors.New("GORELAY_ADDR must be a literal loopback IP and numeric port")
	}
	ip := net.ParseIP(host)
	if ip == nil || !ip.IsLoopback() {
		return errors.New("GORELAY_ADDR must bind a loopback IP")
	}
	return nil
}

func validLocalPort(port string) bool {
	if port == "" {
		return false
	}
	for _, c := range port {
		if c < '0' || c > '9' {
			return false
		}
	}
	n, err := strconv.Atoi(port)
	return err == nil && n <= 65535
}

func isLocalHost(authority string) bool {
	host := authority
	if strings.HasPrefix(authority, "[") && strings.HasSuffix(authority, "]") {
		host = authority[1 : len(authority)-1]
	} else if strings.Contains(authority, ":") {
		var port string
		var err error
		host, port, err = net.SplitHostPort(authority)
		if err != nil || !validLocalPort(port) {
			return false
		}
	}
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// Host validation prevents arbitrary DNS names pointing at loopback from reaching
// this unauthenticated API. Origin-bearing browser requests are unsupported in
// the CLI-only stage, including requests whose Origin claims to be local.
func localOnly(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		if !isLocalHost(r.Host) || len(r.Header.Values("Origin")) != 0 {
			eventError(w, http.StatusForbidden, "local CLI access required")
			return
		}
		next.ServeHTTP(w, r)
	})
}
