package http

import (
	nethttp "net/http"
	"net/http/httptest"
	"testing"
)

// TestClientIP_IgnoresSpoofedHeadersByDefault is the test that would have
// caught D19: chi's RealIP trusted X-Forwarded-For, X-Real-IP and
// True-Client-IP unconditionally, so any caller could assert any source
// address. At the default hop count of zero, none of that is read at all.
func TestClientIP_IgnoresSpoofedHeadersByDefault(t *testing.T) {
	t.Parallel()

	req := httptest.NewRequestWithContext(t.Context(), "GET", "/", nil)
	req.RemoteAddr = "203.0.113.9:54321"
	req.Header.Set("X-Forwarded-For", "10.0.0.1")
	req.Header.Set("X-Real-IP", "10.0.0.2")
	req.Header.Set("True-Client-IP", "10.0.0.3")

	got := resolveClientIP(req, 0)
	if got != "203.0.113.9" {
		t.Fatalf("hops=0: got %q, want the raw TCP peer 203.0.113.9 -- a spoofed header was trusted", got)
	}
}

// TestClientIP_TrustsExactlyTheConfiguredHopCount is the positive case: with a
// real, configured topology, the client's address is extracted correctly and
// everything an attacker could have prepended is ignored.
func TestClientIP_TrustsExactlyTheConfiguredHopCount(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		xff  string
		hops int
		peer string
		want string
	}{
		{
			name: "should trust the rightmost entry when exactly one hop is configured",
			// Client sent a spoofed leftmost value; the one trusted proxy in
			// front appended the address it actually observed.
			xff:  "198.51.100.9, 203.0.113.55",
			hops: 1,
			peer: "10.0.0.1:443",
			want: "203.0.113.55",
		},
		{
			name: "should skip the attacker-controlled prefix when two hops are configured",
			xff:  "198.51.100.9, 203.0.113.55, 172.16.0.9",
			hops: 2,
			peer: "10.0.0.1:443",
			want: "203.0.113.55",
		},
		{
			name: "should fall back to the socket peer when the chain is shorter than the configured hop count",
			// A misconfiguration or a stripped header must not be guessed at.
			xff:  "203.0.113.55",
			hops: 2,
			peer: "10.0.0.1:443",
			want: "10.0.0.1",
		},
		{
			name: "should fall back to the socket peer when no header is present at all",
			xff:  "",
			hops: 1,
			peer: "10.0.0.1:443",
			want: "10.0.0.1",
		},
		{
			name: "should fall back to the socket peer when the trusted entry is not a valid address",
			xff:  "203.0.113.55, not-an-ip",
			hops: 1,
			peer: "10.0.0.1:443",
			want: "10.0.0.1",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequestWithContext(t.Context(), "GET", "/", nil)
			req.RemoteAddr = tc.peer
			if tc.xff != "" {
				req.Header.Set("X-Forwarded-For", tc.xff)
			}

			got := resolveClientIP(req, tc.hops)
			if got != tc.want {
				t.Fatalf("got %q, want %q", got, tc.want)
			}
		})
	}
}

// TestClientIP_MiddlewareStripsThePortForEveryBranch confirms the middleware's
// output shape never varies -- chi's RealIP left this inconsistent (bare on
// the spoofed-header path, host:port otherwise), which is its own foot-gun for
// anything downstream that expects one shape.
func TestClientIP_MiddlewareStripsThePortForEveryBranch(t *testing.T) {
	t.Parallel()

	handlerRan := false
	var seen string

	mw := clientIP(0)
	wrapped := mw(nethttp.HandlerFunc(func(_ nethttp.ResponseWriter, r *nethttp.Request) {
		seen = r.RemoteAddr
		handlerRan = true
	}))

	req := httptest.NewRequestWithContext(t.Context(), "GET", "/", nil)
	req.RemoteAddr = "203.0.113.9:54321"
	rec := httptest.NewRecorder()
	wrapped.ServeHTTP(rec, req)

	if !handlerRan {
		t.Fatal("the wrapped handler never ran")
	}
	if seen != "203.0.113.9" {
		t.Fatalf("got %q, want the bare address with no port", seen)
	}
}
