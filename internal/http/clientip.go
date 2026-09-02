package http

import (
	"net"
	nethttp "net/http"
	"strings"
)

// clientIP replaces chi's middleware.RealIP with a bounded, configurable
// trusted-hop parser of X-Forwarded-For. See docs/DECISIONS.md D19 for why
// RealIP is deprecated upstream as a security issue rather than merely an old
// API, and why the fix waited on a deployment topology that did not exist yet.
//
// hops=0 -- today's actual deployment, nothing sits in front of this service --
// means every X-Forwarded-For entry is IGNORED OUTRIGHT and the resolved
// address is the raw TCP peer alone, which a caller cannot forge without
// controlling the network path itself. That is the safe default: it closes the
// vulnerability without guessing a topology that is not there, which is
// exactly the trap the original deferral was trying to avoid.
//
// X-Real-IP and True-Client-IP are never read, at any hop count. Both are
// single-value headers with no chaining semantics -- a proxy that sets one is
// trusted to have overwritten whatever the client sent, a claim this
// middleware has no way to verify. X-Forwarded-For's comma-separated chain is
// the one signal whose trustworthy prefix can actually be computed from a hop
// count, which is why it is the only header this bounds.
func clientIP(hops int) func(nethttp.Handler) nethttp.Handler {
	return func(next nethttp.Handler) nethttp.Handler {
		return nethttp.HandlerFunc(func(w nethttp.ResponseWriter, r *nethttp.Request) {
			r.RemoteAddr = resolveClientIP(r, hops)
			next.ServeHTTP(w, r)
		})
	}
}

// resolveClientIP applies the N-trusted-hops algorithm.
//
// Each proxy between the client and this service appends the address it
// received the request FROM to the right of X-Forwarded-For. With hops
// trusted proxies actually in the path, the real client's address sits exactly
// hops positions from the right. Anything further left was written by the
// client itself, or by a proxy this deployment does not vouch for, and is
// therefore attacker-controlled.
func resolveClientIP(r *nethttp.Request, hops int) string {
	socketIP := hostOf(r.RemoteAddr)

	if hops <= 0 {
		return socketIP
	}

	raw := r.Header.Get("X-Forwarded-For")
	if raw == "" {
		return socketIP
	}

	entries := strings.Split(raw, ",")
	for i := range entries {
		entries[i] = strings.TrimSpace(entries[i])
	}

	// Fewer entries than trusted hops means the chain is shorter than this
	// deployment's own topology says it should be -- a hop failed to append,
	// or the header was tampered with in a way that removed segments. Guessing
	// which prefix is real in that case is exactly the mistake D19 exists to
	// avoid, so this falls back to the one address that cannot be forged: the
	// immediate TCP peer.
	if len(entries) < hops {
		return socketIP
	}

	client := entries[len(entries)-hops]
	if net.ParseIP(client) == nil {
		// Whatever the trusted hop appended does not parse as an address --
		// proxy misconfiguration, or an attacker who found a way to inject a
		// comma. Either way this is not a value to act on.
		return socketIP
	}
	return client
}

// hostOf strips the port net/http always attaches to RemoteAddr, so this
// middleware's output is a bare address regardless of which branch produced
// it. Chi's RealIP left this inconsistent -- bare on the spoofed-header path,
// host:port otherwise -- which is its own small foot-gun for anything
// downstream that expects one shape.
func hostOf(remoteAddr string) string {
	host, _, err := net.SplitHostPort(remoteAddr)
	if err != nil {
		return remoteAddr
	}
	return host
}
