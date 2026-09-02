package http

import (
	nethttp "net/http"
	"time"

	"github.com/satyamsipah/ledger-core/internal/clock"
)

// HandleClockSkew reads or sets internal/clock's process-wide offset.
//
// This exists for exactly one caller: cmd/chaos-harness. Deliberately NOT
// part of Deps/NewMux -- every route registered there is either on the
// public API (cmd/api's own listener) or reachable on a worker's one and
// only listener, and this must never be either. Every cmd/*/main.go that
// wires this mounts it directly on the admin/metrics mux, standalone, and
// only when LEDGER_FAULT_INJECTION_ENABLED=true. A clock a stranger on the
// internet could skew is not a chaos-testing feature, it is a vulnerability
// wearing one.
//
// GET reports the current offset; POST sets it from a JSON body
// {"offset_seconds": N}. There is no DELETE: POSTing {"offset_seconds": 0}
// is the reset, and giving "clear" a second shape to parse would be the only
// thing this handler does that is not already the simplest way to say it.
func HandleClockSkew() nethttp.HandlerFunc {
	type body struct {
		OffsetSeconds float64 `json:"offset_seconds"`
	}
	type response struct {
		OffsetSeconds float64 `json:"offset_seconds"`
	}

	return func(w nethttp.ResponseWriter, r *nethttp.Request) {
		if r.Method == nethttp.MethodPost {
			var b body
			if err := decodeJSON(r.Body, &b); err != nil {
				writeProblem(w, r, err)
				return
			}
			clock.SetOffset(time.Duration(b.OffsetSeconds * float64(time.Second)))
		}

		writeJSON(w, nethttp.StatusOK, response{OffsetSeconds: clock.Offset().Seconds()})
	}
}
