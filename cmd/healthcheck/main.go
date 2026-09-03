// Command healthcheck is Docker's HEALTHCHECK for a distroless container.
//
// The production image (deploy/Dockerfile.prod) has no shell and no wget --
// deploy/Dockerfile's local-dev image uses those for its HEALTHCHECK, and
// distroless deliberately ships neither, which is the whole point of
// distroless. HEALTHCHECK's exec form runs a binary directly with no shell
// involved, so this is that binary: a static Go program, a few hundred
// kilobytes, that makes one HTTP GET and turns the status code into an exit
// code. Nothing here talks to Postgres or Kafka directly -- it hits the
// service's own /healthz or /readyz, which already does, so this stays a
// generic tool one image copies for every service rather than one more
// thing that could drift from a service's actual dependency list.
package main

import (
	"context"
	"flag"
	"fmt"
	nethttp "net/http"
	"os"
	"time"
)

func main() {
	os.Exit(run())
}

func run() int {
	// The default -url value is a flag.String default, which -- unlike an
	// ENV instruction's right-hand side -- Docker's ARG substitution cannot
	// reach: HEALTHCHECK is not one of the instructions Docker documents
	// build-arg substitution as applying to, so baking a per-service port
	// into the CMD array at build time silently does not work (confirmed by
	// `docker inspect` showing the literal, unexpanded "${HEALTHCHECK_PORT}"
	// the first time this was tried). HEALTHCHECK_URL is read from the
	// environment instead, because ENV *is* on that supported list --
	// deploy/Dockerfile.prod sets it from the HEALTHCHECK_PORT build arg,
	// and the container runtime hands it to this process without any shell
	// needing to expand it, which matters because the distroless image this
	// runs in has none.
	url := flag.String("url", "", "URL to GET (defaults to $HEALTHCHECK_URL, then http://127.0.0.1:9090/healthz)")
	timeout := flag.Duration("timeout", 3*time.Second, "request timeout")
	flag.Parse()

	target := *url
	if target == "" {
		target = os.Getenv("HEALTHCHECK_URL")
	}
	if target == "" {
		target = "http://127.0.0.1:9090/healthz"
	}

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	// SSRF is not the risk model here: target is operator-supplied
	// configuration (a -url flag or $HEALTHCHECK_URL this same image's
	// Dockerfile sets), the same trust boundary internal/reconciliation's
	// CSV path already sits inside, not attacker-controlled request input.
	// This process has no HTTP surface of its own for an attacker to reach
	// in the first place.
	//nolint:gosec // G704: see the trust-boundary comment above.
	req, err := nethttp.NewRequestWithContext(ctx, nethttp.MethodGet, target, nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "healthcheck: %v\n", err)
		return 1
	}

	client := nethttp.Client{Timeout: *timeout}

	//nolint:gosec // G704: see the trust-boundary comment above.
	resp, err := client.Do(req)
	if err != nil {
		fmt.Fprintf(os.Stderr, "healthcheck: %v\n", err)
		return 1
	}
	defer func() { _ = resp.Body.Close() }()

	// 2xx only. A 3xx here would mean this URL started redirecting, which is
	// not this process being healthy -- it is this process being
	// misconfigured, and treating a redirect as success would hide that.
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		fmt.Fprintf(os.Stderr, "healthcheck: %s returned %d\n", target, resp.StatusCode)
		return 1
	}

	return 0
}
