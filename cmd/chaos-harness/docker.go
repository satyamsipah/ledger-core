package main

import (
	"context"
	"fmt"
	"net"
	nethttp "net/http"
	"time"
)

// dockerClient talks to the Docker Engine API over its own Unix socket.
//
// No SDK dependency: pause and unpause are each one HTTP POST with an empty
// body, and pulling in a full Docker client library for two verbs would be a
// strange trade for a tool that exists to be small and obviously correct.
// The same reasoning this codebase already applies elsewhere -- a hand-rolled
// retrier instead of a library, a hand-rolled clientIP parser instead of
// middleware -- applies here too.
type dockerClient struct {
	http *nethttp.Client
}

func newDockerClient() *dockerClient {
	return &dockerClient{
		http: &nethttp.Client{
			// A real, if generous, bound. pause/unpause over a local Unix
			// socket is normally near-instant; this exists so a Docker
			// daemon that has itself wedged turns into a clear timeout error
			// this handler can report, rather than a request this process
			// hangs on for as long as its caller is willing to wait.
			Timeout: 10 * time.Second,
			Transport: &nethttp.Transport{
				DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
					// DialContext, not the bare net.Dial: the whole point of
					// giving this client a Timeout is that a wedged Docker
					// daemon turns into a clean error rather than a hang, and
					// that guarantee has to reach the dial itself, not just
					// the request round trip.
					var d net.Dialer
					return d.DialContext(ctx, "unix", "/var/run/docker.sock")
				},
			},
		},
	}
}

// pause and unpause use the cgroup freezer, exactly like `docker pause` on
// the CLI -- the same mechanism docs/DECISIONS.md D36 already established
// for TestOutboxPublish_KafkaOutage: it freezes the ALREADY-RUNNING process
// rather than restarting it, so every connection to it goes genuinely
// unresponsive with no startup sequence to race.
func (d *dockerClient) pause(ctx context.Context, container string) error {
	return d.post(ctx, "/containers/"+container+"/pause")
}

func (d *dockerClient) unpause(ctx context.Context, container string) error {
	return d.post(ctx, "/containers/"+container+"/unpause")
}

func (d *dockerClient) post(ctx context.Context, path string) error {
	req, err := nethttp.NewRequestWithContext(ctx, nethttp.MethodPost, "http://docker"+path, nil)
	if err != nil {
		return fmt.Errorf("build docker request %s: %w", path, err)
	}

	resp, err := d.http.Do(req)
	if err != nil {
		return fmt.Errorf("call docker %s: %w", path, err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode >= 300 {
		return fmt.Errorf("docker %s: unexpected status %d", path, resp.StatusCode)
	}
	return nil
}
