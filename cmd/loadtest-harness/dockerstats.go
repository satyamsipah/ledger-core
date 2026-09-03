// CPU and memory come from `docker stats`, sampled on an interval for the
// duration of a scenario, rather than from Prometheus/cAdvisor: this stack
// runs no container-metrics exporter (deploy/prometheus/prometheus.yml
// scrapes only this codebase's own /metrics endpoints), and adding one just
// to answer "how much CPU did the api container use" would be a permanent
// stack addition to answer a question this harness only needs to ask while
// it is running. `docker stats` gives the same numbers directly, at the cost
// of sampling rather than continuous collection.
package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"
)

// resolveContainerNames maps compose service name to the actual container
// name docker stats needs, by asking compose itself -- never assumed, because
// compose's auto-generated name depends on the directory the repo happens to
// be checked out into (the same reasoning docs/DECISIONS.md D51 gives for why
// postgres and redpanda needed a pinned container_name; every other service
// here still has compose's default naming, so this harness has to ask rather
// than guess for those).
func resolveContainerNames(ctx context.Context, composeFile string, wantServices []string) (map[string]string, error) {
	//nolint:gosec // G204: composeFile is this process's own -compose-file
	// CLI flag (default deploy/docker-compose.yml), never external input --
	// this binary is a developer-run benchmarking tool, not a network-facing
	// service.
	cmd := exec.CommandContext(ctx, "docker", "compose", "-f", composeFile, "ps", "--format", "json")
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("docker compose ps: %w", err)
	}

	want := make(map[string]bool, len(wantServices))
	for _, s := range wantServices {
		want[s] = true
	}

	result := make(map[string]string, len(wantServices))
	scanner := bufio.NewScanner(bytes.NewReader(out))
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var row struct {
			Service string `json:"Service"`
			Name    string `json:"Name"`
		}
		if err := json.Unmarshal([]byte(line), &row); err != nil {
			return nil, fmt.Errorf("parse docker compose ps line %q: %w", line, err)
		}
		if want[row.Service] {
			result[row.Service] = row.Name
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan docker compose ps output: %w", err)
	}

	for _, s := range wantServices {
		if _, ok := result[s]; !ok {
			return nil, fmt.Errorf("service %q not found in docker compose ps output -- is the stack up?", s)
		}
	}
	return result, nil
}

type resourceSample struct {
	CPUPercent float64
	MemMB      float64
}

type resourceSummary struct {
	CPUAvgPercent float64
	CPUMaxPercent float64
	MemAvgMB      float64
	MemMaxMB      float64
}

// statsSampler polls `docker stats --no-stream` on an interval for a fixed
// set of containers, for as long as a scenario runs.
type statsSampler struct {
	cancel context.CancelFunc
	done   chan struct{}

	mu            sync.Mutex
	samples       map[string][]resourceSample // by compose service name
	nameToService map[string]string
}

func startStatsSampler(ctx context.Context, serviceToContainer map[string]string, interval time.Duration) *statsSampler {
	sctx, cancel := context.WithCancel(ctx)

	nameToService := make(map[string]string, len(serviceToContainer))
	for service, name := range serviceToContainer {
		nameToService[name] = service
	}

	s := &statsSampler{
		cancel:        cancel,
		done:          make(chan struct{}),
		samples:       make(map[string][]resourceSample, len(serviceToContainer)),
		nameToService: nameToService,
	}

	containerNames := make([]string, 0, len(serviceToContainer))
	for _, name := range serviceToContainer {
		containerNames = append(containerNames, name)
	}

	go s.run(sctx, containerNames, interval)
	return s
}

func (s *statsSampler) run(ctx context.Context, containerNames []string, interval time.Duration) {
	defer close(s.done)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	s.sampleOnce(ctx, containerNames) // one immediate sample, so a very short scenario still has data
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.sampleOnce(ctx, containerNames)
		}
	}
}

// sampleOnce is best-effort: a single failed `docker stats` invocation (a
// transient Docker Engine hiccup) drops one sample rather than aborting the
// whole scenario run over a metric that is secondary to the scenario's own
// pass/fail result.
func (s *statsSampler) sampleOnce(ctx context.Context, containerNames []string) {
	args := append([]string{"stats", "--no-stream", "--format", "{{json .}}"}, containerNames...)
	//nolint:gosec // G204: containerNames come from resolveContainerNames,
	// which reads them back from `docker compose ps` itself -- Docker-
	// generated, not external input; see the identical reasoning already
	// established for test/kafka_testdb.go's own container-name arguments.
	cmd := exec.CommandContext(ctx, "docker", args...)
	out, err := cmd.Output()
	if err != nil {
		return
	}

	scanner := bufio.NewScanner(bytes.NewReader(out))
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var row struct {
			Name     string `json:"Name"`
			CPUPerc  string `json:"CPUPerc"`
			MemUsage string `json:"MemUsage"`
		}
		if err := json.Unmarshal([]byte(line), &row); err != nil {
			continue
		}

		service, ok := s.nameToService[row.Name]
		if !ok {
			continue
		}

		sample := resourceSample{
			CPUPercent: parsePercent(row.CPUPerc),
			MemMB:      parseMemUsageMB(row.MemUsage),
		}

		s.mu.Lock()
		s.samples[service] = append(s.samples[service], sample)
		s.mu.Unlock()
	}
}

// stop halts sampling and returns each service's summary. Safe to call once.
func (s *statsSampler) stop() map[string]resourceSummary {
	s.cancel()
	<-s.done

	s.mu.Lock()
	defer s.mu.Unlock()

	result := make(map[string]resourceSummary, len(s.samples))
	for service, samples := range s.samples {
		if len(samples) == 0 {
			continue
		}
		var sum resourceSummary
		var cpuTotal, memTotal float64
		for _, sample := range samples {
			cpuTotal += sample.CPUPercent
			memTotal += sample.MemMB
			if sample.CPUPercent > sum.CPUMaxPercent {
				sum.CPUMaxPercent = sample.CPUPercent
			}
			if sample.MemMB > sum.MemMaxMB {
				sum.MemMaxMB = sample.MemMB
			}
		}
		sum.CPUAvgPercent = cpuTotal / float64(len(samples))
		sum.MemAvgMB = memTotal / float64(len(samples))
		result[service] = sum
	}
	return result
}

func parsePercent(s string) float64 {
	v, _ := strconv.ParseFloat(strings.TrimSuffix(strings.TrimSpace(s), "%"), 64)
	return v
}

// parseMemUsageMB parses docker stats' "26.69MiB / 3.827GiB" shape and
// returns the USED side, in MB. Only the units docker stats actually emits
// are handled; an unrecognised unit returns 0 rather than guessing.
func parseMemUsageMB(s string) float64 {
	parts := strings.SplitN(s, "/", 2)
	if len(parts) == 0 {
		return 0
	}
	used := strings.TrimSpace(parts[0])

	units := []struct {
		suffix    string
		mbPerUnit float64
	}{
		{"GiB", 1024},
		{"MiB", 1},
		{"KiB", 1.0 / 1024},
		{"kB", 1.0 / 1024},
		{"MB", 1},
		{"GB", 1024},
		{"B", 1.0 / (1024 * 1024)},
	}
	for _, u := range units {
		if strings.HasSuffix(used, u.suffix) {
			num, err := strconv.ParseFloat(strings.TrimSuffix(used, u.suffix), 64)
			if err != nil {
				return 0
			}
			return num * u.mbPerUnit
		}
	}
	return 0
}
