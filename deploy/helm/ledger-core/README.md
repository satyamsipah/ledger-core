# ledger-core Helm chart

Deploys the five stateless application services -- `api`,
`outbox-publisher`, `projector`, `saga-orchestrator`, `reconciler` -- on
Kubernetes, plus two pre-install/pre-upgrade hook Jobs (`migrate`,
`kafka-init`) that run before any Deployment is touched.

## What this chart does NOT deploy, and why

Postgres, Kafka, Prometheus and Grafana. A real cluster's Postgres and
Kafka are either a managed service (RDS, MSK, Aiven, Confluent Cloud, ...)
or run from a dedicated operator/chart (CloudNativePG, Strimzi, the
Bitnami charts, ...) with its own backup, failover and upgrade story --
bundling a hand-rolled StatefulSet here would be worse than either of
those, not a convenience. Prometheus/Grafana are conventionally
`kube-prometheus-stack` or similar in a real cluster, which already knows
how to discover this chart's Services via
`app.kubernetes.io/*` labels and its `/metrics` ports without this chart
needing to know anything about it.

`postgres.dsn` and `kafka.brokers` are therefore chart inputs, not chart
outputs: point them at whatever already runs those, in-cluster or not.

## Prerequisites

- A Kubernetes cluster and `kubectl` context pointed at it
- Helm 3
- Images already pushed for the tag you're about to deploy -- see
  `.github/workflows/deploy.yml`, which builds and pushes on every `v*`
  tag. This chart never builds anything itself.

## Required values

None of these have a working default; `helm install`/`upgrade` fails
closed (via each template's `required` check) rather than silently
deploying against an empty string.

| Value | What it is |
|---|---|
| `image.tag` | The release tag CI built and pushed, e.g. `v0.5.0` |
| `postgres.dsn` | A full `postgres://` connection string to a real Postgres |
| `kafka.brokers` | Comma-separated broker list, e.g. `kafka-0:9092,kafka-1:9092` |
| `gateway.url` | The real external payment gateway. **Never** a mock -- this chart builds no mock-gateway image |

Supply them with a values file kept out of version control:

```bash
cat > secrets.values.yaml <<'YAML'
postgres:
  dsn: postgres://ledger:REAL_PASSWORD@postgres.internal:5432/ledger?sslmode=require
kafka:
  brokers: kafka-0.internal:9092,kafka-1.internal:9092,kafka-2.internal:9092
gateway:
  url: https://api.real-payment-gateway.example.com
YAML
```

or from a CI secret store with `--set` / `--set-file`, never hand-typed
into a values file that gets committed by accident.

## Install / upgrade

```bash
helm upgrade --install ledger-core deploy/helm/ledger-core \
  --namespace ledger-core --create-namespace \
  --set image.tag=v0.5.0 \
  -f secrets.values.yaml
```

The `migrate` hook Job must exit 0 before Helm proceeds to the
`kafka-init` hook or any Deployment -- a schema that fails to migrate
stops the rollout before any pod runs against it, not after.

## Verifying without a cluster

```bash
helm lint deploy/helm/ledger-core --set image.tag=v0 --set postgres.dsn=x --set kafka.brokers=x --set gateway.url=x
helm template ledger-core deploy/helm/ledger-core --set image.tag=v0 --set postgres.dsn=x --set kafka.brokers=x --set gateway.url=x | kubeconform -strict -summary
```

Both run in CI-friendly seconds and need no live cluster; `kubeconform`
validates the rendered output against Kubernetes' real OpenAPI schemas.

## Design notes worth knowing before changing this chart

- **One Deployment/Service/HPA/PDB template, `range`d over
  `.Values.services`**, not five near-identical copies of each. Adding a
  sixth service is one `values.yaml` entry, not four new files that can
  drift from the other five. See `templates/_helpers.tpl` and
  `values.yaml`'s own comments for the `healthPort` distinction between
  `api` (health lives on the main HTTP port) and every worker (health
  shares the metrics port).
- **HPA-managed Deployments omit `spec.replicas` entirely.** Setting it
  from `values.yaml` would make every `helm upgrade` reset replica count
  back to the chart default, fighting whatever the autoscaler had already
  decided.
- **Readiness probes are honest about what each service can check.**
  `outbox-publisher` and `projector` hold a live Kafka client
  (`internal/kafka.Checker`) and their `/readyz` reflects that; `api`,
  `saga-orchestrator` and `reconciler` do not touch Kafka directly and
  their `/readyz` checks Postgres only. Every one of them is real --
  see `docs/ARCHITECTURE.md` for which service holds which connection.
- **`checksum/config`/`checksum/secret` pod annotations** force a rollout
  when only the ConfigMap/Secret content changes and the image tag does
  not, so a config-only `helm upgrade` doesn't report success while every
  running pod keeps its stale environment.
