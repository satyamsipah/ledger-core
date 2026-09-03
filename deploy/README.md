# Deployment

Three ways to run this stack, each for a different situation. None of them
has been used against a real, live environment yet -- no cloud accounts
exist for this project as of this phase. Everything here is prepared and
verified statically (`docker build`, `helm lint`, `helm template` +
`kubeconform`, `actionlint`), not deployed.

| | Files | For |
|---|---|---|
| Local development | `docker-compose.yml`, `Dockerfile` | A laptop. Builds images locally, alpine-based, wget healthchecks, every service including LOCAL-ONLY `mock-gateway` |
| Single-VPS production | `docker-compose.prod.yml`, `Dockerfile.prod`, `env.prod.example` | One machine. Pulls pre-built images, distroless, no mock-gateway, every credential a required env var with no fallback |
| Kubernetes | `helm/ledger-core/`, `k8s/ledger-core.yaml` | A cluster. Same distroless images as the VPS path; `helm/` is the source of truth, `k8s/ledger-core.yaml` is its generated, kubectl-apply-able output -- see `helm/ledger-core/README.md` |

## Images

`Dockerfile.prod` builds every service from one shared, multi-stage
Dockerfile (`SERVICE` build arg selects which `cmd/` package), same as
`Dockerfile` does for local dev, so all eight service images cannot drift
apart in base image or build flags. The production image differs from the
local one in one deliberate way: `gcr.io/distroless/static-debian12:nonroot`
instead of `alpine`, which has no shell, no package manager and no `wget`
-- so `HEALTHCHECK` uses a tiny statically-built binary
(`cmd/healthcheck`) instead. Kubernetes' own liveness/readiness probes
need none of this: `kubelet` makes the HTTP call from outside the
container, not a shell inside it.

```bash
docker build -f deploy/Dockerfile.prod --build-arg SERVICE=api --build-arg HEALTHCHECK_PORT=8080 -t ledger-core-api:test .
```

`HEALTHCHECK_PORT` matters for exactly one service: `api`'s `/healthz`
lives on its main HTTP port (8080), every worker's shares its metrics
port instead (9091-9095). Every other build-arg default is fine
untouched.

## Migrations

Both production paths run `cmd/migrate` -- not the standalone
`migrate/migrate` CLI image local dev uses with a bind-mounted
`../migrations` -- because it links `migrations.FS`
(`migrations/embed.go`) directly, needing nothing beyond the image itself.
On the VPS it is a `restart: on-failure` one-shot compose service; on
Kubernetes it is a Helm pre-install/pre-upgrade hook Job that must exit 0
before Helm touches a single Deployment. Either way, migrating stays a
separate deployment step from any service starting up -- see
`migrations/embed.go`'s own doc comment for why services never migrate
themselves.

## What's still ahead

Actually deploying to a real target -- provisioning a VPS or cluster,
setting `KUBE_CONFIG`/`PROD_POSTGRES_DSN`/`PROD_KAFKA_BROKERS`/
`PROD_GATEWAY_URL` as repository secrets, cutting the first real release
tag -- is later work, once accounts for a target platform exist. Until
then, `.github/workflows/deploy.yml` builds and pushes images on every
`v*` tag regardless (so a release is always reproducible from that point
forward) and skips the Kubernetes rollout step with a clear warning if
those secrets aren't set, rather than either failing the whole workflow
or silently doing nothing.
