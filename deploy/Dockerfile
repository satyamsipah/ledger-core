# Builds any one of the service binaries. Which one is chosen by the SERVICE
# build arg, so cmd/api, cmd/projector and cmd/reconciler share a single
# Dockerfile and cannot drift apart in base image or build flags.
ARG SERVICE=api

FROM golang:1.25-alpine AS build
ARG SERVICE

WORKDIR /src

# Dependencies first, in their own layer: source edits then rebuild in seconds
# rather than re-downloading the module cache every time.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# CGO off for a static binary that runs on a bare alpine image.
# -trimpath keeps build-host paths out of panic traces.
RUN CGO_ENABLED=0 GOOS=linux go build \
        -trimpath \
        -ldflags='-s -w' \
        -o /out/service \
        ./cmd/${SERVICE}

FROM alpine:3.20
ARG SERVICE

# ca-certificates for outbound TLS; wget comes from busybox and backs the
# compose healthcheck.
RUN apk add --no-cache ca-certificates tzdata

# Unprivileged: nothing in these services needs root, and a ledger is a poor
# place to find that out later.
RUN adduser -D -u 10001 ledger
USER ledger

COPY --from=build /out/service /usr/local/bin/service

ENV LEDGER_SERVICE=${SERVICE}
EXPOSE 8080 9090

ENTRYPOINT ["/usr/local/bin/service"]
