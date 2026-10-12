# The static core is cross-compiled on the build platform, so a multi-architecture
# release does not compile Go under emulation.
FROM --platform=$BUILDPLATFORM python:3.12.15-slim-bookworm@sha256:54c85f3c47607a77f32adec749d3c81d1348bf25833671f512b26a9b6d778cb3 AS build
WORKDIR /src
COPY scripts/install_go.py scripts/install_go.py
COPY toolchains/go.json toolchains/go.json
RUN python3 scripts/install_go.py
ENV PATH="/src/.tools/go/bin:${PATH}" GOTOOLCHAIN=local CGO_ENABLED=0
COPY go.mod go.sum ./
RUN go mod download
COPY api api
COPY cmd cmd
COPY internal internal
ARG GCP_LOCAL_VERSION=dev
ARG TARGETOS TARGETARCH
RUN GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH} go build -trimpath -ldflags="-s -w -X main.version=${GCP_LOCAL_VERSION}" -o /gcp-local ./cmd/gcp-local

# Downloads the checksum-pinned official Pub/Sub emulator JAR for the runtime stage.
FROM python:3.12.15-slim-bookworm@sha256:54c85f3c47607a77f32adec749d3c81d1348bf25833671f512b26a9b6d778cb3 AS pubsub
WORKDIR /src
COPY scripts/install_emulator_jar.py scripts/install_emulator_jar.py
COPY toolchains/pubsub.json toolchains/pubsub.json
RUN python3 scripts/install_emulator_jar.py pubsub

# Downloads the checksum-pinned official Firestore emulator JAR for the runtime stage.
FROM python:3.12.15-slim-bookworm@sha256:54c85f3c47607a77f32adec749d3c81d1348bf25833671f512b26a9b6d778cb3 AS firestore
WORKDIR /src
COPY scripts/install_emulator_jar.py scripts/install_emulator_jar.py
COPY toolchains/firestore.json toolchains/firestore.json
RUN python3 scripts/install_emulator_jar.py firestore

# Downloads the checksum-pinned official Spanner emulator binary and the newer
# libstdc++ it requires from the per-architecture gcr.io image layers.
FROM python:3.12.15-slim-bookworm@sha256:54c85f3c47607a77f32adec749d3c81d1348bf25833671f512b26a9b6d778cb3 AS spanner
WORKDIR /src
COPY scripts/install_spanner.py scripts/install_spanner.py
COPY toolchains/spanner.json toolchains/spanner.json
RUN python3 scripts/install_spanner.py

# Builds the official Redis 7.2.16 source (checksum-pinned by install_redis.py)
# on the runtime base so the binary links against the same glibc. The compiler
# packages are build-only and absent from the final image.
FROM eclipse-temurin:17-jre-jammy@sha256:8993f1aed8b25fcea7a7047a7949c1866fa558fc6830d938c22c4f13b26be9d7 AS redis
RUN apt-get update && apt-get install -y --no-install-recommends gcc make libc6-dev python3 && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY scripts/install_redis.py scripts/install_redis.py
COPY toolchains/redis.json toolchains/redis.json
RUN python3 scripts/install_redis.py

# Downloads the checksum-pinned PostgreSQL builds Cloud SQL runs per instance.
FROM python:3.12.15-slim-bookworm@sha256:54c85f3c47607a77f32adec749d3c81d1348bf25833671f512b26a9b6d778cb3 AS postgres
WORKDIR /src
COPY scripts/install_postgres.py scripts/install_postgres.py
COPY toolchains/postgres.json toolchains/postgres.json
RUN python3 scripts/install_postgres.py

# The launcher (gcp_local/container.py) and the bundled workflow worker need
# Python. The interpreter and standard library come from the same digest-pinned
# image as the build stage and link against the runtime base's glibc and
# OpenSSL; pip and unused modules are dropped, and the library is precompiled
# because the runtime root filesystem is read-only.
FROM python:3.12.15-slim-bookworm@sha256:54c85f3c47607a77f32adec749d3c81d1348bf25833671f512b26a9b6d778cb3 AS python
RUN set -eu; cd /usr/local/lib/python3.12; \
    rm -rf site-packages/pip* ensurepip idlelib tkinter turtledemo lib2to3 pydoc_data; \
    python3 -m compileall -q --invalidation-mode unchecked-hash /usr/local/lib/python3.12

# The static runtime binary needs no libc, but the Pub/Sub and Firestore backends
# are JVMs their providers start lazily, so the image carries the pinned JRE and JARs. Java's
# temporary directory lives on the Compose tmpfs because the root filesystem is read-only.
FROM eclipse-temurin:17-jre-jammy@sha256:8993f1aed8b25fcea7a7047a7949c1866fa558fc6830d938c22c4f13b26be9d7
RUN set -eu; mkdir -p /opt/pubsub /opt/firestore; ln -sf "$(readlink -f "$(command -v java)")" /usr/bin/java
COPY --from=build /gcp-local /gcp-local
COPY --from=pubsub /src/.tools/pubsub/emulator.jar /opt/pubsub/emulator.jar
COPY --from=firestore /src/.tools/firestore/emulator.jar /opt/firestore/emulator.jar
# The Spanner emulator (selected with -services=...,spanner) uses the base image's
# glibc 2.35, libm, and libgcc_s with its own pinned libstdc++ under /opt/spanner/lib.
COPY --from=spanner /src/.tools/spanner /opt/spanner
# Memorystore starts one redis-server per instance on loopback inside the core.
COPY --from=redis /src/.tools/redis/bin/redis-server /opt/redis/redis-server
# Cloud SQL runs one PostgreSQL server per instance, on a private Unix socket.
COPY --from=postgres /src/.tools/postgres /opt/postgres
COPY --from=python /usr/local/lib/ /usr/local/lib/
COPY --from=python /usr/local/bin/python3.12 /usr/local/bin/python3
COPY gcp_local /usr/local/lib/python3.12/site-packages/gcp_local
COPY examples/workflow/worker.py /opt/gcp-local/workflow/worker.py
RUN set -eu; ldconfig; \
    python3 -I -m compileall -q --invalidation-mode unchecked-hash /usr/local/lib/python3.12/site-packages/gcp_local; \
    python3 -I -c "import gcp_local.container, http.server, json, ssl, urllib.request"
# The container runs as the host user's UID, which has no passwd entry, and
# PostgreSQL's initdb refuses such a user. /etc/passwd points at the tmpfs;
# the launcher writes it from the image's copy plus the running UID.
RUN set -eu; cp /etc/passwd /etc/passwd.image; ln -sf /tmp/passwd /etc/passwd
USER 65532:65532
ENV GCP_LOCAL_PORT=4566
EXPOSE 4566 4567
HEALTHCHECK --interval=2s --timeout=4s --retries=30 CMD ["/usr/local/bin/python3", "-I", "-m", "gcp_local.container", "health"]
# Default: a standalone environment in the /state bind mount (docs/quickstart.md).
# The Compose environment runs "serve" with read-only configuration mounts.
ENTRYPOINT ["/usr/local/bin/python3", "-I", "-m", "gcp_local.container"]
CMD ["standalone"]
