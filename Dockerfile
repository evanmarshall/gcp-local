FROM python:3.12.15-slim-bookworm@sha256:54c85f3c47607a77f32adec749d3c81d1348bf25833671f512b26a9b6d778cb3 AS build
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
RUN go build -trimpath -ldflags="-s -w -X main.version=${GCP_LOCAL_VERSION}" -o /gcp-local ./cmd/gcp-local

# Downloads the checksum-pinned official Pub/Sub emulator JAR for the runtime stage.
FROM python:3.12.15-slim-bookworm@sha256:54c85f3c47607a77f32adec749d3c81d1348bf25833671f512b26a9b6d778cb3 AS pubsub
WORKDIR /src
COPY scripts/install_pubsub.py scripts/install_pubsub.py
COPY toolchains/pubsub.json toolchains/pubsub.json
RUN python3 scripts/install_pubsub.py

# Downloads the checksum-pinned official Firestore emulator JAR for the runtime stage.
FROM python:3.12.15-slim-bookworm@sha256:54c85f3c47607a77f32adec749d3c81d1348bf25833671f512b26a9b6d778cb3 AS firestore
WORKDIR /src
COPY scripts/install_firestore.py scripts/install_firestore.py
COPY toolchains/firestore.json toolchains/firestore.json
RUN python3 scripts/install_firestore.py

# The static runtime binary needs no libc, but the Pub/Sub and Firestore backends
# are JVMs their providers start lazily, so the image carries the pinned JRE and JARs. Java's
# temporary directory lives on the Compose tmpfs because the root filesystem is read-only.
FROM eclipse-temurin:17-jre-jammy@sha256:8993f1aed8b25fcea7a7047a7949c1866fa558fc6830d938c22c4f13b26be9d7
RUN set -eu; mkdir -p /opt/pubsub /opt/firestore; ln -sf "$(readlink -f "$(command -v java)")" /usr/bin/java
COPY --from=build /gcp-local /gcp-local
COPY --from=pubsub /src/.tools/pubsub/emulator.jar /opt/pubsub/emulator.jar
COPY --from=firestore /src/.tools/firestore/emulator.jar /opt/firestore/emulator.jar
USER 65532:65532
ENTRYPOINT ["/gcp-local"]
