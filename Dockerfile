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

FROM scratch
COPY --from=build /gcp-local /gcp-local
USER 65532:65532
ENTRYPOINT ["/gcp-local"]
