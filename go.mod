module github.com/evanmarshall/gcp-local

go 1.26.0

toolchain go1.26.8

require (
	cloud.google.com/go/auth v0.23.0
	cloud.google.com/go/bigtable v1.58.0
	cloud.google.com/go/cloudtasks v1.20.0
	cloud.google.com/go/firestore v1.22.0
	cloud.google.com/go/iam v1.11.0
	cloud.google.com/go/kms v1.35.0
	cloud.google.com/go/logging v1.18.0
	cloud.google.com/go/longrunning v1.2.0
	cloud.google.com/go/pubsub/v2 v2.7.0
	cloud.google.com/go/resourcemanager v1.17.0
	cloud.google.com/go/run v1.23.0
	cloud.google.com/go/scheduler v1.17.0
	cloud.google.com/go/secretmanager v1.22.0
	cloud.google.com/go/spanner v1.95.1
	cloud.google.com/go/storage v1.65.0
	github.com/bufbuild/protocompile v0.14.1
	github.com/fsouza/fake-gcs-server v1.56.1
	github.com/goccy/go-googlesql v0.3.0
	github.com/goccy/googlesqlite v0.3.1
	github.com/google/cel-go v0.26.1
	github.com/robfig/cron/v3 v3.0.1
	google.golang.org/api v0.293.0
	google.golang.org/genproto v0.0.0-20260519071638-aa98bba5eb94
	google.golang.org/genproto/googleapis/api v0.0.0-20260706201446-f0a921348800
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260807164820-c8921c73eeea
	google.golang.org/grpc v1.84.0
	google.golang.org/protobuf v1.36.12
)

require (
	cel.dev/expr v0.25.2 // indirect
	cloud.google.com/go v0.123.0 // indirect
	cloud.google.com/go/auth/oauth2adapt v0.2.8 // indirect
	cloud.google.com/go/compute/metadata v0.9.0 // indirect
	cloud.google.com/go/monitoring v1.29.0 // indirect
	github.com/DataDog/go-hll v1.0.2 // indirect
	github.com/GoogleCloudPlatform/grpc-gcp-go/grpcgcp v1.6.0 // indirect
	github.com/GoogleCloudPlatform/opentelemetry-operations-go/detectors/gcp v1.34.0 // indirect
	github.com/GoogleCloudPlatform/opentelemetry-operations-go/exporter/metric v0.57.0 // indirect
	github.com/GoogleCloudPlatform/opentelemetry-operations-go/internal/resourcemapping v0.57.0 // indirect
	github.com/antlr4-go/antlr/v4 v4.13.0 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/cncf/xds/go v0.0.0-20260202195803-dba9d589def2 // indirect
	github.com/dgryski/go-farm v0.0.0-20240924180020-3414d57e47da // indirect
	github.com/dlclark/regexp2 v1.11.4 // indirect
	github.com/dop251/goja v0.0.0-20260311135729-065cd970411c // indirect
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/envoyproxy/go-control-plane/envoy v1.37.0 // indirect
	github.com/envoyproxy/protoc-gen-validate v1.3.3 // indirect
	github.com/felixge/httpsnoop v1.1.0 // indirect
	github.com/go-jose/go-jose/v4 v4.1.4 // indirect
	github.com/go-logr/logr v1.4.3 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/go-sourcemap/sourcemap v2.1.3+incompatible // indirect
	github.com/goccy/go-json v0.10.6 // indirect
	github.com/goccy/googlesqlwasm2go v0.1.0 // indirect
	github.com/golang/geo v0.0.0-20260505155700-1c5af9662e82 // indirect
	github.com/google/btree v1.1.3 // indirect
	github.com/google/pprof v0.0.0-20230207041349-798e818bf904 // indirect
	github.com/google/renameio/v2 v2.0.0 // indirect
	github.com/google/s2a-go v0.1.9 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/googleapis/enterprise-certificate-proxy v0.3.20 // indirect
	github.com/googleapis/gax-go/v2 v2.23.0 // indirect
	github.com/gorilla/handlers v1.5.2 // indirect
	github.com/gorilla/mux v1.8.1 // indirect
	github.com/klauspost/compress v1.19.2 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/ncruces/go-sqlite3 v0.34.0 // indirect
	github.com/ncruces/go-sqlite3-wasm/v2 v2.1.35300 // indirect
	github.com/ncruces/go-strftime v1.0.0 // indirect
	github.com/ncruces/julianday v1.0.0 // indirect
	github.com/pkg/errors v0.8.0 // indirect
	github.com/pkg/xattr v0.4.12 // indirect
	github.com/planetscale/vtprotobuf v0.6.1-0.20240319094008-0393e58bdf10 // indirect
	github.com/remyoudompheng/bigfft v0.0.0-20230129092748-24d4a6f8daec // indirect
	github.com/spaolacci/murmur3 v1.1.0 // indirect
	github.com/spiffe/go-spiffe/v2 v2.8.1 // indirect
	github.com/stoewer/go-strcase v1.2.0 // indirect
	go.opencensus.io v0.24.0 // indirect
	go.opentelemetry.io/auto/sdk v1.2.1 // indirect
	go.opentelemetry.io/contrib/detectors/gcp v1.44.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc v0.68.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp v0.69.0 // indirect
	go.opentelemetry.io/otel v1.44.0 // indirect
	go.opentelemetry.io/otel/metric v1.44.0 // indirect
	go.opentelemetry.io/otel/sdk v1.44.0 // indirect
	go.opentelemetry.io/otel/sdk/metric v1.44.0 // indirect
	go.opentelemetry.io/otel/trace v1.44.0 // indirect
	golang.org/x/crypto v0.55.0 // indirect
	golang.org/x/exp v0.0.0-20230515195305-f3d0a9c9a5cc // indirect
	golang.org/x/net v0.58.0 // indirect
	golang.org/x/oauth2 v0.36.0 // indirect
	golang.org/x/sync v0.22.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
	golang.org/x/text v0.41.0 // indirect
	golang.org/x/time v0.15.0 // indirect
	gonum.org/v1/gonum v0.17.0 // indirect
	modernc.org/libc v1.72.3 // indirect
	modernc.org/libquickjs v0.12.8 // indirect
	modernc.org/mathutil v1.7.1 // indirect
	modernc.org/memory v1.11.0 // indirect
	modernc.org/quickjs v0.18.2 // indirect
	rsc.io/binaryregexp v0.2.0 // indirect
)
