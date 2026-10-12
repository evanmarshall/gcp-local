# Client setup and the client matrix

Point each client at gcp-local with [local ADC](local-identity.md) and the settings below. Which (service, client) pairs pass is in [coverage](coverage.md#client-compatibility); `sh scripts/check_clients.sh` verifies every one of them.

## Settings

Set `GOOGLE_APPLICATION_CREDENTIALS` to the local `adc.json` and `GOOGLE_CLOUD_PROJECT` to its project. Never set `*_EMULATOR_HOST`: it makes clients anonymous, and the gateway refuses anonymous calls. REST is `http://127.0.0.1:4566`; gRPC is `127.0.0.1:4567`, plaintext.

| Client | REST services | gRPC services |
| --- | --- | --- |
| Go | `option.WithEndpoint("http://127.0.0.1:4566/storage/v1/")`; BigQuery `.../bigquery/v2/` | `option.WithGRPCConn(conn)`: insecure transport plus a per-RPC credential from ADC whose `RequireTransportSecurity()` is false (`grpcOption` in `tests/clients/go/main.go`). The libraries' own credentials refuse to travel without TLS |
| Python | `client_options={"api_endpoint": "http://127.0.0.1:4566"}` | Generated clients: `transport=Client.get_transport_class("grpc")(channel=...)` with `secure_authorized_channel(credentials, Request(), "127.0.0.1:4567", ssl_credentials=grpc.local_channel_credentials())`. Firestore: set `_firestore_api_internal` ([Firestore](firestore.md)). Handwritten Spanner, Bigtable, and Logging clients take no channel: not possible ([#54](https://github.com/evanmarshall/gcp-local/issues/54)) |
| Node.js | `{apiEndpoint: "http://127.0.0.1:4566", useAuthWithCustomEndpoint: true}`; without the second option Storage and BigQuery drop credentials. Storage uploads need `{resumable: false}` ([Storage](storage.md)). `@googleapis/iam`: `rootUrl: "http://127.0.0.1:4566/"` | Not possible: grpc-js sends call credentials only over TLS ([#54](https://github.com/evanmarshall/gcp-local/issues/54)) |
| gcloud | `CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE=adc.json`, `CLOUDSDK_API_ENDPOINT_OVERRIDES_STORAGE=http://127.0.0.1:4566/storage/v1/`, and `..._PUBSUB`, `..._SECRETMANAGER`, `..._CLOUDKMS` set to `http://127.0.0.1:4566/`. Authentication fails ([#53](https://github.com/evanmarshall/gcp-local/issues/53)) | None |
| rclone | `endpoint = http://127.0.0.1:4566/storage/v1/` and `env_auth = true` | None |

- gRPC is served for Pub/Sub, Firestore, Secret Manager, Cloud Tasks v2, the Spanner and Bigtable data planes, and Logging. Default gRPC clients for Cloud KMS, Spanner and Bigtable admin, and IAM admin fail; each has an issue in the coverage table.
- The local token service issues only `cloud-platform` and Storage's scopes, and most client libraries request others by default ([#52](https://github.com/evanmarshall/gcp-local/issues/52)). Load ADC with the `cloud-platform` scope and pass it to the client, as every runner does.
- Turn off client metrics exporters, which would call `monitoring.googleapis.com`: Go Spanner `DisableNativeMetrics`, Go Bigtable `NoopMetricsProvider`, Python Spanner `disable_builtin_metrics`, Node Spanner `disableBuiltInMetrics`, Node Bigtable `metricsEnabled: false`.

## The matrix

[`tests/clients/matrix.json`](../tests/clients/matrix.json) declares each (service, client) pair as `pass` or `xfail` with its issue and the error text it must fail with. Each runner does one workflow per service: create, write, read, list, delete, and one call by an ungranted identity that must be denied.

| Runner | Pins |
| --- | --- |
| [`tests/clients/go`](../tests/clients/go) | `go.mod` |
| [`tests/clients/node`](../tests/clients/node) | `package-lock.json` |
| [`tests/clients/python`](../tests/clients/python) | `requirements.txt` |
| [`tests/clients/cli`](../tests/clients/cli) (gcloud, rclone) | `toolchains/clients.json` |

Run in Linux or moo, after the backends in [CONTRIBUTING.md](../CONTRIBUTING.md#checks):

```sh
python3 scripts/install_client_tools.py   # Node.js, gcloud, rclone
PATH="$PWD/.tools/clients/node/bin:$PATH" npm ci --prefix tests/clients/node
.venv-tools/bin/python -m pip install -r tests/clients/python/requirements.txt
sh scripts/check_clients.sh               # or --client go --service storage
```

The script starts `bin/gcp-local` with local ADC for a granted and an ungranted service account, inside `scripts/run_isolated.py`, so no client can reach the network. It writes `.artifacts/client-matrix/results.json` (service, client, version, transport, outcome, issue, reason) and one log per pair. It fails when a `pass` pair fails, an `xfail` pair passes, or an `xfail` pair fails with a different error.

When a pair starts passing, set it to `pass` in `matrix.json` and move its line in `docs/coverage.md`; `check_repo.py` fails until the two agree.
