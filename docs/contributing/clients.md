# Client setup and the client matrix

Point each client at gcp-local with [local ADC](../guides/local-identity.md) and the settings below. Which (service, client) pairs pass is in [coverage](../coverage.md#client-compatibility); `sh scripts/check/clients.sh` verifies every one of them.

## Settings

Set `GOOGLE_APPLICATION_CREDENTIALS` to the local `adc.json` and `GOOGLE_CLOUD_PROJECT` to its project. Never set `*_EMULATOR_HOST`: it makes clients anonymous, and the gateway refuses anonymous calls. REST is `http://127.0.0.1:4566`; gRPC is `127.0.0.1:4567` (plaintext) and `127.0.0.1:4584` ([TLS](#grpc-over-tls)).

| Client | REST services | gRPC services |
| --- | --- | --- |
| Go | `option.WithEndpoint("http://127.0.0.1:4566/storage/v1/")`; BigQuery `.../bigquery/v2/` | `option.WithGRPCConn(conn)`: insecure transport plus a per-RPC credential from ADC whose `RequireTransportSecurity()` is false (`grpcOption` in `tests/clients/go/main.go`). The libraries' own credentials refuse to travel without TLS |
| Python | `client_options={"api_endpoint": "http://127.0.0.1:4566"}` | Generated clients: `transport=Client.get_transport_class("grpc")(channel=...)` with `secure_authorized_channel(credentials, Request(), "127.0.0.1:4567", ssl_credentials=grpc.local_channel_credentials())`. Firestore: set `_firestore_api_internal` ([Firestore](../services/databases/firestore.md)). Handwritten Spanner, Bigtable, and Logging clients: `client_options={"api_endpoint": "127.0.0.1:4584"}` with `GRPC_DEFAULT_SSL_ROOTS_FILE_PATH` set ([TLS](#grpc-over-tls)) |
| Node.js | `{apiEndpoint: "http://127.0.0.1:4566", useAuthWithCustomEndpoint: true}`; without the second option Storage and BigQuery drop credentials. Storage uploads need `{resumable: false}` ([Storage](../services/storage.md)). `@googleapis/iam`: `rootUrl: "http://127.0.0.1:4566/"` | `{apiEndpoint: "127.0.0.1", port: 4584, sslCreds}` ([TLS](#grpc-over-tls)). Pub/Sub: `{apiEndpoint: "127.0.0.1:4584", emulatorMode: false, sslCreds}` |
| gcloud | `CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE=adc.json`, `CLOUDSDK_API_ENDPOINT_OVERRIDES_STORAGE=http://127.0.0.1:4566/storage/v1/`, and `..._PUBSUB`, `..._SECRETMANAGER`, `..._CLOUDKMS` set to `http://127.0.0.1:4566/`. The ADC file's `token_info_url` must point at the local `/v1/introspect`; gcp-local's ADC files do | None |
| rclone | `endpoint = http://127.0.0.1:4566/storage/v1/`, `env_auth = true`, and `bucket_policy_only = true` (buckets need uniform access) | None |

- gcloud drives Pub/Sub over REST: topic and subscription create, list, describe, delete, IAM policy, `topics publish`, `subscriptions pull` (with `--auto-ack`), and `subscriptions modify-message-ack-deadline` work. Snapshots and seek are gRPC only ([Pub/Sub](../services/messaging/pubsub.md#methods)).
- gRPC is served for Pub/Sub, Firestore, Secret Manager, Cloud Tasks v2, Cloud Scheduler v1, Cloud KMS, Cloud Run v2, Spanner and Bigtable (data and admin), IAM service accounts (`google.iam.admin.v1`), and Logging.
- Keep the client's default credential loading and scopes: the token service issues Google's published service scopes, and IAM refuses a call no scope covers ([OAuth scopes](../guides/local-identity.md#oauth-scopes)). The REST runners and the handwritten Python gRPC clients load ADC this way. Prebuilt gRPC channel credentials bypass the client's credential loading, so the Go and Node.js runners request `cloud-platform` for them and the Python generated clients the transport's default scopes.
- Turn off client metrics exporters, which would call `monitoring.googleapis.com`: Go Spanner `DisableNativeMetrics`, Go Bigtable `NoopMetricsProvider`, Python Spanner `disable_builtin_metrics`, Node Spanner `disableBuiltInMetrics`, Node Bigtable `metricsEnabled: false`.

## gRPC over TLS

grpc-js and the handwritten Python Spanner, Bigtable, and Logging clients send credentials only over TLS. They use the TLS port, which serves the same gRPC services with the same authentication and IAM as 4567, and trust the environment's own CA.

| Setting | Packaged environment (`gcp-local env`) | `bin/gcp-local` |
| --- | --- | --- |
| Endpoint | `GCP_LOCAL_GRPC_TLS_ENDPOINT` (`127.0.0.1:4584`) | `-grpc-tls-listen 127.0.0.1:4584` |
| CA certificate | `GCP_LOCAL_GRPC_TLS_CA` (`<directory>/tls/ca.pem`) | `ca.pem` in `-grpc-tls-dir DIR` ([runtime](../architecture/runtime.md#flags)) |

Python handwritten clients take the CA from gRPC's default-roots variable:

```sh
export GRPC_DEFAULT_SSL_ROOTS_FILE_PATH="$GCP_LOCAL_GRPC_TLS_CA"
```

```python
spanner.Client(project=project, credentials=credentials,
               client_options={"api_endpoint": "127.0.0.1:4584"})
```

Node.js clients take channel credentials built from the CA and ADC. `google-auth-library` returns a `Headers` object that grpc-js does not read, so convert it:

```js
import {readFileSync} from 'node:fs';
import {GoogleAuth} from 'google-auth-library';
import {grpc} from 'google-gax';

const auth = await new GoogleAuth({scopes: ['https://www.googleapis.com/auth/cloud-platform']}).getClient();
const sslCreds = grpc.credentials.combineChannelCredentials(
  grpc.credentials.createSsl(readFileSync(process.env.GCP_LOCAL_GRPC_TLS_CA)),
  grpc.credentials.createFromGoogleCredential({
    getRequestHeaders: async url => Object.fromEntries(await auth.getRequestHeaders(url)),
  }),
);
const client = new SecretManagerServiceClient({apiEndpoint: '127.0.0.1', port: 4584, sslCreds});
```

Go clients keep the plaintext setup above. `cmd/gcp-local/tls_test.go` dials the TLS port with `credentials.NewTLS` and the CA in `RootCAs`; no Go client library is tested against it.

- Each environment creates its own CA on first start. Its private key is discarded after it signs the server certificate, and it is name-constrained to `localhost` and `gcp-local`. The server key stays in the environment directory (mode 0600).
- The certificate names `localhost`, `gcp-local`, `127.0.0.1`, and `::1`, and expires after 825 days; delete the `tls` directory and restart to replace it.
- Don't disable certificate verification; trust `ca.pem` instead.

## The matrix

[`tests/clients/matrix.json`](../../tests/clients/matrix.json) declares each (service, client) pair as `pass` or `xfail` with its issue and the error text it must fail with. Each runner does one workflow per service: create, write, read, list, delete, and one call by an ungranted identity that must be denied.

| Runner | Pins |
| --- | --- |
| [`tests/clients/go`](../../tests/clients/go) | `go.mod` |
| [`tests/clients/node`](../../tests/clients/node) | `package-lock.json` |
| [`tests/clients/python`](../../tests/clients/python) | `requirements.txt` |
| [`tests/clients/cli`](../../tests/clients/cli) (gcloud, rclone) | `toolchains/clients.json` |

Run in Linux or moo, after the backends in [CONTRIBUTING.md](../../.github/CONTRIBUTING.md#checks):

```sh
python3 scripts/install/client_tools.py   # Node.js, gcloud, rclone
PATH="$PWD/.tools/clients/node/bin:$PATH" npm ci --prefix tests/clients/node
.venv-tools/bin/python -m pip install -r tests/clients/python/requirements.txt
sh scripts/check/clients.sh               # or --client go --service storage
```

The script starts `bin/gcp-local` with local ADC for a granted and an ungranted service account, inside `scripts/run/isolated.py`, so no client can reach the network. It writes `.artifacts/client-matrix/results.json` (service, client, version, transport, outcome, issue, reason) and one log per pair. It fails when a `pass` pair fails, an `xfail` pair passes, or an `xfail` pair fails with a different error.

When a pair starts passing, set it to `pass` in `matrix.json` and move its line in `docs/coverage.md`; `scripts/check/repo.py` fails until the two agree.
