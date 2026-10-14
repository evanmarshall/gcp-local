# Transports

Which transports Google serves for each service, which ones gcp-local serves, and the gaps. A gap is a method gcp-local serves on one transport while Google serves it on both. Methods gcp-local doesn't serve at all are coverage gaps, listed in [coverage](../coverage.md), not here.

The table is generated from the running gateway: `TestTransportMatrix` (`cmd/gcp-local/transports_test.go`) registers every provider, reads the methods each one serves (`gateway.Served`), and pairs REST routes with gRPC methods through the transcoding tables and the `google.api.http` bindings of Google's protos pinned in `go.mod`. `sh scripts/check/recordings.sh` fails when this file differs. Regenerate on Linux with every backend:

```sh
. scripts/run/backend_env.sh
sh scripts/go.sh test -count=1 -run '^TestTransportMatrix$' ./cmd/gcp-local -update-transports
```

What Google serves comes from two sources:

- **Services gcp-local serves over gRPC**, from their protos pinned in `go.mod`: REST when the method has a `google.api.http` binding, gRPC always. Client-streaming and bidirectional methods (Listen, Write, StreamingPull) and the Bigtable data API are gRPC only.
- **Services gcp-local serves over REST only**: `gcpTransports` in the test, from the transports in each API's `BUILD.bazel` in [googleapis](https://github.com/googleapis/googleapis). Compute Engine is `rest` only; BigQuery v2's protos are published for reference only; `google.storage.v2` has no notification, HMAC key, service account, ACL, or copy methods; the Storage XML API has no gRPC form.

<!-- matrix:begin -->
| Service | Google serves | REST routes | gRPC methods | Gap: served on REST only | Gap: served on gRPC only |
| --- | --- | --- | --- | --- | --- |
| `artifactregistry` | REST, gRPC | 8 | 0 | all 8 | none |
| `bigquery` | REST (v2 protos are reference only) | 21 | 0 | none | none |
| `bigtable` | REST and gRPC admin; gRPC data | 24 | 28 | none | none |
| `cloudkms` | REST, gRPC | 26 | 23 | cloudkms.locations.get | none |
| `cloudscheduler` | REST, gRPC | 8 | 8 | none | none |
| `cloudsql` | REST; gRPC through the Java client | 27 | 0 | all 27 | none |
| `cloudtasks` | REST, gRPC | 16 | 15 | cloudtasks.locations.list | none |
| `compute` | Compute REST; Network Management REST, gRPC | 46 | 0 | networkmanagement.connectivityTests.create, networkmanagement.connectivityTests.delete, networkmanagement.connectivityTests.get, networkmanagement.connectivityTests.list, networkmanagement.connectivityTests.rerun | none |
| `firestore` | REST, gRPC; Listen and Write gRPC | 18 | 14 | firestore.fields.get, firestore.fields.update, firestore.operations.cancel, firestore.operations.delete, firestore.operations.get, firestore.operations.list | none |
| `iam` | REST, gRPC | 29 | 20 | IAM.CreateServiceAccountKey, IAM.SignBlob, IAM.SignJwt | none |
| `logging` | REST, gRPC | 4 | 4 | none | none |
| `orgpolicy` | REST, gRPC | 21 | 7 | none | none |
| `pubsub` | REST, gRPC; StreamingPull gRPC | 28 | 30 | none | Subscriber.CreateSnapshot, Subscriber.DeleteSnapshot, Subscriber.GetSnapshot, Subscriber.ListSnapshots, Subscriber.Seek |
| `redis` | REST, gRPC (client default) | 10 | 0 | all 10 | none |
| `resourcemanager` | v3 REST, gRPC; v1 REST | 29 | 24 | none | none |
| `run` | REST, gRPC | 14 | 14 | none | none |
| `secretmanager` | REST, gRPC | 15 | 14 | secretmanager.locations.list | none |
| `spanner` | REST, gRPC | 44 | 35 | DatabaseAdmin.CopyBackup, DatabaseAdmin.CreateBackup, DatabaseAdmin.DeleteBackup, DatabaseAdmin.GetBackup, DatabaseAdmin.RestoreDatabase, DatabaseAdmin.UpdateBackup, spanner.databases.operations.cancel, spanner.databases.operations.list, spanner.instances.operations.cancel, spanner.instances.operations.list | Spanner.ExecuteStreamingSql, Spanner.StreamingRead |
| `storage` | JSON and XML REST; gRPC v2 | 27 | 0 | storage.buckets.delete, storage.buckets.get, storage.buckets.getIamPolicy, storage.buckets.getStorageLayout, storage.buckets.insert, storage.buckets.list, storage.buckets.lockRetentionPolicy, storage.buckets.patch, storage.buckets.setIamPolicy, storage.folders.delete, storage.folders.get, storage.folders.insert, storage.folders.list, storage.objects.delete, storage.objects.get, storage.objects.insert, storage.objects.list, storage.objects.patch, storage.objects.restore, storage.objects.rewrite | none |
| `vertexai` | REST, gRPC | 5 | 0 | all 5 | none |

Served on one transport because Google serves only that one (not gaps):

- `bigquery`: bigquery.datasets.create (REST), bigquery.datasets.delete (REST), bigquery.datasets.get (REST), bigquery.datasets.getIamPolicy (REST), bigquery.datasets.list (REST), bigquery.datasets.setIamPolicy (REST), bigquery.datasets.update (REST), bigquery.jobs.get (REST), bigquery.jobs.getQueryResults (REST), bigquery.jobs.insert (REST), bigquery.jobs.list (REST), bigquery.jobs.query (REST), bigquery.tabledata.insertAll (REST), bigquery.tabledata.list (REST), bigquery.tables.create (REST), bigquery.tables.delete (REST), bigquery.tables.get (REST), bigquery.tables.getIamPolicy (REST), bigquery.tables.list (REST), bigquery.tables.setIamPolicy (REST), bigquery.tables.update (REST)
- `bigtable`: Bigtable.CheckAndMutateRow (gRPC), Bigtable.MutateRow (gRPC), Bigtable.MutateRows (gRPC), Bigtable.PingAndWarm (gRPC), Bigtable.ReadModifyWriteRow (gRPC), Bigtable.ReadRows (gRPC), Bigtable.SampleRowKeys (gRPC)
- `compute`: compute.firewalls.create (REST), compute.firewalls.delete (REST), compute.firewalls.get (REST), compute.firewalls.list (REST), compute.firewalls.update (REST), compute.globalOperations.delete (REST), compute.globalOperations.get (REST), compute.globalOperations.list (REST), compute.globalOperations.wait (REST), compute.instances.delete (REST), compute.instances.get (REST), compute.instances.insert (REST), compute.instances.list (REST), compute.networks.addPeering (REST), compute.networks.create (REST), compute.networks.delete (REST), compute.networks.get (REST), compute.networks.list (REST), compute.networks.removePeering (REST), compute.regionOperations.delete (REST), compute.regionOperations.get (REST), compute.regionOperations.list (REST), compute.regionOperations.wait (REST), compute.routers.create (REST), compute.routers.delete (REST), compute.routers.get (REST), compute.routers.list (REST), compute.routes.create (REST), compute.routes.delete (REST), compute.routes.get (REST), compute.routes.list (REST), compute.subnetworks.create (REST), compute.subnetworks.delete (REST), compute.subnetworks.get (REST), compute.subnetworks.getIamPolicy (REST), compute.subnetworks.list (REST), compute.subnetworks.setIamPolicy (REST), compute.zoneOperations.delete (REST), compute.zoneOperations.get (REST), compute.zoneOperations.list (REST), compute.zoneOperations.wait (REST)
- `firestore`: Firestore.Listen (gRPC)
- `pubsub`: Subscriber.StreamingPull (gRPC)
- `resourcemanager`: resourcemanager.projects.get (REST), resourcemanager.projects.getIamPolicy (REST), resourcemanager.projects.setIamPolicy (REST), resourcemanager.projects.testIamPermissions (REST)
- `storage`: storage.notifications.delete (REST), storage.notifications.get (REST), storage.notifications.insert (REST), storage.notifications.list (REST), storage.projects.serviceAccount.get (REST), storage.xml.objects.get (REST), storage.xml.objects.put (REST)
<!-- matrix:end -->

## Verifying a transport

A gap closes when the method runs on the missing transport, a dual-transport scenario records both against GCP, and a client-matrix pair drives the non-default transport. `secretmanager-transport-parity` is the first such recording ([validation](../contributing/validation.md#dual-transport-scenarios)). The Pub/Sub recordings cover gRPC alone; every other gRPC claim in [coverage](../coverage.md) is checked locally only.
