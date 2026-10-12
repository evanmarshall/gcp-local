# Permission drift: secretmanager.versions.access

Status: **no-drift** (source: gcp; probe executed 2026-10-12T00:27:17.429243+00:00). Nothing was applied; review before changing providers, plans, or snapshots.

| | Permissions |
| --- | --- |
| Discovered minimal set | secretmanager.versions.access |
| Local plan | secretmanager.versions.access |
| Missing from local | - |
| Extra in local | - |

## Proposed actions

- Not probed at this attachment point: resourcemanager.projects.get, resourcemanager.projects.list, secretmanager.locations.get, secretmanager.locations.list, secretmanager.secrets.list; local checks of these remain unverified by this report.

Scenario-scoped: one acting identity, one request shape, one initial state, and one grant location. A minimal set found by greedy removal is one sufficient set, not proof of every authorization path.
