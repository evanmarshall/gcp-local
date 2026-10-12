---
name: moo-code
description: "Develop and verify gcp-local changes in an isolated moo machine and Git worktree, including runtime snapshots and fast-forward integration."
---

# Coding with moo

Read [the moo guide](../../../docs/moo.md). The host worktree owns edits and Git; the guest owns packages, processes, and disposable test state. Use one worktree and one named machine per concurrent task.

If `moo doctor` fails and setup is in scope, follow [moo-golden-image](../moo-golden-image/SKILL.md).

```sh
moo new task-name from base
moo run task-name -- 'cd /srv/app && python3 -m unittest discover -s tests/tooling -v'
```

- `moo new` and `moo run` sync tracked and nonignored untracked files from the calling worktree. Edit on the host; a later sync overwrites guest edits.
- Run `check_repo.py` and `check_history.py` on the host; moo doesn't copy `.git`.
- `moo save task-name` after committing and before switching commits. After a rebase or reset, resync and rerun checks. A snapshot is local state, not a GCP recording.
- Keep the baseline free of credentials and user data. Machine isolation doesn't isolate cloud resources or block the internet; live GCP work still needs the scope in `AGENTS.md`.
- Integrate with `git merge --ff-only` and a normal push ([CONTRIBUTING.md](../../../CONTRIBUTING.md#fast-forward-only-integration)).
- When done, drop only this task's machine and worktree. Snapshots survive `moo drop` unless `--snapshots` is passed.

Adapted from [moo v0.3.1](https://github.com/heyito/moo/blob/6ca60ffd23564c740c39d8f21d612bbe6cb63ae9/skills/moo-code/SKILL.md); [MIT notice](../../../third_party/moo/LICENSE).
