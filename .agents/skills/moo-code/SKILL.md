---
name: moo-code
description: "Develop and verify gcp-local changes in an isolated moo machine and Git worktree, including runtime snapshots and fast-forward integration."
---

# Coding with moo

Read [the repository moo guide](../../../docs/moo.md). The host checkout owns edits and Git history; the guest owns packages, processes, and disposable test state. Use one worktree and one named machine for each concurrent task. A machine alone does not isolate source edits.

Check `moo doctor`. If setup is missing and installation is in scope, follow [moo-golden-image](../moo-golden-image/SKILL.md). Ordinary Linux CI continues to run without moo.

From the task's worktree, fork the repository's provisioned baseline and run the applicable guest tests:

```sh
moo new task-name from base
moo run task-name -- 'cd /srv/app && python3 -m unittest discover -s tests/tooling -v'
```

Use a distinct machine name for each task. `moo new` and `moo run` sync tracked and untracked, nonignored files from the calling worktree. Ignored guest files persist. Edit source on the host: a later sync can replace guest edits. Run repository metadata/history checks on the host because moo does not copy `.git`.

Save valuable state before switching commits and after committing: `moo save task-name`. Restoring with `moo new task-name` may replace unsaved runtime state. After a rebase or reset, explicitly restore/resync the machine and rerun checks. A machine snapshot is local development state, not a GCP conformance recording.

Keep the shared baseline free of GCP credentials and user data. Live GCP work still needs the sandbox scope described by `AGENTS.md`; machine isolation does not isolate cloud resources or deny internet access.

Integrate authorized work with `git merge --ff-only` and a normal push, following [CONTRIBUTING.md](../../../CONTRIBUTING.md). Never use an ordinary merge that can create a merge commit. Drop only this task's machine and worktree when finished; snapshots survive `moo drop` unless `--snapshots` is explicitly supplied.

Adapted from [moo v0.3.1](https://github.com/heyito/moo/blob/6ca60ffd23564c740c39d8f21d612bbe6cb63ae9/skills/moo-code/SKILL.md); [MIT notice](../../../third_party/moo/LICENSE).
