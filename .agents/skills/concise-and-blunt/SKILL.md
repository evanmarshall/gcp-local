---
name: concise-and-blunt
description: Use when writing or editing any documentation, README, guide, skill, issue, or PR description in gcp-local.
---

# Concise and blunt

Write for a reader who wants the answer and will stop reading once they have it. The length should match what the reader needs to act, and nothing more.

## Shape of every section

1. **First sentence: the answer.** Say what this is, what works, or what to do. If it doesn't work, say so in the first sentence: "Redis is not reachable from the host."
2. **Then the facts the reader acts on**, in order: a command, a table, or numbered steps. One working example beats three explanations.
3. **Then limits**, as a short bullet list of concrete facts ("8 MiB upload limit", "no queries or transactions").
4. **Stop.** End on the last fact.

## Wording

- Use numbers and names: "15 of 88 methods", "`roles/redis.admin`", "recorded 2026-10-11". Avoid adjectives like "robust", "comprehensive" and "seamless".
- Give one reason only when the reader needs it to decide something. Implementation history and design rationale go in `docs/design.md` or commit messages.
- Say each fact once, in the doc that owns it, and link to it from everywhere else. Service support status lives in `docs/coverage.md`.
- Write to the doc's reader. User docs (README, quickstart, service docs) address application developers. Instructions for agents and contributors go in `AGENTS.md` and `.agents/skills/`.
- Use plain verbs and short sentences. Prefer "is", "runs" and "returns" to "leverages", "facilitates" and "enables".
- State what is unsupported bluntly ("not implemented", "not recorded"). Don't hedge or apologize.

## Check before you finish

Reread the draft and cut every sentence that repeats an earlier one, explains something the reader didn't ask about, or addresses someone other than the reader. If a section is longer than the procedure it describes, cut it again.
