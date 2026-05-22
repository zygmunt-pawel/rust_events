# Review Scope — Accepted Design Decisions & Non-Issues

This file lists deliberate design decisions and operating assumptions for
`rust_events`. They are **intentional and settled**.

**Reviewers — human or agent — must NOT flag the items below as bugs.**
Flag only deviations *from* these assumptions, or genuinely new issues. If a
finding's entire premise is one of the scenarios listed here, drop it.

---

## 1. Single migration file, edited in place

The crate ships exactly one migration file
(`migrations/20260513000001_v01_outbox_init.sql`). Schema changes are made by
**editing that file in place** — not by adding new, append-only migration files.

Rationale: every environment (CI, local dev, staging) starts from a freshly
created database. There are no long-lived databases carrying an older schema
revision, so `sqlx` `_sqlx_migrations` checksum mismatches cannot occur in
practice. A developer with a stale dev database simply drops and recreates it.

**Do not flag:** in-place migration edits, `_sqlx_migrations` checksum-mismatch
or `VersionMismatch` risk, "migrations must be append-only", "freeze the
migration at publish".

## 2. No old jobs — no cross-version queue state

Every deployment starts fresh. The `outbox.*` tables and the `pg_work_queue`
queue never carry jobs enqueued by a previous binary version. There is no
migration of in-flight work between versions.

**Do not flag:** scenarios premised on "jobs enqueued by an old binary",
"mixed old/new rows in the queue", schema or queue-keying skew across versions,
rollover/rollback handling for in-flight jobs. Concretely, this means a feature
may rely on *all* queued jobs having been produced by the current binary — e.g.
conditional stamping of `pg_work_queue` keys is fine, because no unkeyed
legacy job will ever be present.

## 3. Single-instance — no clustering, no multi-process guard

Exactly one worker process runs per database (see CLAUDE.md "Single-instance by
design"). Keeping it to one process is an **operator responsibility**; the crate
deliberately does not detect or guard against an operator accidentally running
two worker processes.

**Do not flag:** multi-replica / clustered-deployment scenarios, "needs a
cross-process advisory lock", `concurrency_limit` doubling under multiple
processes, handler-lookup behaviour under overlapping deploys. Crash-recovery
machinery (fencing tokens, the `pg_work_queue` reaper, lease timeouts) is **not**
clustering — a single process still crashes and restarts — and stays in scope.

---

## What IS still in scope

These assumptions narrow the review surface; they do not silence it. Still
fair game, and still worth flagging:

- Bugs reachable by a single, current-version worker process.
- Concurrency/race issues *within* one process (tasks, the shared pool, the
  fenced UPDATEs).
- Crash and restart of the single process (fencing, reaper, lease recovery).
- Edge cases of inputs, payloads, handler failure modes, timeouts, panics.
- Test-coverage gaps for current-version behaviour.
- Performance and resource use on the hot paths.
