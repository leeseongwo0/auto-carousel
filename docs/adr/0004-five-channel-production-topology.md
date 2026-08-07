# ADR 0004: Five-channel production topology

## Status

Accepted.

## Decision

Newsbot production requires exactly five enabled, case-insensitively unique channels. The existing singleton cutover and its six immutable frontier rows remain historical authority. Current runtime authority is the latest append-only release/config binding, admitted only when the five configured channel digests are a subset of the active five-or-six frontier set.

Activation performs this topology check transactionally before replay or insertion. Collection repeats it before acquiring a lease. Production Codex checks the current binding after mandatory containment admission but before selecting or binding any job or provider work. A dedicated status command exposes only aggregate counts and booleans.

After activation, no new collection, evaluation, candidate, noon, or independent notification authority may originate from the removed source. Traceable descendants of work authorized before activation may complete.

## Drivers

- Preserve immutable cutover/frontier and operating-database history.
- Prevent a direct config deletion from disabling collection or partially activating invalid authority.
- Keep hourly collection, Telegram, Codex containment, and Sheets behavior intact.
- Support forward rollback to the verified six-channel runtime without rewriting history.

## Alternatives considered

### Add membership tables in a new migration

Rejected. It adds authority and trigger surface and makes exact rollback to the migration-008 runtime harder.

### Retire all pre-activation work from the removed source

Rejected for this change. Source-bound revocation would require separate audited authority. The owner selected grandfathering of already-authorized traceable work.

### Validate config and database state in a privileged containment helper

Rejected. It duplicates parsing and widens the root trust boundary. The existing root containment lifecycle remains unchanged.

## Consequences

- The active frontier count may be five for a fresh cutover or six for the preserved production cutover.
- Unknown configured members fail closed and produce no activation or binding write.
- A rejected Codex tick may perform the normal containment start/cleanup cycle, but creates no Newsbot job, lease, attempt, provider binding, or provider call.
- Exact config preimages and matching release manifests must be archived owner-only before replacement so rollback remains reproducible.
- Staged and canonical config files are regular single-link `root:newsbot` files with mode `0640` inside a trusted `0750` directory and are replaced atomically on the same filesystem.

## Rollback

Drain through supported state machines and archive the five-channel preimage. With all workers disabled and quiescence attested, switch and re-attest the verified prior six-capable runtime first; then atomically install and attest the exact archived six-channel config, use that runtime to append/replay its release/config binding, verify redacted status, and only then restore workers.
