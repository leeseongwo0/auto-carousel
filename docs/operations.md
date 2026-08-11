# Newsbot operations

## Default: manual, local workflow

Newsbot v1 is designed to be run deliberately on a local machine. A local SQLite
database is the authority for collected items, approvals, generated drafts, and
delivery handoffs. The normal workflow is:

1. Create or select a local database.
2. Run a collection command when collection is needed.
3. Inspect candidates and make a human approval decision.
4. Generate a draft for an approved candidate and review that exact draft.
5. Perform any delivery only after the second human approval.

Use local paths and synthetic configuration while evaluating the project. Keep
credentials and destination-specific settings in a private production profile;
do not put them in commands, examples, logs, issues, or commits. Inspect status
and individual records before retrying an operation. A timeout, interrupted
process, malformed remote response, or unknown remote result is ambiguous and
must be investigated rather than blindly resent.

A minimal local setup uses the synthetic behavior profile and a private state
directory:

```bash
uv sync --group dev
export NEWSBOT_STATE="$HOME/.local/state/newsbot"
export NEWSBOT_PROFILE="$NEWSBOT_STATE/profile.toml"
install -d -m 700 "$NEWSBOT_STATE"
cp config/manual-profile.example.toml "$NEWSBOT_PROFILE"
uv run newsbot manual-init --profile "$NEWSBOT_PROFILE" \
  --state "$NEWSBOT_STATE" --database newsbot.sqlite3
uv run newsbot manual-status --profile "$NEWSBOT_PROFILE" \
  --state "$NEWSBOT_STATE" --database newsbot.sqlite3
```

The manual/local workflow does not require a VPS, Telegram, Google Sheets,
Codex, a scheduler, or Asia/Seoul time.
Every existing ancestor of a state or output path must be owned by the current user or `root`, must not be a symbolic link, and must not be writable by group or other users. Shared and sticky paths such as `/tmp` are outside the supported manual/local trust boundary.

## Newsbot V2 operational validation

V2 uses an isolated database and never opens, migrates, deletes, or writes the
legacy Newsbot database. The legacy database is a read-only archive. This
document describes copy-only validation; it does not authorize a production
migration, service change, timer enablement, or cutover.

The fixed workflow remains: durable Telegram revision intent → bounded article
enrichment → current-generation policy/claim → candidate approval → draft
generation → second exact-draft approval → Sheets delivery. An unclear Telegram
or Sheets effect, a conflicting claim, and every untyped failure become
`manual_review`; no sender retries an ambiguous effect. The Telegram worker is
the sole poll owner and sends at most one draft-or-candidate approval per tick.

Collection persists immutable Telegram revisions before enrichment. Unchanged
observations advance last-seen state without another fetch or policy evaluation.
Edits received after a candidate claim are retained for audit but do not replace,
re-evaluate, or resend the approved evidence. Article enrichment uses direct,
proxy-free DNS/socket/TLS requests with bounded redirects, response sizes, and
deadlines; unsafe, private, or ambiguous network targets never fall back to an
unrestricted client.
Set `NEWSBOT_V2_BLOCKED_ARTICLE_HOSTS` to a comma-separated private deployment
denylist (for example, internal service and control-plane hostnames). The direct
transport applies the denylist before DNS and again on every redirect; entries
also block their subdomains.

Candidate identity is story-centric. Only the selected requested URL, validated
final URL, accepted same-registrable-domain canonical URL, and an exact
meaningful-body digest can become aliases. A delivered, quarantined, held, or
effectful story remains suppressed after hot payload compaction.
Collection applies one global due-work budget across all configured handles.
When the enrichment backlog reaches `--limit`, it drains existing work without
accepting more Telegram observations. New-message pages share that budget with
a persisted newest-to-oldest edit scan. The edit scan rotates through the last
24 hours across runs, resumes from its durable message-ID cursor after a crash,
and promotes its edit watermark only after the full history sweep completes.

Use an explicit disposable V2 path for every rehearsal:

```bash
newsbot-v2 --db ./fresh-v2.sqlite create-db
newsbot-v2 --db ./fresh-v2.sqlite runtime-db
newsbot-v2 --db ./fresh-v2.sqlite verify-db
newsbot-v2 --db ./fresh-v2.sqlite v2-status --limit 50
newsbot-v2 --db ./fresh-v2.sqlite compact --batch-size 500 --dry-run
newsbot-v2 --db ./fresh-v2.sqlite compact --batch-size 500
```

For a schema migration rehearsal, stop every V2 writer and compaction owner,
then run the guarded command on an exact disposable copy:

```bash
newsbot-v2 --db ./v2-copy.sqlite migrate-db \
  --backup ./v2-copy.before-schema-7.sqlite \
  --timeout-seconds 120
newsbot-v2 --db ./v2-copy.sqlite verify-db
```

`create-db` refuses an existing database. `runtime-db` and `migrate-db` both
require an existing file; neither creates a missing database. `runtime-db` only
accepts a complete V2 schema. `migrate-db` is the one atomic migration boundary.
Its preflight requires a new backup path, valid SQLite integrity and foreign
keys, and a non-busy WAL checkpoint. When source and backup share a filesystem,
that filesystem must have free space of at least four times the
database-plus-WAL size. On separate filesystems, the source must have three
times that size for migration working headroom and the backup filesystem must
have one complete snapshot's capacity. The exclusive migration transaction
holds every clean notification-eligible predecessor candidate before its sole
commit, enforces the deadline, and rolls back on failure; preserve the
hash-reported backup.
`verify-db` opens read-only and must leave the file unchanged. Compaction has
no network work, is bounded to 500 rows, and should be dry-run, applied,
reapplied, and then verified on a copy. Stop the rehearsal on any invariant
mismatch. The current migration target is schema `7`, following deployed
predecessor schemas `3`–`6`; it is intentionally monotonic even though the
original planning draft used an abstract schema-v2 label. Before its sole
commit, migration checks the exact table/column/index contract, foreign keys,
SQLite integrity, duplicate truth lattice, and workflow invariants. The
version marker is its final write.

Retention is persistent rather than a daily reset. The default hot window is
30 days for delivered and current non-candidate payloads and 7 days for
unbound superseded revisions. Compaction keeps fixed identity, story keys and
claims, delivery/effect receipts, ambiguity, active/manual-review evidence,
and digest-only provenance. Expired terminal callbacks are removed; old
delivered draft and successful Codex payload bytes become digest markers
without weakening replay suppression. One apply transaction shares the
requested batch budget across every compacted table and aborts on an invariant
mismatch. `v2-status` returns a dynamic keyset page (default 50, maximum 200);
use its opaque `next_cursor` rather than requesting an unbounded dump.
Each status response also includes capped aggregate state/queue counts, the
completed 15-minute fetch window, database and WAL sizes, oldest queue ages,
`aggregate_cap`, `aggregate_truncated`, and active threshold alerts. Set
`NEWSBOT_V2_SEVEN_DAY_STORAGE_BASELINE_BYTES` to the reviewed seven-day
storage model in production; the command fails closed when it is absent.
Blocked/transient fetch rates above 20%, combined storage above twice that
baseline, and queue/manual-review age above 24 hours emit redacted structured
alerts.
Immediate safety alerts are emitted as redacted critical events for
`private_harness_hit`, `duplicate_claim`, `confirmed_effect_reattempt`, and
`migration_retention_mismatch`; `identity_conflict` is also a paging event.
Treat any of these as a stop signal: preserve the V2 database, disable the
affected owner, and reconcile durable story/effect evidence before resuming.

No-send validation uses fake Telegram, generation, and Sheets ports. Supply a
reviewed fixture and run three copy-only cycles:

```bash
newsbot-v2 --db ./v2-copy.sqlite validate-selection \
  --no-send --fixture ./reviewed-v2-fixture.json
```

The first cycle may create review rows, while the next two must create zero
revisions, claims, callbacks, effects, and fake sends. The validator uses the
SQLite backup API so committed WAL state is included. Do not point this command
at production or the legacy database.

A post-migration backlog is held by default. Produce and review its manifest
before an explicit release:

```bash
newsbot-v2 --db ./v2-copy.sqlite hold-backlog > held-manifest.json
newsbot-v2 --db ./v2-copy.sqlite release-backlog --manifest held-manifest.json
```

The release command requires the manifest's canonical evidence items, digest,
and sorted IDs. The digest binds each candidate to its immutable revision,
article snapshot, story, and story keys; any evidence change invalidates the
release. It also rejects delivered, quarantined, manual-review, callback-bearing,
or effectful rows. Re-run read-only verification and compare the reviewed
manifest digest before any separately approved production action.
A production cutover additionally requires the recorded release
approval, owner quiescence, a snapshot, cursor handoff, reconciliation, and
one Telegram poll owner; none of those actions is authorized here.

Before any new external effect, rollback is restoration of the verified V1
snapshot and prior runtime. After any external effect, never restore or replay
automatically: stop owners, preserve both databases, and reconcile Telegram and
Sheets truth manually. Do not edit either database to force a resend.

## Legacy VPS automation compatibility

Newsbot retains an existing automated deployment model for installations that
already use it. That model has separate scheduled collection, Telegram, Sheets,
and optional Codex workers. It is legacy compatibility rather than the public
default. Run it only with a private production profile, separately provisioned
credentials, and deployment controls appropriate to the installation.

The legacy workers share the same database and coordinate through durable state
and worker locks. They are not presented as independent security sandboxes.
Codex remains separate: its provider credentials and containment controls must
remain isolated from the non-Codex workers. Do not merge its schedule or
credential boundary into the other workers.

Legacy Telegram approval and Sheets delivery retain their safety rules:

- Collection advances its cursor only after durable work completes.
- A candidate approval is distinct from approval to deliver the exact current
  draft.
- Remote effects are recorded before or alongside an attempt where supported.
- Accepted delivery chunks are not blindly resent.
- Ambiguous or partial Telegram and Sheets effects require inspection and an
  explicit immutable resolution.
- Existing delivery rows and durable history are not edited to force a retry.

The legacy noon route uses Asia/Seoul only for installations that enable that
route. Its admission window is noon through, but not including, 13:00. A missing
intent at the end of that window is skipped; it is not caught up or rolled over.
An intent committed on time may still complete through its durable outbox.

## Legacy release, recovery, and rollback

For an existing automation installation, drain scheduled workers before changing
its runtime. Verify that workers are inactive, authority locks are held, and any
Codex containment state is clean before building or switching a release. Keep a
backup according to the installation's own retention policy.
The compatibility drain and lock order is always `collect → Telegram → Sheets`;
do not reorder or parallelize these authority boundaries.
A generic compatibility release uses one owner-only quiescence proof across both
the build and switch attestations:

```bash
sudo sh -c 'umask 077; printf "quiescent\n" > /var/lib/newsbot/quiescence-proof'
sudo /opt/newsbot/releases/<COMMIT_SHA>/src/deploy/build_newsbot_release.py \
  --uv-sha256 <PINNED_UV_SHA256> build <COMMIT_SHA> \
  --quiescence-proof /var/lib/newsbot/quiescence-proof
sudo /opt/newsbot/releases/<COMMIT_SHA>/src/deploy/build_newsbot_release.py \
  --uv-sha256 <PINNED_UV_SHA256> switch <COMMIT_SHA> \
  --quiescence-proof /var/lib/newsbot/quiescence-proof
sudo rm -f /var/lib/newsbot/quiescence-proof
sudo -u newsbot /usr/local/bin/newsbot init-db --db <DATABASE_PATH>
sudo -u newsbot /usr/local/bin/newsbot automation-cutover-preview \
  --config <PRIVATE_CONFIG_PATH> --db <DATABASE_PATH> \
  --proposal-id <PROPOSAL_ID> --release-digest <RELEASE_DIGEST>
sudo -u newsbot /usr/local/bin/newsbot automation-cutover-apply \
  --db <DATABASE_PATH> --proposal-id <PROPOSAL_ID> \
  --proposal-sha256 <PROPOSAL_SHA256> --release-digest <RELEASE_DIGEST>
```

`<COMMIT_SHA>` and `<PINNED_UV_SHA256>` are explicit, reviewed placeholders.
Never substitute credentials, copy an unverified digest from logs, or recreate
the proof between build and switch.

Apply database initialization and migration commands only at the documented
point in that installation's release procedure. Treat release/config activation
as append-only. Do not retrofit older activations, edit durable audit records,
or restore a database snapshot as a shortcut around a failed remote effect.

Rollback is a forward switch to a previously verified compatible runtime or a
corrective runtime. Preserve migrations, approvals, cursors, outboxes, attempts,
remote history, and containment receipts. Resolve production-specific incidents
through that installation's private runbook; do not copy host names, account
identities, credentials, or payloads into this public documentation.
