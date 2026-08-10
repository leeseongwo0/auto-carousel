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

## Newsbot V2 cutover

V2 is an independent workflow and database. It does not migrate, delete, or write
legacy Newsbot records. Provision a separate database such as
`/var/lib/newsbot-v2/newsbot-v2.sqlite`, and run the V2 entrypoint with that path.

The V2 order is fixed: Telegram collection → exclusion-first policy → first
candidate approval → draft generation → second exact-draft approval → Sheets
delivery. The only automatic retries are bounded retries for clear collection
network failures. An unclear Telegram or Sheets result is `manual_review`; do not
blindly resend it.

The tracked channel profile keeps six enabled sources and replaces the old
research counterpart with `the_block_crypto`. The private VPS profile must make
the same one-for-one replacement before a cutover. Keep the legacy timers and
legacy database read-only while validating V2.

Before switching production traffic, run exclusion fixtures, a complete
collection-to-Sheets approval test, and three consecutive clean runs. A V2
cutover is a forward switch; rollback means switching back to the previously
verified runtime without editing either database.

The operational surface always requires the explicit V2 database path:

```bash
newsbot-v2 --db /var/lib/newsbot-v2/newsbot-v2.sqlite status
newsbot-v2 --db /var/lib/newsbot-v2/newsbot-v2.sqlite collect-fixture --fixture <fixture.json>
newsbot-v2 --db /var/lib/newsbot-v2/newsbot-v2.sqlite collect-live --lookback-hours 24 --limit 20
newsbot-v2 --db /var/lib/newsbot-v2/newsbot-v2.sqlite generate-codex <candidate-id>
newsbot-v2 --db /var/lib/newsbot-v2/newsbot-v2.sqlite deliver-google-sheets <draft-id> --deadline 120
```

`collect-live` uses the private `NEWSBOT_V2_TELETHON_*` variables and
`NEWSBOT_V2_TELEGRAM_HANDLES`. `generate-codex` is admitted only after the first
Telegram approval and uses the fixed privilege-separated Codex runner.
`deliver-google-sheets` is admitted only after the second exact-draft approval;
it reads `GOOGLE_SERVICE_ACCOUNT_FILE` and `GOOGLE_SHEETS_SPREADSHEET_ID`,
projects the immutable draft onto the frozen A:V schema, and uses the existing
prepared-mutation idempotency marker. A confirmed receipt is completed locally
after restart, while an ambiguous receipt enters `manual_review` without a
resend. None of these commands infer or open the legacy database.

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
