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
The current public synthetic configuration has exactly five enabled channels.
The former six-channel frontier remains immutable history. Traceable descendants
of work already authorized before the five-channel activation are grandfathered
and must retain their durable authority/history.

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

For an existing automation installation, drain the three non-Codex service/timer
pairs and the Codex timer before changing its runtime. The legal authority order
is `collect → noon → Telegram → Sheets → Codex`; verify inactive workers,
authority locks, and clean Codex containment before building or switching a
release. Keep a backup according to the installation's own retention policy.
The canonical worker-lock order remains `collect → Telegram → Sheets`; the noon state-machine drain occurs between collection shutdown and Telegram drain.

A private config must be staged beneath a `root:newsbot` trusted directory with
mode 0750. The staged and live files must be regular, single-link
`root:newsbot` files with mode 0640; reject symlinks, additional hard links, or
ownership/mode drift. Promote the validated stage using same-filesystem atomic
replacement. Root alone archives the validated semantic preimage and manifest;
operator status exposes only a redacted five-channel topology.

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
the proof between build and switch. The preview/apply sequence is staged
activation: it appends the release digest, `AppConfig.digest`, and canonical
`news_policy_v1` payload as one immutable binding. The latest valid five-channel
release/config binding is current authority; only its unchanged pair may receive
canonical replay.

On binding drift, root alone may start or clean up Codex containment, with zero
Newsbot job/provider authority. Normal Codex restoration still requires its
separate clean receipt and admission gate. Rollback is a forward switch: with every
worker still disabled and quiescence attested, switch and re-attest the prior
six-capable verified runtime first; then atomically install and attest the exact
archived six-channel config and use that prior runtime to append/replay its
release/config activation.
Preserve migrations, approvals, cursors, outboxes, attempts, remote history,
and containment receipts. Resolve production-specific incidents through that
installation's private runbook; do not copy host names, account identities,
credentials, or payloads into this public documentation.
