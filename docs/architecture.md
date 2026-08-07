# Newsbot architecture

## Public v1 topology

Newsbot is an async Python modular monolith. The reusable v1 topology is local
and human-directed: commands run on one machine, SQLite is the durable
authority, and a person approves work before generation and again before
external delivery. Public defaults are synthetic so the repository can be used
without production identities, credentials, destinations, or hosted services.

SQLite uses foreign keys, WAL, bounded transactions, and durable records for
collection progress, candidates, approvals, drafts, handoffs, and delivery
attempts. The database is the source of truth; derived output and remote effects
do not replace its records.

```text
local collection → durable cursor → ranking and policy → candidate
                                                    → human approval
                                                    → frozen generation job
                                                    → exact-draft approval
                                                    → durable handoff → optional delivery
```

The deterministic policy is an additional offline gate after ranking. It records
one route per evaluation. A generation approval is not permission to deliver;
delivery requires approval of the exact current draft.

## Delivery safety

A delivery handoff is immutable. Before a remote mutation, Newsbot records the
information needed to identify the attempt. If a request times out, a process
ends unexpectedly, a response is malformed, or the remote result cannot be
verified, the result is ambiguous. Newsbot must not automatically resend an
ambiguous effect. Inspection, trusted remote evidence, or an explicit immutable
resolution determines the next state.

Idempotent delivery adapters use destination metadata where available. Exact
metadata can prove a zero-write replay; duplicate or conflicting metadata blocks
delivery. Durable cursors, outboxes, attempts, approvals, handoffs, and terminal
history are preserved rather than altered to simulate a clean retry.

## Legacy automation compatibility

An existing private deployment can optionally enable a VPS automation topology:
scheduled collection, Telegram review and notifications, Google Sheets delivery,
Asia/Seoul noon routing, and one-job Codex generation. None of these components
is required for the public v1 workflow.

The non-Codex legacy workers share a database, configuration, session material,
and coordination locks. Those shared resources provide correctness coordination,
not a claim of isolation between workers. Codex is a separate compatibility
boundary: provider credentials remain outside the non-Codex configuration, and
its containment authority and receipts control generation admission and
recovery.
The current public synthetic configuration has exactly five enabled channels. Its
six-channel frontier is immutable history, not a second current topology.
Traceable descendants of work already authorized before the five-channel activation
are grandfathered; no later topology change rewrites that authority.

For legacy noon routing, assignment and admission are linearized with the
database write lock and an aware Asia/Seoul clock sample. An intent is admitted
only from 12:00:00 through 12:59:59. A missing intent at 13:00 is skipped without
catch-up. A timely committed intent can dispatch through its durable outbox;
accepted chunks are never blindly resent, and partial or ambiguous effects need
explicit resolution.

## Releases and history

Local users can update their checkout and database through the documented
commands for their chosen version. Existing automation deployments drain the
three non-Codex service/timer pairs and the Codex timer in the legal authority
order—collection, noon, Telegram, Sheets, then Codex—before a private release.
They verify durable authority and optional Codex containment, build a candidate,
perform the stable runtime switch/re-attest step, run init-db with migration and
foreign-key checks, and append a new activation only after validation.

Private configuration is staged in a `root:newsbot` 0750 trusted directory.
Both staged and live config files are regular, single-link `root:newsbot` 0640
files; a same-filesystem atomic replacement promotes the validated stage. Root
alone archives the semantic preimage and manifest. Operator-visible topology
status is redacted and identifies only the current five-channel shape.

A staged activation appends the exact release digest, `AppConfig.digest`, and
canonical `news_policy_v1` payload as one immutable binding. The latest valid
five-channel release/config binding is current authority. Canonical replay is
permitted only for that unchanged pair. A compatible rollback is forward-only:
while workers remain disabled, switch and re-attest the prior six-capable verified
runtime before atomically installing and attesting the exact archived six-channel
configuration; that prior runtime then appends/replays its release/config activation.
It does not erase or edit migration state, cursors, outboxes, attempts,
approvals, remote history, or containment receipts. The public GitHub `main`
history is likewise canonical and must not be rewritten. Use ordinary commits,
revert commits, and corrective releases instead of force-pushing or replacing
published history.

On binding drift, only root may start or clean up Codex containment; that path
has zero Newsbot job or provider authority. Normal Codex admission remains
controlled by its immutable containment authority and receipts.
