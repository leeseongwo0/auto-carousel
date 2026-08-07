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

For legacy noon routing, assignment and admission are linearized with the
database write lock and an aware Asia/Seoul clock sample. An intent is admitted
only from 12:00:00 through 12:59:59. A missing intent at 13:00 is skipped without
catch-up. A timely committed intent can dispatch through its durable outbox;
accepted chunks are never blindly resent, and partial or ambiguous effects need
explicit resolution.

## Releases and history

Local users can update their checkout and database through the documented
commands for their chosen version. Existing automation deployments use a
private release procedure that drains workers, verifies durable authority and
optional Codex containment, builds a candidate, performs the stable runtime
switch/re-attest step, runs init-db with migration and foreign-key checks, and
records a new activation only after validation.

Release and configuration bindings are append-only. A compatible rollback is a
forward switch to a verified or corrective runtime; it does not erase or edit
migration state, cursors, outboxes, attempts, approvals, remote history, or
containment receipts. The public GitHub `main` history is likewise canonical and
must not be rewritten. Use ordinary commits, revert commits, and corrective
releases instead of force-pushing or replacing published history.
