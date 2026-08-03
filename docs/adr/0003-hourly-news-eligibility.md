# ADR 0003: Hourly collection and deterministic news-only approval

- Status: Accepted
- Date: 2026-08-03

## Context

The existing ranking policy remains a prerequisite but does not establish newsworthiness. Approval buttons must be limited to reproducibly newsworthy material without adding a provider, network fetch, clock dependency, sender, service, timer, lock, UID, credential, or authority boundary. The fixed six-channel topology, existing one-job Codex containment, and Google Sheets handoff/delivery authority remain unchanged.

## Decision

`newsbot-collect.timer` runs hourly after collection-service inactivity. All directives other than `OnUnitInactiveSec=1h` remain unchanged.

A second offline deterministic policy, `news_policy_v1`, runs after ranking eligibility and before candidate callback or Telegram intent creation. It uses frozen config and normalized source-local observations; trusted classification, analysis, evidence marker, and eligible external URL may not be combined across observations. It has no AI, provider, network, or host-clock dependency.

| Ordered condition | Outcome / stable reason | Route |
|---|---|---|
| Ranking ineligible | `non_news/ranking_ineligible` | Silent |
| Event marker + material context + no promotion/tutorial/reaction marker | `definite_news/clean_event` | Immediate approval |
| Trusted official/original source + meaningful analysis + no negative marker | `trusted_analysis/trusted_source_analysis` | Immediate approval |
| Community/aggregator meaningful analysis + evidence marker + eligible external URL + no negative marker | `definite_news/evidenced_analysis` | Immediate approval |
| Positive signal collides with negative marker; event lacks material context; or untrusted meaningful analysis lacks same-source evidence marker or eligible URL | `ambiguous/policy_collision_or_insufficient_evidence` | Noon title window |
| Promotion/tutorial/reaction only | `non_news/negative_only` | Silent |
| No decisive signal | `ambiguous/no_decisive_signal` | Noon title window |

The route is atomically persisted with policy evaluation. Immediate outcomes alone create `pending_selection`, selection digest, callback, and immediate outbox work. Ambiguous results create immutable first-wins title snapshots, once per story/window; material edits do not replace a window title and unchanged content is not repeated. Non-news results create no approval work.

## Noon admission and delivery

The only approved configuration is `news_policy_v1`, `Asia/Seoul`, noon `12:00`, and `activation_minutes = 60`. Assignment and admission acquire `BEGIN IMMEDIATE` first and sample the aware clock only after the SQLite write lock. Assignment before 12:00 targets the same local date; at or after 12:00 it targets the next date.

For today's window, the full half-open admission interval is `[12:00:00,13:00:00)` Asia/Seoul. During that interval, ordered frozen titles and exactly one noon intent are committed atomically and the window becomes `queued`; an empty/absent window becomes `empty`. At exactly `13:00:00`, and after it, a still-collecting or absent window becomes terminal `skipped`: no intent, catch-up, or rollover is allowed. A tick that starts before 13:00 but obtains the write lock at exactly 13:00 skips.

An intent committed from an in-window post-lock sample is durable authority, not an expiry-based notification. It may dispatch or safely retry later, including after 13:00 or process recovery. Noon payloads are frozen ordered titles separated by newlines, with no markup, callback, body, source, reason, or header. Chunks preserve whole titles and are at most 4,096 UTF-16 units. Accepted chunks are terminal evidence and are never blindly resent. A retryable trusted rejection resumes at the first unaccepted noon chunk; an ambiguous result, or a partial accepted prefix, remains manual and requires immutable operator resolution.

## Migration and activation authority

Migration 008 retains migration-007 Telegram authority while rebuilding the outbox under schema, ID/state/evidence, dependent-row, foreign-key, trigger, index, and reopen parity checks. It adds immutable policy evaluations, ambiguous windows/items, and release/config bindings. It creates no backfill, window, callback, notification, or other work. Every policy-derived row is authorized by a required immutable release/config binding FK.

Workers admit only the latest activation with exactly one binding and require runtime config digest, canonical policy payload, and policy version to match before any cursor, callback offset, window, lease, or outbox mutation. A changed activation is quiescence-gated: prior-binding collecting windows and noon outbox states `pending`, `claimed`, `sending`, `ambiguous`, and `partial_manual_required` block it. A historical queued window is admissible only after its outbox is terminal (`sent`, `canceled`, `resolved_delivered`, or `resolved_abandoned`).

| Current latest pair | Requested pair | Result |
|---|---|---|
| Legacy activation without binding | Any validated pair | Under quiescence append a new activation and binding; never retrofit |
| Same release, same config | Exact replay | `changed=false`; return current IDs without quiescence/write |
| Same release, different config | Config-only rollout | Under quiescence append activation with same release and new binding |
| Different release, same config | Code-only rollout | Under quiescence append activation and binding with same digest |
| Different release, different config | Combined rollout | Under quiescence append activation and binding |
| Changed pair with validation/config/window failure | Invalid | Fail before insert; no `changed` result |
| Crash/exception between activation and binding inserts | Any | One transaction rolls both back |

## VPS rollout and rollback

Drain all four timers/services, hold canonical collect → Telegram → Sheets locks, verify Codex containment, and back up the database. Build, switch, and attest the runtime under the quiescence proof. Then run migration 008 and its foreign-key/parity checks, append the release/config activation under the table above, and run no-work canaries. Restore timers in Telegram → Sheets → collection order. Restore Codex only through its unchanged separate containment gate.

Rollback is forward-only: drain again, use only a verified migration-008-compatible runtime, re-run switch/migration parity checks, and append a new activation/config binding under the same quiescence rules. Never restore, delete, or update the database or immutable migration, window, cursor, outbox, attempt, audit, Codex, or Sheets history; never run a pre-008 binary after the migration.

## Consequences

The system gains deterministic news-only immediate approvals and a durable noon title route while retaining the existing Telegram sender, lease/fence, outbox, chunk/attempt, inspection, resolution, and no-blind-resend authority. No fifth timer/unit or separate noon sender/outbox is introduced.
