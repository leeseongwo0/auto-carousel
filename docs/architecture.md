# 아키텍처

## 토폴로지와 권한 경계

하나의 async Python 3.12 모듈형 모놀리스가 고정 여섯 채널(`testingcatalog`, `ai_masters_community`, `aipost`, `coinnesskr`, `exilist_official`, `dolbikong`)과 승인 큐를 처리한다. SQLite는 foreign key, WAL, busy timeout, 짧은 transaction을 사용하는 유일한 durable authority다.

자동화는 세 non-Codex one-shot worker와 timer로 구성된다.

- `newsbot-collect.service` / `.timer`: fenced hourly-after-inactive collection, ranking, deterministic `news_policy_v1`, durable cursor advancement
- `newsbot-telegram.service` / `.timer`: candidate/review notification outbox, Telegram callback polling, and noon-digest admission/dispatch
- `newsbot-sheets.service` / `.timer`: post-baseline approved handoff의 fenced Sheets delivery

세 worker는 `newsbot` UID, `/etc/newsbot/newsbot.env`, Telethon session, SQLite와 locks를 공유한다. 따라서 이들은 서로 독립된 보안 sandbox가 아니며 cross-unit isolation 또는 private mount를 주장하지 않는다. timer concurrency와 worker locks는 correctness coordination이지 privilege separation이 아니다.

Codex는 이 경계와 분리된다. 기존 `newsbot-generate-codex.service` / `.timer`는 한 activation에 frozen generation job 하나만 처리하며, 별도 login-shell 없는 `newsbot-codex` UID와 owner-only `CODEX_HOME`을 사용한다. `newsbot`은 Codex token/API key를 보관하지 않는다. root-attested no-argument runner, immutable containment authority, receipt와 FK-linked immutable release rows의 `COUNT(*)`가 Codex admission/recovery 권위다. 기존 Codex timer는 collect/telegram/sheets timer와 결합되지 않는다.

## 상태 흐름

```text
collection intervals/chunks → durable collection cursor → ranking → news_policy_v1
  ├─ definite_news/trusted_analysis → candidates/digest → Telegram candidate outbox
  │    → human [제작] approval → frozen generation job
  │    → one-job Codex generation (gpt-5.6-terra, 1–8 pages) → review outbox
  │    → human [시트 전달] approval → immutable sheet_handoff → Sheets outbox/operation
  │    → workplace append or manual resolution
  ├─ ambiguous → immutable first-wins noon title window → Telegram noon title chunks
  └─ non_news → silent
```

collection cursor는 channel page/chunk가 durable transaction으로 끝난 뒤에만 전진한다. notification outbox는 intent와 delivery state를 남긴다. callback, selection, current draft review와 handoff는 exact source/draft revision binding 및 idempotency를 보존한다. 사람의 `[제작]` 승인과 `[시트 전달]` 승인은 별도이며, 전자는 final delivery 권한이 아니다.
`news_policy_v1`는 ranking을 대체하지 않는 두 번째 offline gate다. immediate, noon, silent route는 평가와 원자적으로 하나만 기록된다. 정오 assignment와 admission은 모두 `BEGIN IMMEDIATE` write lock 뒤의 aware Asia/Seoul clock sample로 linearize한다. assignment는 12:00 전이면 당일, 12:00부터는 다음 날이다. admission은 `[12:00:00,13:00:00)`에서만 noon intent와 `queued`를 함께 commit한다. 정확히 13:00 이후 intent가 없으면 `skipped`이며 catch-up/rollover가 없다. 이미 on-time commit된 intent에는 expiry가 없고 이후 dispatch/retry할 수 있다.

`gpt-5.6-terra`는 근거의 양에 맞춰 가장 짧고 유용한 총 1–8페이지를 선택한다. 1페이지는 표지이고, 2–8페이지는 표지와 1–7개 본문이다. category와 portable fact references는 immutable generation insertion 전 fail closed로 검증한다.

## 전달과 no-resend 규칙

review 승인 transaction은 remote call 없이 approval event와 immutable Sheets handoff 하나를 만든다. Sheets worker는 binding mutex, owner token과 monotonic fence로 bootstrap/delivery를 직렬화하고, document-scoped deterministic metadata와 append를 atomic batch로 요청한다. 정확한 metadata는 zero-write idempotency 증거다. duplicate/conflict metadata는 차단한다.

원격 mutation 전에 request SHA와 `possibly_sent`를 commit한다. 이후 timeout, reset, 5xx, redirect, malformed response, process death, unavailable/negative probe는 ambiguous이며 Telegram 또는 Sheets 효과를 자동 재전송하지 않는다. exact metadata/transport evidence 또는 trusted atomic rejection만 terminal state를 확정한다. retained cursor, outbox, per-chunk operation, lease, event, probe와 terminal history는 삭제하지 않는다.
Noon digest는 immutable ordered title만 newline으로 전송하며 callback/markup이 없다. whole-title chunk는 최대 4,096 UTF-16 units이고 accepted chunk는 다시 보내지 않는다. retryable trusted rejection은 첫 미accepted noon chunk부터 재개할 수 있지만, ambiguous 또는 accepted prefix 뒤 partial effect는 자동 재claim하지 않고 operator resolution을 요구한다.

## Cutover와 복구

release는 네 timer를 disable하고 네 service를 drain한 뒤 세 application flock, read-only DB authority와 Codex clean containment를 확인하고 backup한다. 같은 root-owned quiescence proof를 사용해 `deploy/build_newsbot_release.py build <COMMIT_SHA> --quiescence-proof <PATH>`로 versioned runtime과 negative capability candidates를 build/attest한 뒤 stable entrypoint를 switch/re-attest한다. 그 이후에만 새 runtime의 `init-db`, foreign-key/parity check, baseline 검증, `automation-cutover-preview`와 exact proposal ID/SHA-256/release digest를 요구하는 `automation-cutover-apply`를 실행한다.
Migration 008은 migration-007 Telegram outbox와 its IDs, state, attempts, chunks, callbacks, events, resolutions의 parity를 보존하여 rebuild하고 immutable policy evaluation, noon window/item, release/config binding authority를 추가한다. 각 policy-derived row는 immutable activation/config binding FK로 권한화된다. worker는 latest activation의 정확히 하나인 binding 및 runtime config/policy digest를 mutation 전에 확인한다. legacy activation을 retrofit하지 않으며, same release/same config replay만 write 없이 current IDs를 반환한다. config-only, code-only, combined change와 forward rollback은 quiescence 아래 새 activation과 binding을 append한다; old binding의 collecting window 또는 nonterminal noon outbox가 있으면 차단한다.
planned drain과 runtime helper는 disabled timers, inactive services, 세 flocks, open authorities와 Codex containment를 switch 전후로 재검증한다. status와 notification inspect/resolve는 redacted aggregate/state만 사용한다. alarm, failed unit, stale lease, collecting window at/after 13:00, skipped window, ambiguous/partial operation은 monitoring 대상이다. ambiguity는 inspect/probe와 명시적 immutable resolution으로만 해소한다.
Codex restoration은 별도 gate다: timer 중지, inactive/cgroup-empty proof, clean/reset receipt를 참조하는 durable `clean` state, root artifact/effective profile attestation, migration/manifest 일치, provider-free canary, one-job canary 및 live activation 후에만 `newsbot-generate-codex.timer`를 복원한다. urgent stop 중 process/remote effect가 불명확하면 timer를 멈추고 state를 ambiguous/dirty로 보존하며 재실행하지 않는다. rollback은 verified migration-008-compatible runtime으로만 forward 전환하고 같은 quiescence 아래 새 activation/config binding을 append한다. DB migration, immutable audit/receipt, cursor/outbox/window/attempt 또는 remote history를 restore/delete/update하여 되돌리지 않으며 교정 release와 새 audit로 진행한다.
