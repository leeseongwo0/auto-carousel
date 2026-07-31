# 아키텍처

## 토폴로지와 capability

하나의 async Python 3.12 모듈형 모놀리스가 여섯 채널과 승인 큐를 처리한다. SQLite는 foreign key, WAL, busy timeout, 짧은 transaction을 사용하는 유일한 durable authority다. 단일 SQLite writer/poller owner를 둔다.

- **수집:** credential-free fixture 또는 Telethon/MTProto.
- **승인:** scripted adapter 또는 Telegram Bot API.
- **생성:** fixture fake provider 또는 OpenAI-compatible provider.
- **저장/전달:** snapshot, 평가, decision event, job, generation, approval, immutable Sheets handoff와 fenced 원격 작업 이력을 SQLite transaction으로 유지한다.

선택하지 않은 capability는 해당 secret를 읽거나 adapter/client를 만들지 않는다.

## 상태 흐름

```text
collection_intervals → source_posts/source_post_versions → candidate_evaluations/candidates
  → digests → [제작] selection → generation_jobs
  → current generations(category 포함) → final review approval
  → sheet_handoffs → fenced Sheets operation → workplace append
```

선택은 candidate, digest, 정렬된 source-version 집합에 결합된다. generation job은 lease token과 expiry를 사용한다. `queued`, `failed_recoverable`, 또는 lease가 만료된 `running` job만 다시 lease할 수 있다. source의 material edit는 current job/generation/review callback을 supersede하여 새 선택으로 되돌린다.

## 저장 모델과 지속 수집

`source_posts`는 channel/message identity와 provenance를, `source_post_versions`는 불변 material(text, URL, media)만 보관한다. edit timestamp와 engagement는 `source_post_observations`의 timestamped observation metadata다. 따라서 같은 material의 timestamp/metric refresh는 version을 만들지 않고, `None` missing과 observed `0`도 보존한다. `collection_intervals`는 initial floor, fixed upper bound, next page cursor, overlap frontier를 보관한다. 페이지와 overlap이 끝난 transaction 뒤에만 `collection_cursors`가 승격되어 중단 뒤에도 수집을 계속한다. `reconcile-live`는 bounded interval을 수집하며 normal cursor를 갱신하지 않는다.

`candidate_evaluations`는 ordered hard-filter result, score input/contribution, missing flag, raw referral query, warning/rationale, policy/config digest, sort key를 보관한다. `candidates`, `candidate_sources`, `digests`, `selections`, `generation_jobs`, `generations`, `generation_sources`, `decision_events`, `callback_tokens`는 exact source revision binding과 idempotency를 보존한다. callback token은 SHA-256 hash만 저장하며 consume 또는 state transition 때 sibling token을 revoke한다. defer는 stage와 due time을 보관하고, due poll은 current binding일 때만 정확한 selection/review callback을 재발행한다. Migration 003의 Sheets tables는 target binding, handoff, bootstrap, remote operation, retained lease, immutable event/probe를 보존하며 모든 authority FK는 `ON DELETE RESTRICT`다. Legacy `export_outbox` rows는 local-only audit로만 남고 새 승인에서는 생성·materialize하지 않는다.

## 생성과 Sheets 전달

Generation request의 fact packet은 stable claim/source/material/observation identity, exact capture time, source URL(없으면 `null`), evidence와 span, conflict와 server uncertainty를 담는다. provider response는 factual unit마다 exact claim/source reference와 `newsbot-category-v1`의 `AI|Blockchain`을 포함해야 한다. category와 모든 portable factual reference는 generation insertion 전에 fail closed로 검증된다. exact packet, category, pages와 caption은 immutable generation content에 저장되고 승인 뒤 재분류하지 않는다.

정확한 review 승인 transaction은 approval event와 하나의 immutable Sheets handoff를 함께 만든다. `export_id`는 canonical semantic payload의 provenance, generation identity, approval decision identity, category, pages와 caption을 포함한 SHA-256 identity다. 승인 transaction에서는 원격 호출을 하지 않는다.

`workplace` delivery worker는 고정 binding의 bootstrap과 delivery를 하나의 SQLite mutex로 직렬화한다. A1:V3 oracle과 controls를 검증한 뒤, document-scoped deterministic metadata 생성과 한 개의 typed `AppendCellsRequest`를 한 `spreadsheets.batchUpdate`에 넣는다. A는 빈 문자열이고 E는 `X`다. 정확한 metadata는 zero-write idempotency 증거이며 duplicate/conflict는 차단한다.

원격 mutation 전에 lease token/fence와 request SHA를 사용해 `possibly_sent`를 커밋한다. 그 뒤에는 exact metadata 또는 신뢰 가능한 원자적 rejection만 상태를 확정한다. timeout, 5xx, redirect, malformed response, process death, 음성/unavailable probe는 영구 ambiguity이며 자동 재전송하지 않는다. Bootstrap과 delivery operation의 lease/event/probe/terminal history는 삭제하지 않는다.
