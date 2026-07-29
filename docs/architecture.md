# 아키텍처

## 토폴로지와 capability

하나의 async Python 3.12 모듈형 모놀리스가 여섯 채널과 승인 큐를 처리한다. SQLite는 foreign key, WAL, busy timeout, 짧은 transaction을 사용하는 유일한 durable authority다. 단일 SQLite writer/poller owner를 둔다.

- **수집:** credential-free fixture 또는 Telethon/MTProto.
- **승인:** scripted adapter 또는 Telegram Bot API.
- **생성:** fixture fake provider 또는 OpenAI-compatible provider.
- **저장:** snapshot, 평가, decision event, job, generation, outbox를 SQLite transaction으로 유지한다.

선택하지 않은 capability는 해당 secret를 읽거나 adapter/client를 만들지 않는다.

## 상태 흐름

```text
collection_intervals → source_posts/source_post_versions → candidate_evaluations/candidates
  → digests → [제작] selection → generation_jobs
  → current generations → review approval → export_outbox → ready JSON/Markdown pair
```

선택은 candidate, digest, 정렬된 source-version 집합에 결합된다. generation job은 lease token과 expiry를 사용한다. `queued`, `failed_recoverable`, 또는 lease가 만료된 `running` job만 다시 lease할 수 있다. source의 material edit는 current job/generation/review callback을 supersede하여 새 선택으로 되돌린다.

## 저장 모델과 지속 수집

`source_posts`는 channel/message identity와 provenance를, `source_post_versions`는 불변 material(text, URL, media)만 보관한다. edit timestamp와 engagement는 `source_post_observations`의 timestamped observation metadata다. 따라서 같은 material의 timestamp/metric refresh는 version을 만들지 않고, `None` missing과 observed `0`도 보존한다. `collection_intervals`는 initial floor, fixed upper bound, next page cursor, overlap frontier를 보관한다. 페이지와 overlap이 끝난 transaction 뒤에만 `collection_cursors`가 승격되어 중단 뒤에도 수집을 계속한다. `reconcile-live`는 bounded interval을 수집하며 normal cursor를 갱신하지 않는다.

`candidate_evaluations`는 ordered hard-filter result, score input/contribution, missing flag, raw referral query, warning/rationale, policy/config digest, sort key를 보관한다. `candidates`, `candidate_sources`, `digests`, `selections`, `generation_jobs`, `generations`, `generation_sources`, `decision_events`, `callback_tokens`, `export_outbox`는 exact source revision binding과 idempotency를 보존한다. callback token은 SHA-256 hash만 저장하며 consume 또는 state transition 때 sibling token을 revoke한다. defer는 stage와 due time을 보관하고, due poll은 current binding일 때만 정확한 selection/review callback을 재발행한다.

## 생성과 전달

Generation request의 fact packet은 stable claim/source/material/observation identity, exact capture time, source URL(없으면 `null`), evidence와 span, conflict와 server uncertainty를 담는다. provider response는 factual unit마다 exact claim/source reference를 포함해야 하며 unknown trust/override field는 거절된다. 검증된 exact packet은 `newsbot-generation-claim-v1` manifest로 immutable generation content에 저장된다. approval/export는 당시 provider에 준 manifest만 사용하고 최신 observation으로 재계산하지 않으며, 모든 portable factual reference가 manifest 항목 하나에 정확히 resolve되지 않으면 fail closed한다. validator는 source reference와 1–8 total page를 검사하고 adaptive page count는 선택 source body에서 결정론적으로 계산한다. Telegram adapter는 미리보기 text를 4096 UTF-16 code unit 경계로 분할한다.

정확한 review 승인 transaction은 approval event와 JSON/Markdown outbox intent를 만든다. `export_id`는 canonical semantic payload의 actionable source-version provenance, exact generation claim manifest, generation identity, approval decision identity, pages, caption, sorted warnings, `draft`, `source_reported`를 SHA-256해 만든 `exp_` + 32 lowercase hex이고 두 export가 공유한다. canonical bytes와 각 SHA-256은 SQLite에 있고 materializer는 temporary file, fsync, atomic rename, digest 검증을 수행한다. mismatch는 기존 파일을 보존하고 outbox를 `corrupt`로 표시한다. `ready` 쌍은 수동 Figma 편집과 Instagram 게시에 전달한다.
