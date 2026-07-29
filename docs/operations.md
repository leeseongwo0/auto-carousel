# 로컬 운영 가이드

## 기본 원칙

모든 상태와 canonical export bytes는 로컬 SQLite에 둔다. fixture 명령은 자격증명과 네트워크가 필요 없다. Telegram과 OpenAI-compatible capability는 해당 명령이 선택될 때만 필요한 환경 변수를 검사한다. 비밀값을 CLI 인수나 로그에 넣지 않는다.

현재 구현된 credential-free 흐름은 다음과 같다.

```bash
uv run newsbot init-db --db var/e2e/newsbot.db
uv run newsbot run-fixture --config config/channels.toml --fixture tests/fixtures/channel_messages.json --db var/e2e/newsbot.db --output var/e2e/exports
uv run newsbot reconcile --config config/channels.toml --fixture tests/fixtures/channel_messages.json --channel exilist_official --lookback-hours 24 --db var/e2e/newsbot.db --output var/e2e/exports
uv run newsbot rank --config config/channels.toml --db var/e2e/newsbot.db --output var/e2e/exports
uv run newsbot status --db var/e2e/newsbot.db
uv run newsbot inspect --db var/e2e/newsbot.db --run-id 1
uv run newsbot repair-exports --db var/e2e/newsbot.db --output var/e2e/exports
```

기본 `run-fixture`는 live와 같은 durable collection/cursor 경로로 고정 24시간 범위를 저장하고 후보 다이제스트까지만 만든다. `reconcile`은 fixture snapshot을 불변 source version/별도 engagement observation으로 저장하고 후보를 다시 평가한다. `rank`는 이미 저장된 최신 observation만 평가한다. 모두 provider, Telethon import, 네트워크, 자격증명이 없다. `--scripted-approve`는 fixture fake provider로 선택, 생성, 검토 승인, export materialization을 수행한다. 동일 구성에 검증된 `ready` 쌍이 있으면 재실행은 수집·평가·callback·파일을 변경하지 않고 기존 쌍을 반환한다.

## capability별 환경 변수

| 명령 | 필요한 환경 변수 |
|---|---|
| `init-db`, `run-fixture`, `reconcile`, `rank`, `status`, `inspect`, `repair-exports` | 없음 |
| `auth-telethon` | `TELEGRAM_API_ID`, `TELEGRAM_API_HASH`, `TELEGRAM_SESSION_PATH` |
| `collect-live`, `reconcile-live` | `TELEGRAM_API_ID`, `TELEGRAM_API_HASH`, `TELEGRAM_SESSION_PATH` |
| `notify-candidates`, `notify-review`, `poll-approvals` | `TELEGRAM_BOT_TOKEN`, `NEWSBOT_APPROVER_CHAT_ID`, `NEWSBOT_APPROVER_USER_IDS` |
| `generate-pending --provider openai_compatible` | `OPENAI_BASE_URL`, `OPENAI_API_KEY`, `OPENAI_MODEL`, `OPENAI_TIMEOUT_SECONDS` |
| `generate-pending --provider fake --fixture-only` | 없음 |
| `poll-approvals --process-generation --provider openai_compatible` | Bot API 변수와 OpenAI-compatible 변수 모두 |
| `poll-approvals --process-generation --provider fake --fixture-only` | Bot API 변수만 |

`poll-approvals --process-generation`은 Bot과 선택 provider의 인수·capability를 Telegram import, 네트워크 요청, callback 적용보다 먼저 함께 검증한다. 검토 거절은 terminal이며 이후 `generate-pending`과 `notify-review`가 해당 draft를 다시 노출하지 않는다.

`NEWSBOT_DATABASE`와 `NEWSBOT_OUTPUT_DIR`는 각각 기본 DB와 output 경로를 바꾼다. `--db`와 `--output`은 해당 명령의 환경 값보다 우선한다. `.env.example`의 값은 비어 있으며 fixture에는 환경 변수가 없다.

실제 adapter 명령의 표면은 다음과 같다.

```bash
uv run newsbot auth-telethon
uv run newsbot collect-live --config config/channels.toml --db var/live/newsbot.db --output var/live/exports --lookback-hours 24 --page-size 100 --max-pages 10
uv run newsbot reconcile-live --config config/channels.toml --db var/live/newsbot.db --output var/live/exports --channel testingcatalog --lookback-hours 24 --page-size 100 --max-pages 10
uv run newsbot reconcile-live --config config/channels.toml --db var/live/newsbot.db --output var/live/exports --channel testingcatalog --from-id 100 --to-id 200 --page-size 100 --max-pages 10
uv run newsbot generate-pending --config config/channels.toml --db var/live/newsbot.db --output var/live/exports --candidate-id 1 --provider openai_compatible
uv run newsbot notify-candidates --db var/live/newsbot.db --run-id 1 --actor-id 123456
uv run newsbot notify-review --db var/live/newsbot.db --candidate-id 1 --generation-id 1 --actor-id 123456
uv run newsbot poll-approvals --config config/channels.toml --db var/live/newsbot.db --output var/live/exports --timeout 0
uv run newsbot poll-approvals --config config/channels.toml --db var/live/newsbot.db --output var/live/exports --timeout 0 --process-generation --provider openai_compatible
```

`auth-telethon`만 interactive MTProto authorization을 연다. `collect-live`와 `reconcile-live`는 MTProto로 configured channel을 읽는다. session parent는 owner-only로 만들고, 완료 뒤 session은 owner-only regular file이어야 한다. `reconcile`과 `reconcile-live`는 `--channel`과 lookback 또는 정확한 ID range 하나를 받는다. `--from-id`와 `--to-id`는 함께 필요하고 양수이며 `from <= to`; 양 끝은 inclusive다. range mode는 newest-message 조회 없이 지정한 ID 경계와 `--page-size`/`--max-pages` cap 안에서 결정론적으로 page한다.

## 복구와 점검

- `collect-live`는 `collection_intervals`의 fixed floor, upper bound, page/overlap frontier를 저장한다. 각 scan의 실제 capture time을 observation에 기록한다. cap, crash, timeout, 또는 bounded FloodWait 재시도 실패 뒤 같은 명령을 다시 실행하면 미완료 interval에서 계속한다. 한 channel의 live failure는 이미 commit된 progress를 보존하고 다른 channel의 scan/ranking을 막지 않으며 `channel_errors`로 출력한다.
- `reconcile-live`는 lookback 또는 정확한 inclusive ID range를 `--page-size`와 `--max-pages`로 제한해 수집하고 normal cursor를 변경하지 않는다. cap에 걸린 range는 같은 range 명령을 다시 실행해도 cursor를 바꾸지 않는다.
- 선택된 generation job은 SQLite lease로 `queued`, `failed_recoverable`, 또는 만료된 `running` 상태에서 하나를 복구해 생성한다. provider 실패는 redacted recoverable failure만 기록하며 draft, approval, outbox를 만들지 않는다. review의 6/24/72시간 연기는 due time 뒤 `poll-approvals`가 `resume_due`로 pending selection/review를 복원하고 해당 digest/draft를 다시 보낸다.
- callback은 hash와 exact candidate/source-version/draft binding을 검사한다. consume 또는 상태 변경은 sibling callback을 revoke하므로 stale/revoked token은 무해하다. defer는 selection/review stage와 due time을 저장하고, due poll은 source binding이 current일 때만 정확한 digest 또는 current draft callback을 다시 보낸다.
- fixture와 live reconciliation은 모두 bounded다. `reconcile`과 `reconcile-live`는 대상 `--channel` 하나와 양수 lookback 또는 inclusive positive ID range 하나만 `page_size`/`max_pages` cap 안에서 읽으며 normal cursor를 절대 전진시키지 않는다.
- `status`와 `inspect --run-id`는 redacted local counter와 run 요약을 출력한다. `[제작]` 전 `provider_calls_before_selection`은 0이다.
- `repair-exports --db … --output …`는 verified JSON에서 누락된 Markdown만 다시 materialize한다. SQLite canonical bytes/digest와 맞지 않는 파일은 보존하고 outbox를 `corrupt`로 남긴다. `ready` JSON/Markdown 쌍만 수동 handoff 대상이다.

## 수동 경계

Figma 편집과 Instagram 게시는 수동이다. VPS 스케줄링, backup/retention, secret transport, health monitoring, shutdown, rollback, 배포는 이 도구가 제공하는 명령이 아니다.
