# 뉴스 캐러셀 워크플로

Telegram의 여섯 소스 채널에서 후보를 수집·정렬하고, 사람이 주제를 선택한 뒤 생성 초안을 별도로 검토하여 **수동 Figma/Instagram 작업용** 내보내기 쌍을 준비하는 로컬 우선 도구입니다.

## 편집 흐름

1. 수집과 결정론적 후보 평가를 수행합니다. 이 단계에서는 AI 제공자를 만들거나 호출하지 않습니다.
2. `pending_selection` 후보 전용 다이제스트에 출처, 점수, 근거, 불확실성을 표시합니다. 생성 문구나 캡션은 포함하지 않습니다.
3. 권한 있는 사람이 정확한 후보/리비전에서 `[제작]`을 선택합니다. 선택은 하나의 재시도 가능한 생성 작업만 큐에 넣습니다.
4. 선택된 작업만 생성합니다. 생성 결과는 `draft`, `source_reported`, 충돌·불확실성 표시를 유지합니다.
5. 사람이 표시된 **정확한 초안 리비전**을 별도로 검토하여 승인, 재생성, 페이지 수 변경, 연기 또는 거절합니다.
6. 정확한 초안의 승인만 SQLite 내보내기 outbox를 만들고, 검증된 파일 쌍은 수동 Figma 편집 및 Instagram 게시에 넘깁니다.

선택은 승인이 아니며, 생성 성공도 승인이 아닙니다. Figma 렌더링·자동화, Instagram 인증/API/예약/게시, 배포는 구현 범위 밖입니다.

## 고정 소스 채널

`testingcatalog`, `ai_masters_community`, `aipost`, `coinnesskr`, `exilist_official`, `dolbikong` — 정확히 여섯 채널만 구성합니다.

## 로컬 명령

```bash
uv sync --group dev
uv run newsbot --help
uv run newsbot init-db --db var/e2e/newsbot.db
uv run newsbot run-fixture --config config/channels.toml --fixture tests/fixtures/channel_messages.json --db var/e2e/newsbot.db --output var/e2e/exports --page-count 2
uv run newsbot run-fixture --config config/channels.toml --fixture tests/fixtures/channel_messages.json --db var/e2e/newsbot.db --output var/e2e/exports --page-count 2 --scripted-approve
uv run newsbot status --db var/e2e/newsbot.db
uv run newsbot inspect --db var/e2e/newsbot.db --run-id 1
uv run newsbot repair-exports --db var/e2e/newsbot.db --output var/e2e/exports
```

`run-fixture`의 기본 실행은 자격증명·네트워크·제공자 없이 live와 같은 durable collection/cursor 경로로 고정 24시간 범위를 저장한 뒤 후보 다이제스트까지만 만듭니다. `--scripted-approve`는 `[제작]`으로 작업이 lease된 뒤에만 fixture fake 제공자를 만들고, 정확한 초안의 별도 승인 뒤에만 내보내기 쌍을 만듭니다. 이미 검증된 `ready` 쌍이 있는 동일 구성의 재실행은 수집·평가·callback을 변경하지 않고 그 쌍을 재사용합니다. `--page-count`를 생략하면 선택된 소스의 길이·문장 구조로 1~8 페이지를 결정론적으로 고릅니다.

실계정 표면은 명시적으로 선택해야 합니다. `collect-live`와 `reconcile-live`는 Telethon의 durable scan/cursor API를 사용하며 각각 필요한 Telethon 자격증명만 검사합니다. `generate-pending`는 선택·큐잉된 후보 하나만 lease한 뒤 제공자를 만들고, `openai_compatible`에는 OpenAI 설정 전체가 필요합니다. fake 제공자는 `--fixture-only` 검증에서만 허용됩니다. Bot API 명령은 Bot/승인자 자격증명만 필요하며 후보와 정확한 generation/source revision에 결합된 검토 초안을 보냅니다. `poll-approvals --process-generation`은 Bot과 선택 provider capability 전체를 네트워크·callback 처리 전에 검증합니다. 검토에서 거절된 terminal draft는 다시 생성 결과나 검토 알림으로 노출되지 않습니다. Telegram 메시지는 UTF-16 기준 4096 unit 이하로 분할됩니다.

```bash
uv run newsbot collect-live --config config/channels.toml
uv run newsbot reconcile-live --config config/channels.toml --channel testingcatalog --lookback-hours 24
uv run newsbot generate-pending --config config/channels.toml --db var/e2e/newsbot.db --candidate-id 1 --provider openai_compatible
uv run newsbot notify-candidates --db var/e2e/newsbot.db --run-id 1 --actor-id 123456
uv run newsbot notify-review --db var/e2e/newsbot.db --candidate-id 1 --generation-id 1 --actor-id 123456
uv run newsbot poll-approvals --db var/e2e/newsbot.db --timeout 0
```

## 문서

- [요구사항](docs/requirements.md)
- [아키텍처](docs/architecture.md)
- [운영](docs/operations.md)
- [ADR-0001](docs/adr/0001-local-first-sqlite-and-adapters.md)

## 전달 경계

승인된 실행의 완료 조건은 모든 검증 후 생성하는 **검증된 로컬 Git 커밋 1개**입니다. push, 배포, 릴리스, VPS 운영은 하지 않습니다.
