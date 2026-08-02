# 뉴스 캐러셀 워크플로

Telegram의 고정된 여섯 소스 채널(`testingcatalog`, `ai_masters_community`, `aipost`, `coinnesskr`, `exilist_official`, `dolbikong`)을 자동 수집·평가하고, 사람이 승인한 후보만 `gpt-5.6-terra`로 유연하게 1–8페이지 카드뉴스 초안으로 만듭니다. 사람이 정확한 초안을 다시 승인하면 Google Sheets의 고정 `workplace` 탭에 idempotent하게 한 행을 추가합니다. SQLite가 cursor, outbox, 승인, 원고, handoff와 원격 작업의 유일한 durable authority입니다.

## 자동 사용자 흐름

1. `newsbot-collect.timer`가 수집 worker를 실행합니다. 페이지/chunk가 성공적으로 commit된 뒤에만 durable cursor가 전진하므로 crash, cap, timeout 뒤에도 이어서 수집합니다.
2. `newsbot-telegram.timer`가 후보 알림, callback polling, 선택된 작업의 생성 알림과 검토 알림을 처리합니다. 후보의 `[제작]`은 인간의 생성 허가일 뿐입니다.
3. Codex는 별도의 기존 `newsbot-generate-codex.timer`가 한 activation에 frozen job 하나만 처리합니다. 이 타이머의 역할과 containment는 변경하지 않습니다.
4. 검토자는 정확한 current draft에서 `[시트 전달]`을 승인합니다. 승인 transaction은 immutable Sheets handoff만 만들며 원격 호출은 하지 않습니다.
5. `newsbot-sheets.timer`가 승인된 handoff 하나를 전달합니다. document metadata가 정확히 일치하면 zero-write 재사용하며, ambiguous Telegram/Sheets 효과는 자동 재전송하지 않습니다.

생성 성공이나 후보 선택은 최종 전달 승인이 아닙니다. Figma/Instagram 자동화, 기존 행 변경, 다른 Sheets 탭 변경은 범위 밖입니다.

## 설치와 핵심 명령

```bash
uv sync --group dev
uv sync --group dev --extra sheets
uv run newsbot init-db --db var/e2e/newsbot.db
uv run newsbot status --db var/e2e/newsbot.db
```

운영 cutover는 immutable proposal receipt를 반드시 왕복합니다.

```bash
newsbot automation-cutover-preview --config /etc/newsbot/config.toml --db /var/lib/newsbot/newsbot.db --proposal-id <PROPOSAL_ID> --release-digest <RELEASE_DIGEST>
newsbot automation-cutover-apply --db /var/lib/newsbot/newsbot.db --proposal-id <PROPOSAL_ID> --proposal-sha256 <PREVIEW_PROPOSAL_SHA256> --release-digest <RELEASE_DIGEST>
```

`<…>`는 command 이름이 아니라 운영자가 receipt 또는 release manifest에서 대입하는 **명시적 placeholder**다. preview stdout의 proposal receipt와 SHA-256이 apply 입력과 정확히 같지 않으면 apply하지 않습니다.

운영 systemd unit은 다음 여섯 개입니다.

- `newsbot-collect.service` / `newsbot-collect.timer`
- `newsbot-telegram.service` / `newsbot-telegram.timer`
- `newsbot-sheets.service` / `newsbot-sheets.timer`

기존 `newsbot-generate-codex.service` / `newsbot-generate-codex.timer`는 별도 one-job Codex 경계이며 위 여섯 unit으로 대체하거나 합치지 않습니다.

## 안전한 운영

`automation-status`로 aggregate health를, `automation-quiescence-check`로 cutover 전 bounded quiescence assertion을 확인합니다. 특정 ambiguous notification은 payload를 보지 않는 `automation-notification-inspect`로 상태를 확인하고, 허용된 `manual_required` 상태만 `automation-notification-resolve`로 immutable resolution 합니다. Telegram 또는 Sheets 요청이 timeout, process death, 5xx, malformed response, unavailable probe 뒤 ambiguous가 되면 자동으로 다시 보내지 않습니다.

non-Codex 세 worker는 모두 `newsbot` UID, `/etc/newsbot/newsbot.env`, Telethon session, SQLite 및 worker locks를 공유합니다. 이들은 cross-unit isolation이나 `PrivateMounts` 보안 경계를 제공하지 않습니다. Codex만 login-shell 없는 별도 `newsbot-codex` UID에서 owner-only `CODEX_HOME`으로 device auth/provider credential을 보유하고, root-owned no-argument containment runner가 immutable authority/receipt를 attest한 activation만 그 UID에 전달하는 두 단계 경계입니다. immutable resume operation에 FK로 연결된 release row `COUNT(*)`를 권위로 유지하며 Codex token/API key를 `newsbot` 환경 파일에 넣거나 출력하지 않습니다.

상세 설치, monitoring, drain, recovery와 forward-only rollback은 [운영 가이드](docs/operations.md), 배포용 초보자 가이드는 `vps-deployment-guide.html`을 참조합니다. 최초 cutover는 switch 뒤 `init-db`로 schema를 만들고, 이후 post-007 release/rollback은 compatible forward switch만 사용하며 immutable history를 restore/delete/update하지 않습니다.
