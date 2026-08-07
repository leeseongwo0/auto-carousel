# 제품 및 운영 요구사항

## 범위와 실행 흐름

Newsbot은 Python 3.12 모듈형 모놀리스와 SQLite 내구성 권위를 사용합니다. 공개 기본 경로는 사용자가 작성한 `newsbot.behavior.v1` 프로필, 제한된 로컬 입력, 명시적인 후보 선택·검토, 로컬 JSON/Markdown 내보내기로 구성되며 Telegram·Google Sheets·고정 시간대 권한을 만들지 않습니다. 저장 경로와 프로필은 실행마다 명시하고 공개 예시는 합성 소스만 사용합니다.

기존 자동 운전은 선택적 호환 계층입니다. 공개 합성 설정의 현재 권위는 정확히 다섯 enabled 채널이며, 과거 여섯 채널 frontier와 그 immutable history는 변경하지 않습니다. 해당 여섯 채널 frontier에서 activation 전에 권한을 얻고 traceable한 descendant work는 grandfathered입니다. `newsbot-collect`, `newsbot-telegram`, `newsbot-sheets`의 세 service/timer 쌍과 별도 `newsbot-generate-codex.timer`가 비공개 운영 프로필에서 Telegram 승인, Asia/Seoul 정오 분류, Google Sheets 전달을 유지합니다. 운영자는 공개 합성 설정을 그대로 배포하지 않고 자신의 비공개 프로필과 자격 증명 경계를 별도로 관리해야 합니다.

수집 cursor는 각 page/chunk transaction이 성공한 뒤에만 전진해야 한다. crash, cap, timeout과 FloodWait 뒤에는 durable frontier에서 계속해야 한다. Telegram notification outbox, Sheets remote operation, per-chunk records와 terminal history는 retention 대상이며 삭제하거나 추측으로 재작성하지 않는다.
`ambiguous` 글은 버튼·본문·URL·판정 이유 없이 제목만 내구성 있게 보관한다. Asia/Seoul 기준 `[12:00:00,13:00:00)`에 같은 Telegram worker가 하루 한 번의 intent를 commit하며, 정확히 13:00까지 intent가 없으면 그 날짜를 `skipped`로 종결하고 catch-up이나 rollover를 하지 않는다. 시간 안에 commit한 intent는 13:00 이후에도 기존 no-blind-resend 규칙으로 전송·안전 재시도할 수 있다.

## 인간 승인과 생성

수집/후보 평가는 provider를 호출하지 않는다. 기존 ranking과 `news_policy_v1`을 모두 통과한 실제 뉴스만 즉시 승인 후보가 된다. 권한 있는 인간의 `[제작]`은 exact candidate/digest/source-version binding의 generation job 하나만 만들며, 중복 callback은 job을 만들지 않는다. 생성은 그 frozen job만 lease한다.

`gpt-5.6-terra` 생성은 근거와 내용에 맞춰 가장 짧고 유용한 총 1–8페이지를 유연하게 선택한다. 1페이지는 표지, 2–8페이지는 표지와 1–7개 본문이다. category는 `AI|Blockchain`이어야 하며 page/text/factual-reference 검증 실패는 draft, approval, handoff를 만들지 않는다.

사람은 정확한 current draft를 검토하여 승인, 재생성 또는 거절한다. 최종 승인만 immutable Sheets handoff를 atomic하게 만들며 approval transaction은 원격 호출을 하지 않는다.

## idempotent delivery와 ambiguity

Sheets 대상은 고정 `workplace` 탭(sheetId 0)뿐이다. worker는 binding mutex, random owner token, monotonic fence를 사용하고 request SHA 및 `possibly_sent`를 remote mutation 전에 commit한다. exact document metadata가 있으면 zero-write 재사용하고 duplicate/conflict metadata는 block한다. 기존 행은 수정하지 않는다.

timeout, reset, 5xx, redirect, malformed response, process death, negative/unavailable probe 뒤 Telegram 또는 Sheets 효과는 ambiguous다. 자동 resend는 금지한다. exact metadata 또는 verified transport evidence만 delivered를 증명하며, trusted atomic rejection만 settled-not-applied가 될 수 있다. `automation-notification-inspect`로 상태만 확인하고 supported `manual_required` 상태는 `automation-notification-resolve --expected-status manual_required --resolution delivered|abandoned --actor-id <ACTOR_ID> --reason-code transport_verified|operator_abandoned`로 immutable하게 해소한다.

## systemd 보안 모델

`newsbot-collect.service`, `newsbot-telegram.service`, `newsbot-sheets.service`는 모두 `newsbot` UID, 같은 environment file, Telethon session, SQLite database와 locks를 공유한다. 이는 실용적인 운영 배치이며 cross-unit isolation, separate credential boundary 또는 private mounts를 제공하지 않는다. secret는 owner-only env/session/service-account file에 두고 로그, receipt, SQLite safe output에 넣지 않는다.

Codex만 별도 login-shell 없는 `newsbot-codex`와 owner-only `CODEX_HOME`을 사용한다. `newsbot-generate-codex.service`의 root-owned no-argument runner, immutable `dirty|clean` containment authority와 attested receipt가 admission을 통제한다. global pause/resume/release authority의 count는 mutable counter가 아니라 immutable resume operation을 FK로 참조하는 `provider_resumed` release row의 `COUNT(*)`다. FK/cardinality/version 불일치는 fail closed다.

## 배포, monitoring, recovery

runtime 변경 전에는 세 service/timer 쌍과 Codex timer를 disable하고, collect → noon → Telegram → Sheets → Codex의 법적 authority drain을 완료한다. Codex는 clean containment, cgroup-empty/inactive proof를 확인한다. 세 application flock, read-only DB authority를 확인하고 backup한 뒤 같은 root-owned quiescence proof로 `deploy/build_newsbot_release.py`의 versioned build/attestation과 stable entrypoint switch/re-attestation을 완료한다. 새 runtime에서 migration/FK check와 migration 008 schema parity를 검증하고 exact release/config binding을 append한다.
trusted config directory는 `root:newsbot` 0750이고, config staging file과 live file은 regular single-link `root:newsbot` 0640이어야 한다. staging과 live replacement는 같은 filesystem에서 atomic하게 수행하며 symlink, hard-link, mode 또는 ownership 불일치는 fail closed다. root만 validated semantic preimage와 manifest를 root-only archive에 보관하고, status에는 redacted five-channel topology만 표시한다.
초기 cutover 또는 후속 `automation-release-activate --config`는 staged activation으로 현재 release digest, `AppConfig.digest`, canonical `news_policy_v1` payload를 한 immutable pair로 묶는다. latest valid five-channel release/config binding이 current authority이며, 성공 후 canonical replay는 동일 pair의 무변경 replay만 허용한다. planned drain/runtime gate는 disabled timers, inactive services, collect/Telegram/Sheets 세 locks, open authority 부재와 Codex clean containment를 재검증한다. `automation-status`, `systemctl status`, timer/unit failure, stale lease, queued/outbox count와 ambiguity를 모니터링한다. urgent stop에서 remote effect가 불명확하면 해당 timer를 중지하고 inspect/probe/immutable resolution 전 재실행하지 않는다.
Codex containment는 binding drift에서 root만 start/cleanup할 수 있으며 Newsbot job/provider authority는 항상 0이다. Codex 복원은 별도 gate다. timer를 중지하고 cgroup-empty/inactive, durable clean receipt, immutable authority, manifest/migration, effective profile과 provider-free/codex canary를 확인한 뒤 one-job live activation을 거쳐 `newsbot-generate-codex.timer`를 enable한다. rollback은 workers를 disabled 상태로 유지한 채 먼저 이전 verified six-capable runtime으로 switch/re-attest하고, 그 runtime에서 root-only archive의 exact six-channel config를 atomic install/attest한 다음 새 release/config activation을 append/replay하는 방식만 허용한다. migration, immutable audit/receipt, cursor/outbox, noon window, Sheets/Telegram remote history는 delete/update/restore하지 않는다.
