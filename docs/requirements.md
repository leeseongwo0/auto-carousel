# 제품 및 운영 요구사항

## 범위와 자동 흐름

로컬 우선 Python 3.12 모듈형 모놀리스은 SQLite를 유일한 내구성 권위로 사용한다. 고정 소스는 `testingcatalog`, `ai_masters_community`, `aipost`, `coinnesskr`, `exilist_official`, `dolbikong` 여섯 개이며 설정은 `config/channels.toml`이 권위다.

자동 운전은 `newsbot-collect`, `newsbot-telegram`, `newsbot-sheets` service/timer 쌍으로 한다. collect는 6개 채널을 매시간 수집하고 기존 ranking 뒤의 provider-free `news_policy_v1`으로 뉴스 적격성을 판정한다. Telegram은 실제 뉴스(`definite_news|trusted_analysis`)의 candidate/review outbox와 approval callback polling, 그리고 애매한 글의 제목 전용 정오 digest를 담당한다. Sheets는 최종 승인 handoff delivery를 수행한다. 기존 `newsbot-generate-codex.timer`는 변경되지 않은 별도 one-job Codex scheduler다.

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

runtime은 네 timer를 disable하고 네 service를 drain한 뒤 세 application flock, read-only DB authority와 Codex clean containment를 확인하고 backup한다. 같은 root-owned quiescence proof로 `deploy/build_newsbot_release.py`의 versioned build/attestation과 stable entrypoint switch/re-attestation을 완료한 뒤 새 runtime에서 migration/FK check와 migration 008 schema parity를 검증하고 exact release/config binding을 append한다. 초기 cutover 또는 후속 `automation-release-activate --config`는 현재 release digest, `AppConfig.digest`, canonical `news_policy_v1` payload를 한 immutable pair로 묶으며 동일 pair replay만 무변경으로 허용한다.
planned drain/runtime gate는 disabled timers, inactive services, collect/Telegram/Sheets 세 locks, open authority 부재와 Codex clean containment를 재검증한다. `automation-status`, `systemctl status`, timer/unit failure, stale lease, queued/outbox count와 ambiguity를 모니터링한다. urgent stop에서 remote effect가 불명확하면 해당 timer를 중지하고 inspect/probe/immutable resolution 전 재실행하지 않는다.

Codex 복원은 별도 gate다. timer를 중지하고 cgroup-empty/inactive, durable clean receipt, immutable authority, manifest/migration, effective profile과 provider-free/codex canary를 확인한 뒤 one-job live activation을 거쳐 `newsbot-generate-codex.timer`를 enable한다. rollback은 migration 008 호환성이 검증된 이전 runtime으로 forward switch하고 새 release/config activation을 append하는 방식만 허용한다. migration, immutable audit/receipt, cursor/outbox, noon window, Sheets/Telegram remote history는 delete/update/restore하지 않는다.
