# 운영 가이드

## 운영 모델

SQLite가 cursor, Telegram notification outbox/chunk attempt, approvals, generation, immutable Sheets handoff, remote operation/lease/probe와 audit의 권위다. 고정 여섯 채널은 `config/channels.toml`에만 정의한다. `newsbot-collect`, `newsbot-telegram`, `newsbot-sheets` service/timer는 모두 `newsbot` UID와 `/etc/newsbot/newsbot.env`, Telethon session, database, locks를 공유한다. 이는 편의와 coordination을 위한 공유 runtime이며 unit 사이 isolation/private mount security boundary가 아니다. Signed provenance and private cross-unit isolation are explicitly out of scope.

Codex는 예외다. 기존 `newsbot-generate-codex.service`/`.timer` 자체는 `newsbot` UID로 실행되며, 고정 sudo runner만 login-shell 없는 `newsbot-codex` UID의 owner-only `CODEX_HOME` 경계를 통과한다. root-attested no-argument runner와 durable immutable containment authority는 그대로 유지하고, non-Codex units와 Codex unit의 credential/containment/restore 절차를 섞지 않는다.

## 일상 상태와 monitoring

```bash
sudo systemctl status newsbot-collect.timer newsbot-telegram.timer newsbot-sheets.timer newsbot-generate-codex.timer
sudo systemctl status newsbot-collect.service newsbot-telegram.service newsbot-sheets.service newsbot-generate-codex.service
sudo -u newsbot /usr/local/bin/newsbot automation-status --db /var/lib/newsbot/newsbot.db
sudo -u newsbot /usr/local/bin/newsbot status --db /var/lib/newsbot/newsbot.db
```

Timer last/next activation, failed unit, automation aggregate counters, stale lease, pending/ambiguous/partial notification과 Sheets operation을 관찰한다. `newsbot-collect.timer`의 cadence는 service inactivity 뒤 1시간이다. 13:00 Asia/Seoul 이후에도 dispatch 가능한 것은 `[12:00:00,13:00:00)` post-write-lock sample로 이미 commit된 durable noon intent뿐이다. collecting window가 13:00 이후 남아 있거나 skipped window, config drift, migration/FK failure가 보이면 affected timer를 멈추고 조사한다. secret, Telegram message, Sheet payload, Google 오류 원문은 log/receipt에 복사하지 않는다.

특정 notification 상태는 내용 없이 확인한다.

```bash
sudo -u newsbot /usr/local/bin/newsbot automation-notification-inspect --db /var/lib/newsbot/newsbot.db --intent-id <NOTIFICATION_INTENT_ID>
```

`<NOTIFICATION_INTENT_ID>`는 `automation-status`나 durable 운영 기록에서 얻는 명시적 runtime placeholder다. ambiguous Telegram effect는 timer를 반복 실행해 해결하지 않는다. supported manual-required 상태만 검증된 결과에 맞게 다음 중 하나로 immutable하게 resolve한다.

```bash
sudo -u newsbot /usr/local/bin/newsbot automation-notification-resolve --db /var/lib/newsbot/newsbot.db --intent-id <NOTIFICATION_INTENT_ID> --expected-status manual_required --resolution delivered --actor-id <OPERATOR_ACTOR_ID> --reason-code transport_verified
sudo -u newsbot /usr/local/bin/newsbot automation-notification-resolve --db /var/lib/newsbot/newsbot.db --intent-id <NOTIFICATION_INTENT_ID> --expected-status manual_required --resolution abandoned --actor-id <OPERATOR_ACTOR_ID> --reason-code operator_abandoned
```

Sheets timeout, reset, 5xx, malformed response 또는 unavailable probe도 자동 delivery 재시도가 아니라 exact document metadata probe/운영자 판단 대상이다. existing row를 고치거나 동일 handoff를 직접 `sheets-deliver`로 재호출하지 않는다.

## release build, quiesced switch, migration, cutover

`<COMMIT_SHA>`, `<UV_SHA256>`, `<RELEASE_DIGEST>`, `<PROPOSAL_ID>`와 `<PREVIEW_PROPOSAL_SHA256>`는 각각 Git commit, approved uv artifact digest, attested runtime manifest digest, 운영자가 만든 proposal ID, preview receipt SHA-256이다.
새 호스트의 최초 cutover는 이 release window 전에 아래의 empty authority container와 canonical lock을 provision한다. 이는 schema migration이 아니며 `init-db`는 반드시 switch 뒤에 실행한다.
```bash
sudo install -d -o newsbot -g newsbot -m 0700 /var/lib/newsbot/locks
for lock in collect telegram sheets; do sudo install -o newsbot -g newsbot -m 0600 /dev/null "/var/lib/newsbot/locks/$lock.lock"; done
sudo -u newsbot sqlite3 /var/lib/newsbot/newsbot.db 'PRAGMA user_version;'
```


네 timer를 disable/stop하고 service drain, collect → Telegram → Sheets 순서의 세 canonical flock을 **계속 보유**한 채 final pre-swap check, `os.replace`, target attestation까지 수행한다. helper는 lock 파일이 모두 존재하고 `newsbot:newsbot`, regular, single-link, `0600`인지도 거부-우선으로 확인한다. Codex containment를 확인한 뒤 backup한다. 그 뒤에만 root-owned quiescence proof를 만들고 현 stable release의 non-migrating release helper로 candidate runtime을 build/attest하며, 같은 proof로 switch한다. helper는 build와 switch 전후에 disabled timers, inactive services, read-only application DB 상태, 세 flock, Codex clean containment를 직접 재검증한다. 따라서 switch 전에는 `init-db`, `automation-cutover-preview`, `automation-cutover-apply` 또는 다른 migration-capable 명령을 실행하지 않는다.
```bash
sudo systemctl disable --now newsbot-collect.timer newsbot-telegram.timer newsbot-sheets.timer newsbot-generate-codex.timer
for unit in newsbot-collect.service newsbot-telegram.service newsbot-sheets.service newsbot-generate-codex.service; do test "$(systemctl is-active "$unit" || true)" = inactive; done
sudo /usr/local/sbin/newsbot-codex-containment-v1 inspect
sudo install -d -o root -g root -m 0700 /var/backups/newsbot
sudo sqlite3 /var/lib/newsbot/newsbot.db '.backup /var/backups/newsbot/newsbot-before-<COMMIT_SHA>.db'
sudo chown root:root /var/backups/newsbot/newsbot-before-<COMMIT_SHA>.db
sudo chmod 0600 /var/backups/newsbot/newsbot-before-<COMMIT_SHA>.db
sudo sh -c 'umask 077; printf "quiescent\n" > <ROOT_OWNED_QUIESCENCE_PROOF_PATH>'
sudo /opt/newsbot/current/deploy/build_newsbot_release.py --uv-sha256 <UV_SHA256> build <COMMIT_SHA> --quiescence-proof <ROOT_OWNED_QUIESCENCE_PROOF_PATH>
sudo test -f /opt/newsbot/releases/<COMMIT_SHA>/runtime-manifest.json
export RELEASE_DIGEST="$(sha256sum /opt/newsbot/releases/<COMMIT_SHA>/runtime-manifest.json | cut -d' ' -f1)"
sudo /opt/newsbot/current/deploy/build_newsbot_release.py --uv-sha256 <UV_SHA256> switch <COMMIT_SHA> --quiescence-proof <ROOT_OWNED_QUIESCENCE_PROOF_PATH>
sudo rm -f <ROOT_OWNED_QUIESCENCE_PROOF_PATH>
```

첫 설치는 release window 전에 빈 SQLite authority file과 세 canonical lock file을 `newsbot:newsbot`, `0600`으로 provision한다. switch 뒤의 `init-db`가 최초 schema/cutover를 만든다. 이미 migration-008 cutover를 마친 호스트의 이후 release와 rollback은 아래와 같은 008-compatible forward switch만 사용하며 immutable SQLite history를 초기화하거나 backup으로 복원하지 않는다.

```bash
sudo -u newsbot /usr/local/bin/newsbot init-db --db /var/lib/newsbot/newsbot.db
sudo -u newsbot -H bash -c 'set -a; . /etc/newsbot/newsbot.env; set +a; exec /usr/local/bin/newsbot "$@"' _ automation-cutover-preview --config /etc/newsbot/config.toml --db /var/lib/newsbot/newsbot.db --proposal-id <PROPOSAL_ID> --release-digest <RELEASE_DIGEST>
sudo -u newsbot -H bash -c 'set -a; . /etc/newsbot/newsbot.env; set +a; exec /usr/local/bin/newsbot "$@"' _ automation-cutover-apply --config /etc/newsbot/config.toml --db /var/lib/newsbot/newsbot.db --proposal-id <PROPOSAL_ID> --proposal-sha256 <PREVIEW_PROPOSAL_SHA256> --release-digest <RELEASE_DIGEST>
```

후속 compatible release 또는 rollback은 initial cutover를 다시 실행하지 않는다. 모든 timer/service를 drain하고 동일 quiescence·switch gate를 통과한 뒤 새 runtime manifest digest와 exact config/policy binding을 append-only activation으로 기록한다. changed pair는 prior binding의 `collecting` noon window 또는 `pending|claimed|sending|ambiguous|partial_manual_required` noon outbox가 있으면 activation하지 않는다. historical `queued` window는 outbox가 `sent|canceled|resolved_delivered|resolved_abandoned`인 경우만 허용된다.
```bash
sudo -u newsbot /usr/local/bin/newsbot automation-release-activate \
  --config /etc/newsbot/config.toml \
  --db /var/lib/newsbot/newsbot.db \
  --release-digest <RELEASE_DIGEST>
```
명령은 release manifest, exact AppConfig digest, canonical policy JSON, active audience/frontier와 quiescence를 검증한 뒤 activation과 binding을 하나의 transaction으로 append한다. legacy activation에 binding을 retrofit하지 않는다. same release/same config exact replay만 write 없이 current IDs를 반환한다. same release/different config, different release/same config, different release/different config은 모두 새 activation/binding을 append해야 하며 validation/config/window failure는 insert 전에 fail closed한다. 전달값이 실제 stable runtime과 다르면 SQLite activation을 기록하지 않는다.
첫 설치의 DB/lock provision은 **release window 이전에만** 실행한다; subsequent migration-008 release에서는 실행하거나 existing DB를 바꾸지 않는다. preview receipt의 SHA-256을 그대로 apply에 사용한다. `NEWSBOT_CALLBACK_ACTOR_ID`는 `NEWSBOT_APPROVER_USER_IDS` 안의 단 한 명으로 `/etc/newsbot/newsbot.env`에 반드시 설정한다.

## Canaries와 timer 복원

non-Codex baseline/cutover apply 뒤 각 worker는 no-work canary로 한 번씩만 확인한다. systemd unit에 정의된 exact command/options를 직접 복사해 실행하지 말고 unit을 start한다.

```bash
for unit in newsbot-collect.service newsbot-collect.timer newsbot-telegram.service newsbot-telegram.timer newsbot-sheets.service newsbot-sheets.timer; do sudo cmp -s "/opt/newsbot/releases/<COMMIT_SHA>/src/deploy/systemd/$unit" "/etc/systemd/system/$unit"; done
sudo systemctl daemon-reload
sudo systemctl start newsbot-telegram.service
sudo systemctl start newsbot-sheets.service
sudo systemctl start newsbot-collect.service
sudo systemctl enable --now newsbot-telegram.timer
sudo systemctl enable --now newsbot-sheets.timer
sudo systemctl enable --now newsbot-collect.timer
```

enable 순서는 반드시 Telegram → Sheets → collection이다. Telegram canary는 Seoul noon admission을 `[12:00:00,13:00:00)` write-lock-after-sample interval로 확인한다. 정확히 13:00에 intent가 없으면 date는 `skipped`이고 catch-up/rollover하지 않는다. on-time committed intent는 13:00 뒤에도 durable outbox/chunk authority로 완료 또는 안전한 retry할 수 있지만, accepted chunk는 resend하지 않고 ambiguous/partial effect는 inspect 후 immutable resolution만 한다. 이후 candidate의 인간 `[제작]` 승인, one-job generation, 정확한 draft의 인간 `[시트 전달]` 승인, Sheets metadata/append와 no-resend behavior를 E2E로 확인한다.

Codex restoration은 **두 단계 UID boundary**다: device auth와 provider credential은 login-shell 없는 `newsbot-codex` UID의 owner-only `CODEX_HOME`에서만 수행하고, root-owned no-argument containment runner가 별도 immutable authority/receipt를 attest한 activation만 그 UID로 실행한다. Codex timer를 계속 중지한 상태에서 inactive/cgroup-empty, durable clean/reset receipt, immutable authority, migration/manifest, effective permission profile, provider-free canary 및 one-job Codex canary/live activation을 확인한다. 그 결과가 모두 통과했을 때만 다음을 실행한다.

```bash
sudo systemctl enable --now newsbot-generate-codex.timer
```

## 실패, urgent stop, rollback

planned drain은 위 순서대로 모든 timer를 disable/stop하고 service/application/Codex quiescence를 release helper로 증명한 뒤 backup/build/switch한다. urgent stop에서 Telegram/Sheets/Codex remote effect 또는 process 종료가 불명확하면 해당 timer를 즉시 멈추고 operation을 ambiguous/containment state를 dirty로 보존한다. 자동 restart, resend, DB restore/edit로 해소하지 않는다.
rollback은 migration-008-compatible이고 이전에 verified된 runtime release 또는 corrective runtime으로 forward switch하는 것뿐이다. drain, canonical collect→Telegram→Sheets locks, Codex containment, backup, migration/FK/parity checks와 release/config activation quiescence를 다시 거친다. 새 activation/config binding을 append하고 모든 migration, window, item, cursor/outbox, attempt, Sheets/Telegram history, approval/release audit, containment receipt를 preserve한다. DB restore/delete/update나 pre-008 binary는 rollback에 사용하지 않는다. 문제가 난 code/config는 corrective release, 새 cutover receipt와 immutable operator resolution으로 복구한다.
