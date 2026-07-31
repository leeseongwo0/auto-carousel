# 제품 및 운영 요구사항

## 범위와 소스

로컬 우선 Python 3.12 모듈형 모놀리스는 SQLite를 유일한 내구성 권위로 사용한다. 고정 소스는 `testingcatalog`, `ai_masters_community`, `aipost`, `coinnesskr`, `exilist_official`, `dolbikong` 여섯 개다. 채널 품질·분류(`official`, `original_publisher`, `aggregator`, `community`)와 도메인은 `config/channels.toml`에 명시한다.

fixture 수집, fixture `reconcile`, `rank`, scripted 승인은 credential-free다. `auth-telethon`만 interactive MTProto authorization을 수행하며, live collection은 Telethon/MTProto, 승인 알림은 Telegram Bot API, generation은 선택된 OpenAI-compatible API를 명시적으로 사용한다. Google Sheets 명령은 `sheets` extra와 service-account JSON을 명시적으로 요구한다. 선택하지 않은 capability는 해당 패키지를 import하거나 secret를 읽거나 네트워크를 열지 않는다.

## 후보, 생성, 검토

수집과 후보 평가는 provider를 만들거나 호출하지 않는다. `[제작]` 선택은 candidate, digest, 정렬된 `source_post_version_id` 집합에 결합되어 initial generation job 하나를 큐에 넣는다. 중복 callback은 새 job을 만들지 않는다.

생성은 선택된 job만 lease한다. 실패한 provider 호출은 `failed_recoverable` job으로 남고 재시도할 수 있다. provider 실패는 draft, fallback, approval, handoff를 만들지 않는다. provider 결과에는 원고 내용을 기준으로 한 정확한 `AI|Blockchain` category가 필요하며 missing/invalid 값은 generation insertion 전에 실패한다. material source edit는 현재 job, generation, callback을 supersede하고 새 선택을 요구한다.

정확한 current draft만 별도 review에서 승인, 재생성, 페이지 `+/-`, 6/24/72시간 연기 또는 거절한다. review 최종 승인은 category·provenance·canonical 원고와 고정 target binding을 포함한 하나의 immutable Sheets handoff를 원자적으로 만든다. 승인 transaction은 원격 호출을 하지 않는다.
`poll-approvals`는 due된 defer를 resume하여 원래 상태가 selection이면 candidate digest, review이면 정확한 current draft와 bound source-version callback을 다시 보낸다.


## 카드, 캡션, 출처

`page_count`는 표지를 포함한 총 페이지 수이며 선택된 source body에서 결정론적으로 1–8을 고른다. 1페이지는 표지 전용이며, 2–8페이지는 표지와 `page_count - 1`개 본문으로 구성된다. 생성 요청의 페이지 수는 검증되며 범위를 벗어나거나 text limit을 넘는 초안은 거절된다.

Telegram 미리보기는 4096 UTF-16 code unit 이하로 분할한다. surrogate pair를 분리하지 않으며, 조각을 이어 붙이면 원문과 같다. Instagram 캡션은 승인 원고의 텍스트이며, Figma 편집과 Instagram 게시 자동화는 없다.

`source_posts`와 `source_post_versions`는 불변 material snapshot(text, URL, media)을 보관하고 edit timestamp는 `source_post_observations`의 관측 메타데이터로 보관한다. candidate source, generation source, selection/review callback은 정확한 source-version binding을 보존한다. claim ID 검증은 allowlisted source reference의 무결성만 검사하며 사실성이나 의미적 함의를 보장하지 않는다. engagement는 material과 분리된 `source_post_observations`에 observation timestamp와 함께 저장한다. text/media/url 등 material payload가 같고 edit timestamp 또는 engagement만 달라지면 새 version이 아니라 새 observation이다. `None`은 API가 관측값을 제공하지 않은 missing이고 `0`은 관측된 0이며 서로 바꾸지 않는다.


## 수집과 Sheets 전달

`collection_intervals`는 채널별 floor, fixed upper bound, page frontier, overlap frontier를 보관한다. `collect-live`는 interval을 완료한 뒤에만 `collection_cursors`를 전진시켜 cap/crash/timeout/FloodWait 뒤에도 계속 가능하다. `reconcile-live`는 bounded lookback을 별도로 스캔하고 normal cursor를 변경하지 않는다.
`reconcile-live`는 대상 `--channel` 하나와 양수 `--lookback-hours` 또는 `--from-id`/`--to-id` 하나를 사용한다. ID pair는 함께 지정하고 양수, `from <= to`이며 양 끝을 포함한다. range mode는 newest-ID discovery 없이 지정 ID로 page한다. `page_size`와 `max_pages`는 양수여야 하고 cap은 normal cursor를 전진시키지 않는다.


최종 승인된 새 handoff만 고정 `workplace` 탭(sheetId 0)에 전달할 수 있다. A1:V3 header oracle과 exact controls를 통과해야 하며, A는 빈 문자열, B는 승인일, C는 `1`–`8`, D는 immutable generated category, E는 `X`, F:V는 caption과 page copy다. 사람은 Instagram 업로드 뒤 E를 `O`로 바꾼다. 정확한 document metadata와 append는 한 atomic batch이고 기존 행은 bot이 수정하지 않는다.

SQLite binding mutex, random owner token과 monotonically increasing fence가 bootstrap과 delivery를 직렬화한다. mutation 가능성을 기록한 뒤 exact metadata가 없거나 조회가 실패해도 자동 재전송하지 않는다. trusted atomic rejection만 settled-not-applied가 될 수 있고 나머지는 probe-only ambiguity다. Legacy `export_outbox`와 파일은 backfill/materialize/report하지 않는다.
## 평가 정책

각 story의 source quality, freshness, engagement, topic, official evidence는 source별 값을 **최대**로 집계하고 certainty는 **최소**로 집계한다. 동점은 source key의 결정론적 순서로 푼다. freshness는 `max(0, 1 - age_hours / freshness_horizon_hours)`이다. engagement가 모두 missing이면 score `0.25`와 missing flag를 기록한다. 하나라도 관측되면 각 관측 metric의 contribution은 `weight * min(1, ln(1 + value) / ln(1 + saturation))`이고 missing metric은 합산하지 않는다. 기본 weights는 views/reactions/forwards `0.60/0.25/0.15`, saturation은 `100000/5000/1000`이다.

기본 총점은 source quality `0.15`, freshness `0.15`, engagement `0.10`, topic relevance `0.25`, novelty `0.15`, official evidence `0.15`, certainty `0.05`의 가중합이며 banker’s rounding으로 소수 6자리 기록한다. topic과 total은 각각 설정된 minimum 이상이어야 eligible이다. 기본 page count는 source body에서 결정론적으로 1–8이고, generation의 page count도 반드시 1–8이다.

## 범위 밖

렌더링, 이미지/템플릿 제작, Figma 자동화, Instagram 로그인/API/예약/게시, 자동 승인, CI/CD, 배포, Docker/systemd, VPS 운영, push, release는 범위 밖이다.

## 결정론적 평가와 감사 oracle

hard filter는 이 순서로 기록한다: `unknown_channel`, `service_message`, `empty_record`, `explicit_ad`, `referral_only`, `low_value`, `published_window`. disclosure marker는 첫 두 non-empty line의 시작에서 lexical boundary를 지켜 일치해야 한다. 통과 source만 story URL/material identity로 묶는다. referral query 원문 key/value, URL 선택 source, material/observation identity, marker span, referral/coupon/CTA/URL/noise를 제거한 residual material text, conflict warning을 rationale에 보존한다.

evidence는 official/classification 또는 official domain `1`, original publisher/domain `0.8`, URL 있는 aggregator `0.5`, 기타 URL `0.3`, URL 없음 `0`이다. certainty category는 penalty 값이 아니라 명시적 alias set으로 정한다: `rumor` (`rumor`, `alleged`, `루머`, `설`)는 `0.30`, `anonymous` (`anonymous`, `unattributed`, `익명`)는 `0.20`이며 alias가 여럿 일치해도 category당 한 번만 벌점이다. conflict는 `0.50`, missing URL은 `0.20`이고 certainty는 `max(0, 1 - min(1, conflict + category penalties + missing_url))`다.

`candidate_policy_v1` scalar bounds는 고정값이다: initial lookback 24시간, candidate 최대 age 72시간(정확히 72시간 통과), future tolerance 2시간, semantic 최소 80자, material sentence 최소 40자, freshness horizon 48시간, novelty window 7일, topic floor `0.20`, total floor `0.55`, page count 1–8, Telegram preview 4096 UTF-16 code units다. core weights, engagement weights, saturation, positive topic terms, certainty penalties/markers는 0보다 커야 하며 exclusion topic term만 0을 허용한다. weight sum은 정확히 1일 필요는 없지만 Decimal `0.000001` 이내여야 한다. freshness는 `max(0, 1 - age_hours / 48)`이고, engagement의 관측 metric은 `weight × min(1, ln(1 + value) / ln(1 + saturation))`의 합이다. 모두 missing이면 `0.25`; views/reactions/forwards weight는 `0.60/0.25/0.15`, saturation은 `100000/5000/1000`이다. total은 exact Decimal component inputs와 unquantized weighted contributions를 rationale에 남긴 뒤 `ROUND_HALF_EVEN`으로 6자리 quantize한다. rationale은 topic/exclusion weight와 span, engagement constants/contributions/missing flags, evidence reason, ordered filter evidence, deterministic winner/worst source를 포함한다.

`tests/fixtures/channel_messages.json`은 `2026-07-29T12:00:00+00:00` 고정 epoch의 executable production-pipeline oracle이다. named case `official_ai_launch`은 `0.931250`, `aggregator_rumor_missing_engagement`는 `0.612500`이다. 여섯 채널 fixture는 conflict, prompt-injection-shaped text, material edit, prior-selected novelty, raw `ref=NEWSBOT`, 모든 hard filter, 정확히/초과 72시간 경계를 포함한다. unit oracle은 source pipeline, literal score/rationale evidence, input shuffle 및 host-clock 독립성을 검증한다.

export identity의 stable input은 canonical semantic payload의 actionable `source_versions`, exact `newsbot-generation-claim-v1` claim manifest, `generation_identity`, `approval_decision_identity`, pages, caption, sorted warnings, `draft`, `source_reported`다. 각 factual reference는 generation에 저장된 claim manifest 항목 하나와 정확히 일치해야 하며, observation refresh 뒤에도 승인 초안이 실제로 인용한 capture/evidence binding을 재계산하지 않는다. 이 payload의 SHA-256 앞 32 lowercase hex에 `exp_`를 붙이며 JSON/Markdown은 같은 identity를 공유한다. callback은 token hash와 exact candidate/source-version/draft binding으로 저장한다. 성공 action은 sibling callback을 revoke하고 defer는 exact selection 또는 review stage를 due time까지 보존한 뒤 아직 current인 binding만 재발행한다.
