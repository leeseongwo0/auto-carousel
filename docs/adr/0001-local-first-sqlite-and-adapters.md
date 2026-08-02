# ADR-0001: 로컬 우선 SQLite와 adapter 선택

- 상태: 승인됨
- 날짜: 2026-07-29

## 결정

로컬 우선 Python 3.12 async 모듈형 모놀리스를 사용하고 SQLite를 유일한 durable authority로 둔다. fixture는 credential-free이고, 공개 Telegram 채널 수집은 Telethon/MTProto, 사람의 후보/초안 검토는 Telegram Bot API, 생성은 fixture fake provider 또는 OpenAI-compatible provider를 사용한다.

권한 있는 `[제작]` 선택만 candidate, digest, 정렬된 source-version binding을 가진 generation job을 만든다. generation job은 SQLite lease와 `failed_recoverable` 상태로 중단 또는 provider failure 뒤 재시도된다. 정확한 current draft의 review 승인만 canonical export outbox를 원자적으로 만든다.

최초 출력은 AI가 선택 source body의 분량에 따라 간결한 카드뉴스 문체로 1–8페이지 중 가장 짧고 유용한 수를 선택한다. 사전 계산한 고정 페이지 수는 강제하지 않고, 명시적인 페이지 증감 수정만 정확한 페이지 수를 요구한다. Telegram preview는 UTF-16 4096 code unit 이하로 분할한다. outbox mismatch는 파일을 보존하고 `corrupt`로 남긴다. Figma 편집과 Instagram 게시도 수동 경계다.

## 배경과 동인

여섯 저용량 채널과 하나의 승인 큐에는 분산 서비스보다 SQLite transaction, 불변 provenance, idempotent callback/job, crash recovery가 단순하다. `collection_intervals`의 fixed bounds와 frontier, `collection_cursors`의 완료 후 승격은 durable continuation을 제공한다. bounded `reconcile-live`는 normal cursor와 분리되어 이전 기간을 확인한다.

## 고려한 대안

- **Bot API만으로 수집:** 공개 채널 이력 수집 adapter가 아니다.
- **선택 전 모든 후보 생성:** provider 비용과 credential/network 노출을 늘리고 선택과 검토를 혼동한다.
- **선택 callback에서 동기 생성:** callback retry와 provider failure가 선택을 막는다.
- **PostgreSQL/Redis/별도 service:** 현재 규모에서는 coordination만 늘린다.
- **고정 또는 사전 계산 페이지 수:** AI가 근거 분량에 따라 선택하는 1–8페이지 정책과 맞지 않는다.

## 결과와 범위 경계

SQLite는 source snapshot, candidate rationale, selection, generation, review, canonical export bytes/digest의 복구 권위다. materialization은 outbox 이후의 crash-safe 부수 효과이며 `ready` 쌍만 수동 handoff한다.

이 ADR은 자동 render, Figma automation, Instagram auth/API/publishing, VPS scheduler/operations, deployment를 승인하지 않는다.
