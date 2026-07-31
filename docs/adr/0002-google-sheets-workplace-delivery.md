# ADR-0002: Google Sheets `workplace` 전달

- 상태: 승인됨
- 결정일: 2026-07-30

## 결정

최종 승인 transaction은 원격 호출 대신 SQLite에 하나의 immutable handoff를 만든다. 별도 worker가 사전 공유된 고정 스프레드시트의 `workplace`(sheetId 0)에 A:V 22개 문자열을 append한다. Bootstrap과 delivery는 spreadsheet binding 하나의 SQLite mutex, retained lease, random owner token과 monotonically increasing fence를 공유한다.

원격 batch는 document-scoped deterministic metadata 생성과 한 개의 `AppendCellsRequest`를 원자적으로 포함한다. 정확한 metadata만 적용 증거다. Mutation 가능성을 SQLite에 먼저 기록한 뒤에는 trusted atomic rejection을 제외하고 자동 재전송하지 않는다.

## 동인

- timeout/reset 뒤 중복 행 방지가 unattended liveness보다 중요하다.
- 사람에게 보이는 A:V schema와 사람의 E:V 편집권을 유지해야 한다.
- SQLite가 승인 provenance와 전달 상태의 유일한 권위여야 한다.
- Fixture와 기본 명령은 Google dependency, credential, network, 결과 파일 없이 동작해야 한다.

## 고려한 대안

- 승인 transaction에서 직접 Sheets 호출: SQLite와 원격 효과를 원자적으로 결합할 수 없어 거절.
- visible/hidden identity column 또는 helper tab: 사용자 template를 변경하므로 거절.
- timeout 또는 negative probe 뒤 자동 append 재시도: 중복 가능성이 있어 거절.
- JSON/Markdown local materializer 유지: 두 전달 권위를 만들므로 거절.
- provider channel 기반 category 또는 사람 category 버튼: 원고 내용 기반 자동 분류 요구와 충돌하여 거절.

## 결과

- 새 행은 A blank, E `X`; Instagram 업로드 후 사람이 E를 `O`로 변경한다.
- 정확한 metadata reuse는 visible cell을 절대 수정하지 않는다.
- ambiguous operation은 장기간 binding을 막을 수 있다. 이는 의도한 nonduplication tradeoff이며 probe 또는 운영자 판단으로만 종료한다.
- Google client는 optional `sheets` extra이며 Sheets scope, frozen token, no redirect/retry mutation transport를 사용한다.
- Legacy outbox/files는 backfill하거나 Sheet-delivered로 보고하지 않는다.

## 후속 운영

Release 전 disposable spreadsheet에서 bootstrap controls와 append placement canary를 실행한다. Cutover는 구 materializer를 중지하고 DB backup/migration/oracle 검증 뒤 새 worker만 활성화한다. Rollback은 효과를 중지하고 fix-forward하며 원격 행·metadata·controls와 SQLite audit를 삭제하지 않는다.
