from __future__ import annotations

import json

from newsbot.approval.telegram import TELEGRAM_TEXT_LIMIT, _utf16_units, split_telegram_titles
from newsbot.candidates import CandidateApprovalService
from newsbot.storage import Storage


def test_noon_titles_are_whole_title_utf16_chunks_without_markup() -> None:
    first = "첫 번째 제목"
    second = "😀" * (TELEGRAM_TEXT_LIMIT // 2)
    chunks = split_telegram_titles((first, second))

    assert chunks == (first, second)
    assert all(_utf16_units(chunk) <= TELEGRAM_TEXT_LIMIT for chunk in chunks)
    assert all("제목:" not in chunk and "출처:" not in chunk for chunk in chunks)


def test_noon_title_packing_preserves_newline_boundaries() -> None:
    titles = ("A" * 2049, "B" * 2047, "C")
    chunks = split_telegram_titles(titles)

    assert chunks == (titles[0], titles[1] + "\n" + titles[2])
    assert all(_utf16_units(chunk) <= TELEGRAM_TEXT_LIMIT for chunk in chunks)


def test_noon_title_snapshot_collapses_embedded_line_separators(tmp_path) -> None:
    with Storage.open(tmp_path / "title.sqlite") as storage, storage.transaction() as connection:
        connection.execute(
            "INSERT INTO source_posts(channel_id,external_post_id) VALUES('testingcatalog','1')"
        )
        post_id = int(connection.execute("SELECT last_insert_rowid()").fetchone()[0])
        connection.execute(
            "INSERT INTO source_post_versions(source_post_id,version_key,body,urls_json) "
            "VALUES(?,'v1','fallback',?)",
            (
                post_id,
                json.dumps(
                    [{"url": "https://example.test", "title": "첫 줄\r\n둘째 줄\u2028셋째 줄"}],
                    ensure_ascii=False,
                ),
            ),
        )
        version_id = int(connection.execute("SELECT last_insert_rowid()").fetchone()[0])
        title, _ = CandidateApprovalService._candidate_display(connection, version_id)

    assert title == "첫 줄 둘째 줄 셋째 줄"
    assert split_telegram_titles((title, "다음 제목")) == ("첫 줄 둘째 줄 셋째 줄\n다음 제목",)
