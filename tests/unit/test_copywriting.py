from __future__ import annotations

import pytest

from newsbot.copywriting import (
    BodyPage,
    Caption,
    CopyDraft,
    CopyValidationError,
    CoverPage,
    FactReference,
    FactualUnit,
    validate_copy,
)

CLAIMS = {"announcement": 17}


def _caption() -> Caption:
    return Caption(
        "핵심 소식",
        "공식 발표 내용입니다.",
        "지원 기준을 확인하세요.",
        "산업 현장 적용이 확대됩니다.",
        "어떤 변화가 예상되나요?",
        ("#AI", "#뉴스"),
    )


def _draft(*bodies: BodyPage) -> CopyDraft:
    return CopyDraft(
        cover=CoverPage(
            "AI 반도체 실증 확대",
            factual_units=(FactualUnit("정부가 사업 확대를 발표했습니다.", (FactReference("announcement", 17),)),),
        ),
        bodies=bodies,
        caption=_caption(),
    )


def test_cover_only_draft_and_standalone_caption_are_valid() -> None:
    draft = _draft()

    validated = validate_copy(draft, allowed_claim_sources=CLAIMS, expected_page_count=1)

    assert validated.page_count == 1
    assert (
        validated.caption.text
        == "핵심 소식\n\n공식 발표 내용입니다.\n\n지원 기준을 확인하세요.\n\n산업 현장 적용이 확대됩니다.\n\n어떤 변화가 예상되나요?\n\n#AI #뉴스"
    )


def test_adaptive_page_counts_accept_one_and_eight_but_reject_zero_and_nine() -> None:
    body = BodyPage(
        "세부 내용",
        "공식 발표에 근거한 세부 내용입니다.",
        (FactualUnit("사업 확대가 발표되었습니다.", (FactReference("announcement", 17),)),),
    )
    validate_copy(_draft(*(body for _ in range(7))), allowed_claim_sources=CLAIMS, expected_page_count=8)


def test_body_limits_use_python_code_points_and_facts_require_matching_references() -> None:
    valid = BodyPage("가" * 35, "나" * 240, (FactualUnit("근거 문장", (FactReference("announcement", 17),)),))
    validate_copy(_draft(valid), allowed_claim_sources=CLAIMS)

    with pytest.raises(CopyValidationError, match="subtitle exceeds 35 Unicode code points"):
        validate_copy(_draft(BodyPage("가" * 36, "본문", ())), allowed_claim_sources=CLAIMS)
    with pytest.raises(CopyValidationError, match="body exceeds 240 Unicode code points"):
        validate_copy(_draft(BodyPage("소제목", "나" * 241, ())), allowed_claim_sources=CLAIMS)
    with pytest.raises(CopyValidationError, match="claim source does not match"):
        validate_copy(
            _draft(BodyPage("소제목", "본문", (FactualUnit("근거 문장", (FactReference("announcement", 18),)),))),
            allowed_claim_sources=CLAIMS,
        )
