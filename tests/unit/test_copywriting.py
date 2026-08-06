from __future__ import annotations

import unicodedata
from dataclasses import FrozenInstanceError, replace

import pytest

from newsbot.ai.fake import fixture_category
from newsbot.ai.openai_compatible import _draft_from_mapping
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
        category="AI",
    )


def test_cover_only_draft_and_standalone_caption_are_valid() -> None:
    draft = _draft()

    validated = validate_copy(draft, allowed_claim_sources=CLAIMS, expected_page_count=1)

    assert validated.page_count == 1
    assert (
        validated.caption.text
        == "핵심 소식\n\n공식 발표 내용입니다.\n\n지원 기준을 확인하세요.\n\n산업 현장 적용이 확대됩니다.\n\n어떤 변화가 예상되나요?\n\n#AI #뉴스"
    )


def test_caption_hashtag_limit_accepts_five_and_rejects_six() -> None:
    draft = _draft()
    five = replace(draft, caption=replace(draft.caption, hashtags=("#하나", "#둘", "#셋", "#넷", "#다섯")))
    validate_copy(five, allowed_claim_sources=CLAIMS)

    six = replace(five, caption=replace(five.caption, hashtags=(*five.caption.hashtags, "#여섯")))
    with pytest.raises(CopyValidationError, match="at most 5"):
        validate_copy(six, allowed_claim_sources=CLAIMS)


def test_provider_rejects_more_than_five_hashtags() -> None:
    payload = _provider_payload()
    payload["caption"]["hashtags"] = ["#하나", "#둘", "#셋", "#넷", "#다섯", "#여섯"]  # type: ignore[index]

    with pytest.raises(ValueError, match="at most 5"):
        _draft_from_mapping(payload)


def test_category_is_required_and_immutable() -> None:
    draft = _draft()
    with pytest.raises(TypeError, match="category"):
        CopyDraft(draft.cover, draft.bodies, draft.caption)
    with pytest.raises(FrozenInstanceError):
        draft.category = "Blockchain"  # type: ignore[misc]


def test_invalid_category_fails_copy_validation() -> None:
    draft = _draft()
    with pytest.raises(CopyValidationError, match="category"):
        validate_copy(replace(draft, category="Other"), allowed_claim_sources=CLAIMS)


def _provider_payload(category: object = "AI") -> dict[str, object]:
    return {
        "draft": True,
        "source_reported": True,
        "category": category,
        "cover": {
            "title": "제목",
            "subtitle": "",
            "factual_units": [{"text": "근거", "references": [{"claim_id": "announcement", "source_version_id": 17}]}],
        },
        "bodies": [],
        "caption": {
            "hook": "요약",
            "context": "맥락",
            "details": "세부",
            "implications": "영향",
            "questions": "질문",
            "hashtags": ["#뉴스"],
        },
    }


def test_provider_missing_or_invalid_category_fails_closed() -> None:
    missing = _provider_payload()
    del missing["category"]
    with pytest.raises(ValueError, match="missing fields"):
        _draft_from_mapping(missing)
    with pytest.raises(ValueError, match="category"):
        _draft_from_mapping(_provider_payload("Other"))
    with pytest.raises(ValueError, match="category"):
        _draft_from_mapping(_provider_payload("ai"))


def _fixture_draft(*, prose: str = "", hashtags: tuple[str, ...] = ("#뉴스",)) -> CopyDraft:
    draft = _draft()
    return replace(
        draft,
        cover=CoverPage(prose, factual_units=draft.cover.factual_units),
        bodies=(),
        caption=Caption("", "", "", "", "", hashtags),
    )


@pytest.mark.parametrize(
    ("prose", "expected"),
    [
        ("AI machine learning agent", "AI"),
        ("bitcoin wallet exchange", "Blockchain"),
        ("AI blockchain", "AI"),
        ("블록체인 이더리움 인공지능", "Blockchain"),
        ("FOUNDATION MODEL blockchain", "AI"),
    ],
)
def test_fixture_category_v1_lexical_scores(prose: str, expected: str) -> None:
    assert fixture_category(_fixture_draft(prose=prose)) == expected


def test_fixture_category_v1_normalizes_korean_and_excludes_hashtags() -> None:
    nfd_blockchain = unicodedata.normalize("NFD", "블록체인")
    assert fixture_category(_fixture_draft(prose=nfd_blockchain)) == "Blockchain"
    assert fixture_category(_fixture_draft(hashtags=("#blockchain", "#bitcoin"))) == "AI"


def test_fixture_category_v1_uses_boundaries_and_consumes_longer_aliases() -> None:
    assert fixture_category(_fixture_draft(prose="chair cryptocurrency crypto blockchain")) == "Blockchain"


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
