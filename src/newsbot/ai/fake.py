"""Credential-free deterministic Korean generation provider for fixtures and dry runs."""

from __future__ import annotations

import unicodedata
from dataclasses import replace

from newsbot.ai.base import FactClaim, GenerationRequest
from newsbot.copywriting import (
    BodyPage,
    Caption,
    Category,
    CopyDraft,
    CoverPage,
    FactReference,
    FactualUnit,
    validate_copy,
)

_FIXTURE_ALIASES: tuple[tuple[Category, str], ...] = (
    ("Blockchain", "blockchain"),
    ("Blockchain", "crypto"),
    ("Blockchain", "cryptocurrency"),
    ("Blockchain", "bitcoin"),
    ("Blockchain", "ethereum"),
    ("Blockchain", "web3"),
    ("Blockchain", "token"),
    ("Blockchain", "wallet"),
    ("Blockchain", "exchange"),
    ("Blockchain", "on-chain"),
    ("Blockchain", "블록체인"),
    ("Blockchain", "암호화폐"),
    ("Blockchain", "비트코인"),
    ("Blockchain", "이더리움"),
    ("Blockchain", "웹3"),
    ("Blockchain", "토큰"),
    ("Blockchain", "코인"),
    ("Blockchain", "지갑"),
    ("Blockchain", "거래소"),
    ("Blockchain", "온체인"),
    ("AI", "artificial intelligence"),
    ("AI", "machine learning"),
    ("AI", "llm"),
    ("AI", "foundation model"),
    ("AI", "language model"),
    ("AI", "multimodal"),
    ("AI", "ai"),
    ("AI", "agent"),
    ("AI", "인공지능"),
    ("AI", "머신러닝"),
    ("AI", "거대언어모델"),
    ("AI", "파운데이션 모델"),
    ("AI", "언어 모델"),
    ("AI", "멀티모달"),
    ("AI", "에이전트"),
)
_SORTED_FIXTURE_ALIASES = tuple(sorted(_FIXTURE_ALIASES, key=lambda item: len(item[1]), reverse=True))


def fixture_category(draft: CopyDraft) -> Category:
    """Classify a fixture manuscript with the frozen fixture-category-v1 lexicon."""

    fields = (
        draft.cover.title,
        draft.cover.subtitle,
        *(field for body in draft.bodies for field in (body.subtitle, body.body)),
        draft.caption.hook,
        draft.caption.context,
        draft.caption.details,
        draft.caption.implications,
        draft.caption.questions,
    )
    text = "\n".join(unicodedata.normalize("NFC", field).casefold() for field in fields)
    consumed: list[tuple[int, int]] = []
    scores = {"AI": 0, "Blockchain": 0}

    for category, alias in _SORTED_FIXTURE_ALIASES:
        start = text.find(alias)
        while start != -1:
            end = start + len(alias)
            before = text[start - 1] if start else ""
            after = text[end] if end < len(text) else ""
            if (
                not _is_word_character(before)
                and not _is_word_character(after)
                and not any(start < consumed_end and consumed_start < end for consumed_start, consumed_end in consumed)
            ):
                consumed.append((start, end))
                scores[category] += 1
            start = text.find(alias, start + 1)

    return "Blockchain" if scores["Blockchain"] > scores["AI"] else "AI"


def _is_word_character(value: str) -> bool:
    return value == "_" or value.isalnum()

class FakeGenerationProvider:
    """Produce stable Korean copy without network, clocks, or randomness."""

    async def generate(self, request: GenerationRequest) -> CopyDraft:
        if not request.facts:
            raise ValueError("fake generation requires at least one fact claim")

        def unit_for(fact: FactClaim) -> FactualUnit:
            return FactualUnit(
                text="선택된 출처에 기록된 사실입니다.",
                references=(FactReference(fact.id, fact.source_version_id),),
            )

        first = request.facts[0]
        cover = CoverPage(
            title="오늘의 뉴스 브리핑",
            subtitle="선정된 출처를 바탕으로 정리했습니다",
            factual_units=(unit_for(first),),
        )
        bodies = tuple(
            BodyPage(
                subtitle=f"핵심 내용 {index}",
                body="선택된 출처의 사실을 검토했습니다.",
                factual_units=(unit_for(fact),),
            )
            for index, fact in enumerate(
                (request.facts[index % len(request.facts)] for index in range(request.page_count - 1)),
                start=1,
            )
        )
        draft = CopyDraft(
            cover=cover,
            bodies=bodies,
            caption=Caption(
                hook="오늘 확인할 뉴스입니다.",
                context="아래 내용은 선택된 원문 출처를 기준으로 작성한 초안입니다.",
                details="핵심 사실과 출처를 함께 검토해 주세요.",
                implications="변화의 영향은 추가 확인이 필요할 수 있습니다.",
                questions="여러분은 이 소식을 어떻게 보시나요?",
                hashtags=("#뉴스", "#브리핑"),
            ),
            category="AI",
        )
        draft = replace(draft, category=fixture_category(draft))
        return validate_copy(
            draft,
            allowed_claim_sources={fact.id: fact.source_version_id for fact in request.facts},
            expected_page_count=request.page_count,
        )
