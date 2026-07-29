"""Credential-free deterministic Korean generation provider for fixtures and dry runs."""

from __future__ import annotations

from newsbot.ai.base import FactClaim, GenerationRequest
from newsbot.copywriting import (
    BodyPage,
    Caption,
    CopyDraft,
    CoverPage,
    FactReference,
    FactualUnit,
    validate_copy,
)


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
        )
        return validate_copy(
            draft,
            allowed_claim_sources={fact.id: fact.source_version_id for fact in request.facts},
            expected_page_count=request.page_count,
        )
