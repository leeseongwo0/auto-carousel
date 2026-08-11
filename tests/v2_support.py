"""Test-only V2 workflow setup helpers; product code must not import this module."""

from __future__ import annotations

from newsbot.collectors.base import SourceObservation
from newsbot.v2_article import (
    ArticleResult,
    ArticleSnapshot,
    CanonicalSource,
    UrlReference,
    body_identity,
    material_character_count,
    select_article_urls,
)
from newsbot.v2_policy import V2PolicyInput, evaluate_v2_content
from newsbot.v2_workflow import V2Candidate, V2Workflow


def create_candidate(workflow: V2Workflow, observation: SourceObservation) -> V2Candidate | None:
    """Create test data through the immutable revision and enrichment path."""
    revision = workflow.record_revision(observation)
    existing = next(
        (
            candidate
            for candidate in workflow.list_candidates()
            if candidate.channel_id == observation.channel_id
            and candidate.external_post_id == observation.external_post_id
        ),
        None,
    )
    if existing is not None:
        return existing

    lease = workflow.claim_enrichment("tests.v2_support.create_candidate", revision.id)
    if lease is None:
        return next(
            (
                candidate
                for candidate in workflow.list_candidates()
                if candidate.channel_id == observation.channel_id
                and candidate.external_post_id == observation.external_post_id
            ),
            None,
        )

    selected = select_article_urls(
        (UrlReference(item.url, item.source, item.occurrence) for item in observation.urls),
        limit=8,
    )
    if not selected:
        return workflow.finalize_enrichment(
            lease,
            ArticleSnapshot(ArticleResult.PERMANENT_FAILURE, ""),
            evaluate_v2_content(
                V2PolicyInput(observation.text, observation.published_at, None),
                now=observation.published_at,
            ),
        )

    selected_url = selected[0]
    body = f"{observation.text or 'Test source evidence.'}\nSource observation {observation.external_post_id}."
    while material_character_count(body) < 200:
        body = f"{body}\n{observation.text or 'Test source evidence.'}"
    snapshot = ArticleSnapshot(
        ArticleResult.SUCCESS,
        selected_url.requested_url,
        final_url=selected_url.canonical_url,
        canonical_url=selected_url.canonical_url,
        canonical_source=CanonicalSource.REQUESTED,
        body=body,
        body_hash=body_identity(body),
        material_count=material_character_count(body),
    )
    policy = evaluate_v2_content(
        V2PolicyInput(
            telegram_text=observation.text,
            telegram_date=observation.published_at,
            display_url=selected_url.canonical_url,
            preview_title=observation.preview_title,
            preview_description=observation.preview_description,
            article_body=body,
            sponsored=observation.sponsored,
        ),
        now=observation.published_at,
    )
    return workflow.finalize_enrichment(lease, snapshot, policy)
