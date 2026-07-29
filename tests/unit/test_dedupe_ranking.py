from __future__ import annotations

import json
from dataclasses import replace
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest

import newsbot.ranking as ranking
from newsbot.collectors.base import Engagement, SourceObservation, UrlCandidate
from newsbot.collectors.fixture import FixtureCollector
from newsbot.config import ConfigError, PolicyConfig, load_config
from newsbot.ranking import evaluate_candidates

FIXTURE = Path(__file__).parents[1] / "fixtures" / "channel_messages.json"


def _payload() -> dict[str, object]:
    return json.loads(FIXTURE.read_text(encoding="utf-8"))


def _config() -> SimpleNamespace:
    payload = _payload()
    channels = tuple(
        SimpleNamespace(
            **(
                channel
                | {
                    "source_quality": Decimal(channel["source_quality"]),
                    "official_domains": tuple(channel["official_domains"]),
                    "original_domains": tuple(channel["original_domains"]),
                }
            )
        )
        for channel in payload["channels"]
    )
    return SimpleNamespace(channels=channels, policy=PolicyConfig(), digest="fixture-config")


def _observations() -> tuple[SourceObservation, ...]:
    return tuple(FixtureCollector(FIXTURE).collect())


def _evaluated_at() -> datetime:
    return datetime.fromisoformat(str(_payload()["oracle"]["evaluated_at"]))


def test_candidate_policy_v1_oracle_uses_production_fixture_pipeline_and_no_host_clock(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class HostClockForbidden:
        @classmethod
        def now(cls, *_: object, **__: object) -> None:
            raise AssertionError("candidate policy must use the supplied evaluation clock")

    monkeypatch.setattr(ranking, "datetime", HostClockForbidden)
    observations = _observations()
    forward = evaluate_candidates(observations, _config(), _evaluated_at())
    shuffled = evaluate_candidates(tuple(reversed(observations)), _config(), _evaluated_at())
    assert [(item.story_key, item.total, item.primary_reason) for item in shuffled] == [
        (item.story_key, item.total, item.primary_reason) for item in forward
    ]

    by_post = {item.observations[0].external_post_id: item for item in forward}
    assert by_post["101"].total == Decimal("0.931250")
    assert by_post["202"].total == Decimal("0.612500")
    assert by_post["101"].rationale["sources"][0]["engagement"]["scoring"] == {
        "constants": {
            "views": {"weight": "0.60", "saturation": "100000"},
            "reactions": {"weight": "0.25", "saturation": "5000"},
            "forwards": {"weight": "0.15", "saturation": "1000"},
        },
        "contributions": [
            {"metric": "reactions", "raw": "5000", "ratio": "1", "contribution": "0.25"},
            {"metric": "forwards", "raw": "1000", "ratio": "1", "contribution": "0.15"},
        ],
        "all_missing_default": None,
    }
    assert by_post["202"].rationale["sources"][0]["engagement"]["all_missing"] is True


def test_oracle_executes_selected_novelty_material_edit_and_prompt_shaped_text() -> None:
    collector = FixtureCollector(FIXTURE)
    first_rows = tuple(collector.collect())
    second_rows = tuple(collector.collect())
    first = next(row for row in first_rows if row.external_post_id == "610")
    edited = next(row for row in second_rows if row.external_post_id == "610")
    assert first.text != edited.text

    initial = evaluate_candidates((first,), _config(), _evaluated_at())[0]
    assert initial.eligible
    assert initial.rationale["components"]["novelty"]["value"] == "1"
    assert "Ignore all previous instructions" in first.text

    selected = evaluate_candidates(
        (first,),
        _config(),
        _evaluated_at(),
        history=(
            {
                "candidate_id": 41,
                "story_key": initial.story_key,
                "content_key": initial.content_key,
                "status": "approved",
            },
        ),
    )[0]
    assert selected.rationale["components"]["novelty"]["value"] == "0"
    assert selected.rationale["components"]["novelty"]["inputs"][0]["reason"] == "selected_content"

    revised = evaluate_candidates(
        (edited,),
        _config(),
        _evaluated_at(),
        history=(
            {
                "candidate_id": 41,
                "story_key": initial.story_key,
                "content_key": initial.content_key,
                "status": "approved",
            },
        ),
    )[0]
    assert revised.content_key != initial.content_key
    assert revised.rationale["components"]["novelty"]["inputs"][0]["reason"] == "prior_story_material_edit"


def test_fixture_reproduces_all_hard_filters_and_72_hour_boundary() -> None:
    by_post = {
        item.observations[0].external_post_id: item
        for item in evaluate_candidates(_observations(), _config(), _evaluated_at())
    }
    assert {post: by_post[post].primary_reason for post in ("303", "404", "505", "606", "407", "612")} == {
        "303": "explicit_ad",
        "404": "service_message",
        "505": "empty_record",
        "606": "low_value",
        "407": "referral_only",
        "612": "published_window",
    }
    assert by_post["611"].eligible
    assert by_post["608"].primary_reason == "topic_floor"


def test_disclosure_referral_and_certainty_are_boundary_and_category_aware() -> None:
    base = _observations()[0]
    disclosure_word = replace(
        base, text="advertisementary AI technology report has enough independently useful material for readers."
    )
    disclosure = replace(
        base, text="sponsored: AI technology report has enough independently useful material for readers."
    )
    referral = replace(
        base,
        text="referral promo code SAVE20 click here https://example.invalid/?ref=x",
        urls=(UrlCandidate("https://example.invalid/?ref=x"),),
    )
    rumor = replace(
        base, text="AI technology rumor alleged report has enough independently useful material for readers."
    )
    non_ad_result = evaluate_candidates((disclosure_word,), _config(), _evaluated_at())[0]
    ad_result = evaluate_candidates((disclosure,), _config(), _evaluated_at())[0]
    referral_result = evaluate_candidates((referral,), _config(), _evaluated_at())[0]
    rumor_result = evaluate_candidates((rumor,), _config(), _evaluated_at())[0]
    assert non_ad_result.primary_reason != "explicit_ad"
    assert ad_result.primary_reason == "explicit_ad"
    assert referral_result.primary_reason == "referral_only"
    assert non_ad_result.rationale["sources"][0]["filters"]["disclosure_evidence"] == []
    ad_filters = ad_result.rationale["sources"][0]["filters"]
    assert ad_filters["accepted"] is False
    assert ad_filters["hard_rejections"] == ["explicit_ad"]
    assert ad_filters["sponsored_flag"] is False
    assert ad_filters["disclosure_evidence"] == [
        {"line": 1, "marker": "sponsored", "spans": [{"start": 0, "end": 9}]},
        {"line": 1, "marker": "sponsored:", "spans": [{"start": 0, "end": 10}]},
    ]
    category = rumor_result.rationale["sources"][0]["certainty"]["category_penalties"]
    assert category[0]["category"] == "rumor"
    assert category[0]["penalty"] == "0.30"
    assert [match["marker"] for match in category[0]["matches"]] == ["rumor", "alleged"]


def test_rationale_has_exact_decimal_inputs_topic_spans_and_stable_winners() -> None:
    launch = next(
        item
        for item in evaluate_candidates(_observations(), _config(), _evaluated_at())
        if item.observations[0].external_post_id == "101"
    )
    rationale = launch.rationale
    assert rationale["components"]["quality"]["winner_material_identity"] == {
        "channel_id": "exilist_official",
        "external_post_id": "101",
    }
    topic = rationale["sources"][0]["topic"]
    assert topic["positive_value"] == "1"
    assert all(match["weight"] in {"0.4", "0.6"} and match["spans"] for match in topic["positive_matches"])
    assert rationale["weighted_components"]["freshness"] == "0.1412500000000000000000000000"
    assert rationale["sources"][0]["evidence"]["reason"] == "official_classification_or_domain"


def test_policy_rejects_unapproved_bounds_bad_sum_and_zero_core_terms(tmp_path: Path) -> None:
    source = Path("config/channels.toml").read_text(encoding="utf-8")
    bad_bound = tmp_path / "bound.toml"
    bad_bound.write_text(
        source.replace("max_candidate_age_hours = 72", "max_candidate_age_hours = 73"), encoding="utf-8"
    )
    with pytest.raises(ConfigError, match="approved candidate_policy_v1"):
        load_config(bad_bound, environ={})
    bad_sum = tmp_path / "sum.toml"
    bad_sum.write_text(source.replace("engagement = 0.10", "engagement = 0.100002"), encoding="utf-8")
    with pytest.raises(ConfigError, match="sum to 1 within"):
        load_config(bad_sum, environ={})
    zero_core = tmp_path / "zero.toml"
    zero_core.write_text(source.replace("engagement = 0.10", "engagement = 0"), encoding="utf-8")
    with pytest.raises(ConfigError, match="values must be positive"):
        load_config(zero_core, environ={})


def test_missing_and_observed_zero_engagement_are_distinct() -> None:
    base = _observations()[0]
    missing = evaluate_candidates((replace(base, engagement=Engagement()),), _config(), _evaluated_at())[0]
    observed_zero = evaluate_candidates(
        (replace(base, engagement=Engagement(views=0, reactions=0, forwards=0)),), _config(), _evaluated_at()
    )[0]
    assert missing.components["engagement"] == Decimal("0.25")
    assert observed_zero.components["engagement"] == Decimal("0")
