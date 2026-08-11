from __future__ import annotations

import json
import logging
from datetime import UTC, datetime

import pytest

from newsbot.collectors.base import SourceObservation, UrlCandidate
from newsbot.v2_observability import (
    CompactionResult,
    CompactionTable,
    EffectStage,
    EffectStatus,
    FetchResult,
    ImmediateAlert,
    InMemoryObservabilitySink,
    KeyKind,
    LanguagePath,
    LoggingObservabilitySink,
    MetricName,
    NoopObservabilitySink,
    Outcome,
    Queue,
    Reason,
    ThresholdAlert,
    ThresholdSnapshot,
    evaluate_thresholds,
    event,
)
from newsbot.v2_workflow import V2Workflow
from tests.v2_support import create_candidate


def test_metric_labels_have_only_declared_finite_values() -> None:
    cases = (
        (MetricName.POLICY_DECISION, {"outcome": Outcome.CANDIDATE, "reason": Reason.NEWS}),
        (MetricName.FETCH, {"result": FetchResult.SUCCESS}),
        (MetricName.KEY, {"kind": KeyKind.CANDIDATE}),
        (MetricName.EFFECT, {"stage": EffectStage.SHEETS_DELIVERY, "status": EffectStatus.CONFIRMED}),
        (MetricName.QUEUE, {"queue": Queue.ENRICHMENT}),
        (MetricName.LANGUAGE, {"path": LanguagePath.MIXED}),
        (MetricName.COMPACTION, {"table": CompactionTable.CALLBACKS, "result": CompactionResult.COMPACTED}),
        (MetricName.ALERT, {"alert": ImmediateAlert.DUPLICATE_CLAIM}),
    )

    for metric, labels in cases:
        serialized = event(metric, labels=labels).as_dict()
        assert serialized["labels"] == {name: value.value for name, value in labels.items()}

    with pytest.raises(ValueError, match="invalid labels"):
        event(MetricName.FETCH, labels={"domain": FetchResult.SUCCESS})
    with pytest.raises(TypeError, match="invalid value"):
        event(MetricName.FETCH, labels={"result": "anything"})  # type: ignore[arg-type]


def test_serialized_events_redact_hostile_values() -> None:
    secret = "https://user:token-123@127.0.0.1/private?telegram=message&password=secret"  # pragma: allowlist secret
    error = RuntimeError(f"authorization: Bearer top-secret; url={secret}")
    recorded = event(
        MetricName.FETCH,
        labels={"result": FetchResult.BLOCKED},
        entity="telegram text: confidential " + secret,
        domain="10.0.0.5.internal",
        redirects=(secret, "http://[::1]/admin?api_key=secret"),
        error=error,
    )
    sink = InMemoryObservabilitySink()
    sink.emit(recorded)
    NoopObservabilitySink().emit(recorded)

    payload = json.dumps(sink.events[0].as_dict(), sort_keys=True)
    for forbidden in ("token-123", "telegram text", "top-secret", "127.0.0.1", "10.0.0.5", "::1", "api_key"):
        assert forbidden not in payload
    assert sink.events[0].redirect_count == 2
    assert len(str(sink.events[0].as_dict()["entity_fingerprint"])) == 16
    assert len(str(sink.events[0].as_dict()["redirect_digest"])) == 16


def test_logging_sink_serializes_only_redacted_event(caplog) -> None:
    sink = LoggingObservabilitySink()
    secret = "https://user:password@127.0.0.1/private?token=secret"  # pragma: allowlist secret
    with caplog.at_level(
        logging.INFO,
        logger="newsbot.v2.observability",
    ):
        sink.emit(
            event(
                MetricName.FETCH,
                labels={"result": FetchResult.BLOCKED},
                entity=secret,
                domain=secret,
                error=RuntimeError(secret),
            )
        )

    message = caplog.records[-1].getMessage()
    assert "password" not in message
    assert "127.0.0.1" not in message
    assert "token" not in message
    assert json.loads(message)["metric"] == "fetch"


def test_workflow_emits_policy_fetch_effect_queue_and_alert_events(
    tmp_path,
) -> None:
    sink = InMemoryObservabilitySink()
    with V2Workflow(
        tmp_path / "v2.sqlite",
        mode="create",
        observability=sink,
    ) as workflow:
        candidate = create_candidate(
            workflow,
            SourceObservation(
                channel_id="secret-channel",
                channel_handle="secret-handle",
                external_post_id="secret-post",
                published_at=datetime.now(UTC),
                text=(
                    "OpenAI announced a major enterprise security "
                    "integration available to customers with documented "
                    "deployment scope and product availability. "
                )
                * 5,
                urls=(UrlCandidate("https://example.test/private?token=secret"),),
            ),
        )
        assert candidate is not None
        workflow.record_remote_attempt(
            candidate.id,
            "candidate_notification",
        )
        workflow.settle_remote_effect(
            candidate.id,
            "candidate_notification",
            "confirmed",
            receipt_id="telegram-message",
        )
        aggregate = workflow.status_aggregate(
            seven_day_storage_baseline_bytes=1,
        )

    metrics = {item.metric for item in sink.events}
    assert {
        MetricName.POLICY_DECISION,
        MetricName.FETCH,
        MetricName.EFFECT,
        MetricName.QUEUE,
        MetricName.ALERT,
    }.issubset(metrics)
    assert "database_growth" in aggregate["alerts"]
    serialized = json.dumps(
        [item.as_dict() for item in sink.events],
        sort_keys=True,
    )
    for forbidden in (
        "secret-channel",
        "secret-handle",
        "secret-post",
        "user:secret",
        "telegram-message",
    ):
        assert forbidden not in serialized


def test_workflow_emits_immediate_safety_alerts(
    tmp_path,
) -> None:
    sink = InMemoryObservabilitySink()
    published = datetime.now(UTC)
    text = (
        "OpenAI announced a major enterprise security integration "
        "available to customers with documented deployment scope. "
    ) * 5
    with V2Workflow(
        tmp_path / "v2-alerts.sqlite",
        mode="create",
        observability=sink,
    ) as workflow:
        first = create_candidate(
            workflow,
            SourceObservation(
                channel_id="channel",
                channel_handle="publisher",
                external_post_id="one",
                published_at=published,
                text=text,
                urls=(UrlCandidate("https://example.test/story"),),
            ),
        )
        assert first is not None
        workflow.record_remote_attempt(
            first.id,
            "candidate_notification",
        )
        workflow.settle_remote_effect(
            first.id,
            "candidate_notification",
            "confirmed",
            receipt_id="message",
        )
        with pytest.raises(
            RuntimeError,
            match="not eligible for a safe retry",
        ):
            workflow.record_remote_attempt(
                first.id,
                "candidate_notification",
            )

        duplicate = create_candidate(
            workflow,
            SourceObservation(
                channel_id="channel",
                channel_handle="publisher",
                external_post_id="two",
                published_at=published,
                text=text,
                urls=(UrlCandidate("https://example.test/story"),),
            ),
        )
        assert duplicate is None

        workflow._db.execute(
            "DROP INDEX v2_revisions_created",
        )
        with pytest.raises(
            RuntimeError,
            match="missing required Newsbot V2 indexes",
        ):
            workflow._validate_migration_before_commit()

    alert_values = [item.labels["alert"] for item in sink.events if item.metric is MetricName.ALERT]
    assert ImmediateAlert.CONFIRMED_EFFECT_REATTEMPT in alert_values
    assert ImmediateAlert.DUPLICATE_CLAIM in alert_values
    assert ImmediateAlert.MIGRATION_RETENTION_MISMATCH in alert_values


def test_threshold_evaluation_is_strict_and_deterministic() -> None:
    at_boundary = ThresholdSnapshot(
        fetch_total=10,
        fetch_blocked=2,
        fetch_transient=2,
        database_bytes=150,
        wal_bytes=50,
        seven_day_storage_baseline_bytes=100,
        oldest_queue_age_seconds=86_400,
        oldest_manual_review_age_seconds=86_400,
    )
    assert evaluate_thresholds(at_boundary) == ()

    above_boundary = ThresholdSnapshot(
        fetch_total=10,
        fetch_blocked=3,
        fetch_transient=3,
        database_bytes=151,
        wal_bytes=50,
        seven_day_storage_baseline_bytes=100,
        oldest_queue_age_seconds=86_401,
        oldest_manual_review_age_seconds=86_401,
    )
    expected = (
        ThresholdAlert.FETCH_BLOCKED,
        ThresholdAlert.FETCH_TRANSIENT,
        ThresholdAlert.DATABASE_GROWTH,
        ThresholdAlert.OLDEST_QUEUE_AGE,
        ThresholdAlert.OLDEST_MANUAL_REVIEW_AGE,
    )
    assert evaluate_thresholds(above_boundary) == expected
    assert evaluate_thresholds(above_boundary) == expected


@pytest.mark.parametrize(
    "kwargs, message",
    (
        ({"fetch_total": -1}, "cannot be negative"),
        ({"fetch_total": 2, "fetch_blocked": 3}, "cannot exceed"),
        ({"fetch_total": 2, "fetch_transient": 3}, "cannot exceed"),
        ({"fetch_total": 2, "fetch_blocked": 1, "fetch_transient": 2}, "cannot exceed"),
        ({"fetch_total": 0, "seven_day_storage_baseline_bytes": 0}, "baseline must be positive"),
        ({"fetch_total": 0, "seven_day_storage_baseline_bytes": -1}, "baseline must be positive"),
    ),
)
def test_threshold_snapshot_rejects_impossible_values(kwargs, message) -> None:
    with pytest.raises(ValueError, match=message):
        ThresholdSnapshot(**kwargs)
