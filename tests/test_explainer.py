"""Tests for the LLM explainer (Anthropic client is mocked)."""

from unittest.mock import MagicMock, patch

import pytest

from dqm.explainer import Explainer, _build_prompt, _cache_key
from dqm.models import Anomaly, AnomalyContext


@pytest.fixture
def anomaly():
    return Anomaly(
        column="title",
        metric="null_pct",
        value_before=0.01,
        value_after=0.24,
        severity="ALERT",
        delta_pp=23.0,
    )


@pytest.fixture
def context():
    return AnomalyContext(
        table="episodes",
        top_values_before=["ep_001", "ep_002", "ep_003"],
        top_values_after=["None", "None", "ep_004"],
    )


def _make_client(text: str) -> MagicMock:
    msg = MagicMock()
    msg.content = [MagicMock(text=text)]
    client = MagicMock()
    client.messages.create.return_value = msg
    return client


def test_prompt_contains_key_info(anomaly, context):
    prompt = _build_prompt(anomaly, context)
    assert "episodes" in prompt
    assert "title" in prompt
    assert "null_pct" in prompt
    assert "1.0%" in prompt   # value_before formatted
    assert "24.0%" in prompt  # value_after formatted
    assert "+23pp" in prompt


def test_prompt_includes_top_values(anomaly, context):
    prompt = _build_prompt(anomaly, context)
    assert "ep_001" in prompt
    assert "None" in prompt


def test_explain_calls_claude(anomaly, context):
    client = _make_client("Likely a pipeline failure.")
    exp = Explainer(client=client)
    result = exp.explain(anomaly, context)
    assert result == "Likely a pipeline failure."
    client.messages.create.assert_called_once()


def test_explain_uses_cache(anomaly, context):
    client = _make_client("First call.")
    exp = Explainer(client=client)
    first = exp.explain(anomaly, context)
    second = exp.explain(anomaly, context)
    assert first == second
    assert client.messages.create.call_count == 1  # not called twice


def test_explain_cache_key_stable(anomaly, context):
    k1 = _cache_key(anomaly, context)
    k2 = _cache_key(anomaly, context)
    assert k1 == k2


def test_explain_cache_key_differs_on_column(anomaly, context):
    anomaly2 = Anomaly(
        column="duration",
        metric="null_pct",
        value_before=0.01,
        value_after=0.24,
        severity="ALERT",
        delta_pp=23.0,
    )
    assert _cache_key(anomaly, context) != _cache_key(anomaly2, context)


def test_explain_all_returns_dict(anomaly, context):
    client = _make_client("Pipeline dropped null checks.")
    exp = Explainer(client=client)
    results = exp.explain_all([anomaly], context)
    assert "title" in results
    assert "Pipeline" in results["title"]


def test_explain_all_empty(context):
    exp = Explainer(client=MagicMock())
    results = exp.explain_all([], context)
    assert results == {}


def test_pre_populated_cache_skips_api(anomaly, context):
    key = _cache_key(anomaly, context)
    cache = {key: "Cached explanation."}
    client = MagicMock()
    exp = Explainer(client=client, cache=cache)
    result = exp.explain(anomaly, context)
    assert result == "Cached explanation."
    client.messages.create.assert_not_called()
