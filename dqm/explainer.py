"""LLM explainer: ask Claude to diagnose anomalies in plain English."""

import hashlib
import anthropic

from .models import Anomaly, AnomalyContext

_DEFAULT_MODEL = "claude-haiku-4-5-20251001"
_MAX_TOKENS = 300


def _cache_key(anomaly: Anomaly, context: AnomalyContext) -> str:
    parts = (
        context.table,
        anomaly.column,
        anomaly.metric,
        f"{anomaly.value_before:.4f}",
        f"{anomaly.value_after:.4f}",
    )
    return hashlib.sha256("|".join(parts).encode()).hexdigest()


def _build_prompt(anomaly: Anomaly, context: AnomalyContext) -> str:
    old_pct = f"{anomaly.value_before:.1%}"
    new_pct = f"{anomaly.value_after:.1%}"
    sign = "+" if anomaly.delta_pp >= 0 else ""
    delta = f"{sign}{anomaly.delta_pp:.0f}pp"

    top_before = ", ".join(context.top_values_before) if context.top_values_before else "N/A"
    top_after = ", ".join(context.top_values_after) if context.top_values_after else "N/A"

    return (
        f"Table: {context.table}, Column: {anomaly.column}\n"
        f"Anomaly: {anomaly.metric} jumped from {old_pct} to {new_pct} ({delta})\n"
        f"Top values before: {top_before}\n"
        f"Top values after: {top_after}\n\n"
        "What likely caused this? What should the data engineer investigate?\n"
        "Keep answer under 100 words."
    )


class Explainer:
    def __init__(self, client: anthropic.Anthropic | None = None, cache: dict[str, str] | None = None):
        self._client = client or anthropic.Anthropic()
        self._cache: dict[str, str] = cache if cache is not None else {}

    def explain(self, anomaly: Anomaly, context: AnomalyContext) -> str:
        """Return a plain-English explanation of the anomaly, using cache if available."""
        key = _cache_key(anomaly, context)
        if key in self._cache:
            return self._cache[key]

        prompt = _build_prompt(anomaly, context)
        message = self._client.messages.create(
            model=_DEFAULT_MODEL,
            max_tokens=_MAX_TOKENS,
            messages=[{"role": "user", "content": prompt}],
        )
        explanation = message.content[0].text.strip()
        self._cache[key] = explanation
        return explanation

    def explain_all(
        self,
        anomalies: list[Anomaly],
        context: AnomalyContext,
    ) -> dict[str, str]:
        """Return {column: explanation} for every anomaly."""
        return {a.column: self.explain(a, context) for a in anomalies}
