"""Fraud-model serving: a hot-reloadable MLlib PipelineModel with a rules fallback.

The stream never stops to deploy a model. The trainer writes a new version and atomically flips
`LATEST.json`; the running job notices within MODEL_RELOAD_S seconds and swaps it in between
micro-batches. If no model exists yet (or loading fails) a transparent rules score is used, so
the pipeline degrades gracefully instead of crashing.
"""

from __future__ import annotations

import json
import logging
import math
import os
import time
from dataclasses import dataclass

from pyspark.ml import PipelineModel
from pyspark.ml.functions import vector_to_array
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

log = logging.getLogger("shopmetric.model")

POINTER_FILE = "LATEST.json"
RULES_VERSION = "rules-v1"
RULES_THRESHOLD = 0.6


def rules_score_col() -> Column:
    """Hand-tuned heuristic score in [0, 1]. Also the baseline the ML model must beat."""
    score = (
        0.25 * F.col("is_new_device")
        + 0.20 * F.col("ip_billing_mismatch")
        + 0.15 * F.col("ship_billing_mismatch")
        + 0.20 * (F.col("cust_txn_count_10m") >= 4).cast("double")
        + 0.15 * (F.col("amount_usd") >= 1000).cast("double")
        + 0.15 * F.col("payment_method").isin("crypto", "gift_card").cast("double")
        + 0.10 * F.col("is_new_account")
    )
    return F.least(score, F.lit(1.0))


@dataclass(frozen=True)
class LoadedModel:
    version: str
    threshold: float
    pipeline: PipelineModel | None = None  # None -> rules fallback

    def score(self, df: DataFrame) -> DataFrame:
        """Adds `fraud_score` (probability of fraud) and keeps every original column."""
        if self.pipeline is None:
            return df.withColumn("fraud_score", F.round(rules_score_col(), 4))
        cols = df.columns
        scored = self.pipeline.transform(df)
        return scored.select(*cols, F.round(vector_to_array("probability")[1], 4).alias("fraud_score"))


class ModelProvider:
    def __init__(self, model_dir: str, reload_s: int = 30) -> None:
        self.model_dir = model_dir
        self.reload_s = reload_s
        self._current = LoadedModel(RULES_VERSION, RULES_THRESHOLD)
        self._pointer: dict | None = None
        self._last_check = -math.inf
        self._warned_missing = False

    def _read_pointer(self) -> dict | None:
        try:
            with open(os.path.join(self.model_dir, POINTER_FILE)) as fh:
                return json.load(fh)
        except FileNotFoundError:
            return None

    def get(self) -> LoadedModel:
        now = time.monotonic()
        if now - self._last_check < self.reload_s:
            return self._current
        self._last_check = now
        pointer = self._read_pointer()
        if pointer is None:
            if not self._warned_missing:
                log.info("no trained model at %s yet -> using %s", self.model_dir, RULES_VERSION)
                self._warned_missing = True
            return self._current
        if pointer != self._pointer:
            try:
                path = pointer["path"]
                if not os.path.isabs(path):
                    path = os.path.join(self.model_dir, path)
                pipeline = PipelineModel.load(path)
                self._current = LoadedModel(pointer["version"], float(pointer["threshold"]), pipeline)
                self._pointer = pointer
                log.info(
                    "loaded fraud model %s (threshold=%.3f)", pointer["version"], self._current.threshold
                )
            except Exception:  # keep serving the previous model rather than failing the stream
                log.exception("failed to load model %s; keeping %s", pointer, self._current.version)
        return self._current
