"""Train the fraud model (Spark MLlib Gradient-Boosted Trees) with honest evaluation.

  * Same parsing + data-quality + feature code as the stream (no training/serving skew).
  * Time-based split 70/15/15: train on the past, pick the threshold on the next slice,
    report on the most recent slice the model has never seen (no future leakage).
  * Class imbalance handled with instance weights instead of resampling.
  * Promotion gate: a new version only becomes LATEST if it beats the rules baseline on
    the test slice. The running stream hot-swaps it within MODEL_RELOAD_S seconds.

Usage:  python -m shopmetric.train_model --orders 250000 --rate 5
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import time
from datetime import UTC, datetime

import numpy as np
import pandas as pd
from pyspark.ml import Pipeline
from pyspark.ml.classification import GBTClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .config import get_settings
from .contract import FRAUD_LABELS
from .features import CATEGORICAL_FEATURES, NUMERIC_FEATURES, add_velocity_features_batch
from .generator import simulate_history
from .model import POINTER_FILE, RULES_THRESHOLD, LoadedModel, rules_score_col
from .spark_session import build_spark
from .transforms import parse_orders

log = logging.getLogger("shopmetric.train")


def build_training_frame(
    spark: SparkSession, n_orders: int, rate: float, seed: int, window_ms: int
) -> DataFrame:
    rows = simulate_history(n_orders, rate=rate, seed=seed)
    raw = spark.createDataFrame(pd.DataFrame({"raw_value": [json.dumps(r) for r in rows]}))
    src = raw.select(
        "raw_value",
        F.lit(None).cast("string").alias("kafka_key"),
        F.lit("history").alias("kafka_topic"),
        F.lit(0).alias("kafka_partition"),
        F.monotonically_increasing_id().alias("kafka_offset"),
        F.current_timestamp().alias("kafka_ts"),
        F.lit("order").alias("sim_kind"),
    )
    df = parse_orders(src).filter(F.col("dq_reason").isNull()).dropDuplicates(["order_id"])
    df = add_velocity_features_batch(df, window_ms)
    return df.withColumn("label", F.col("simulated_label").isin(*FRAUD_LABELS).cast("double"))


def confusion(scores: np.ndarray, labels: np.ndarray, threshold: float) -> dict:
    pred = scores >= threshold
    pos = labels == 1
    tp, fp = int((pred & pos).sum()), int((pred & ~pos).sum())
    fn, tn = int((~pred & pos).sum()), int((~pred & ~pos).sum())
    precision = tp / (tp + fp) if tp + fp else 0.0
    recall = tp / (tp + fn) if tp + fn else 0.0
    f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
    return {
        "threshold": round(threshold, 3),
        "tp": tp,
        "fp": fp,
        "fn": fn,
        "tn": tn,
        "precision": round(precision, 4),
        "recall": round(recall, 4),
        "f1": round(f1, 4),
        "false_positive_rate": round(fp / (fp + tn), 5) if fp + tn else 0.0,
    }


def best_threshold(scores: np.ndarray, labels: np.ndarray) -> float:
    grid = np.round(np.arange(0.05, 0.96, 0.01), 2)
    return float(max(grid, key=lambda t: (confusion(scores, labels, t)["f1"], t)))


def evaluate(df: DataFrame, score_col: str, threshold: float) -> dict:
    auc_roc = BinaryClassificationEvaluator(
        rawPredictionCol=score_col, labelCol="label", metricName="areaUnderROC"
    ).evaluate(df)
    auc_pr = BinaryClassificationEvaluator(
        rawPredictionCol=score_col, labelCol="label", metricName="areaUnderPR"
    ).evaluate(df)
    pdf = df.select(score_col, "label", "simulated_label").toPandas()
    out = confusion(pdf[score_col].to_numpy(), pdf["label"].to_numpy(), threshold)
    out.update({"auc_roc": round(auc_roc, 4), "auc_pr": round(auc_pr, 4)})
    caught = pdf[score_col] >= threshold
    out["recall_by_scenario"] = {
        lbl: round(float(caught[pdf["simulated_label"] == lbl].mean()), 4)
        for lbl in FRAUD_LABELS
        if (pdf["simulated_label"] == lbl).any()
    }
    return out


def main(argv: list[str] | None = None) -> int:
    s = get_settings()
    ap = argparse.ArgumentParser(description="Train the ShopMetric fraud model")
    ap.add_argument("--orders", type=int, default=250_000)
    ap.add_argument(
        "--rate",
        type=float,
        default=float(os.getenv("PRODUCER_RATE", "5")),
        help="orders/s of the simulated history; match the live producer so velocity features line up",
    )
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--max-iter", type=int, default=60)
    ap.add_argument("--max-depth", type=int, default=5)
    ap.add_argument("--model-dir", default=s.model_dir)
    ap.add_argument(
        "--force-promote", action="store_true", help="promote even if it does not beat the baseline"
    )
    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s | %(message)s")

    started = time.time()
    spark = build_spark(s, "shopmetric-train", {"spark.sql.shuffle.partitions": "8"})
    df = build_training_frame(spark, args.orders, args.rate, args.seed, s.velocity_window_ms).cache()

    q70, q85 = df.approxQuantile("event_ts_ms", [0.70, 0.85], 0.0005)
    train = df.filter(F.col("event_ts_ms") < q70)
    val = df.filter((F.col("event_ts_ms") >= q70) & (F.col("event_ts_ms") < q85))
    test = df.filter(F.col("event_ts_ms") >= q85)

    n_pos, n_total = train.agg(F.sum("label"), F.count(F.lit(1))).first()
    pos_weight = min((n_total - n_pos) / max(n_pos, 1), 25.0)
    train = train.withColumn("weight", F.when(F.col("label") == 1, F.lit(pos_weight)).otherwise(F.lit(1.0)))
    log.info(
        "rows=%d train=%d (fraud=%d, weight=%.1f) val=%d test=%d",
        df.count(),
        n_total,
        n_pos,
        pos_weight,
        val.count(),
        test.count(),
    )

    idx_cols = [f"{c}_idx" for c in CATEGORICAL_FEATURES]
    pipeline = Pipeline(
        stages=[
            StringIndexer(inputCols=CATEGORICAL_FEATURES, outputCols=idx_cols, handleInvalid="keep"),
            VectorAssembler(
                inputCols=NUMERIC_FEATURES + idx_cols, outputCol="features", handleInvalid="keep"
            ),
            GBTClassifier(
                labelCol="label",
                featuresCol="features",
                weightCol="weight",
                maxIter=args.max_iter,
                maxDepth=args.max_depth,
                stepSize=0.1,
                subsamplingRate=0.8,
                seed=args.seed,
            ),
        ]
    )
    fitted = pipeline.fit(train)
    candidate = LoadedModel("candidate", 0.5, fitted)

    val_scored = candidate.score(val).select("fraud_score", "label").toPandas()
    threshold = best_threshold(val_scored["fraud_score"].to_numpy(), val_scored["label"].to_numpy())
    test_metrics = evaluate(candidate.score(test), "fraud_score", threshold)
    baseline = evaluate(test.withColumn("rules_score", rules_score_col()), "rules_score", RULES_THRESHOLD)

    gbt = fitted.stages[-1]
    importances = sorted(
        (
            (name, round(float(v), 4))
            for name, v in zip(NUMERIC_FEATURES + idx_cols, gbt.featureImportances.toArray(), strict=True)
        ),
        key=lambda kv: -kv[1],
    )
    version = f"gbt-{datetime.now(UTC):%Y%m%d-%H%M%S}"
    rel_path = f"versions/{version}/model"
    os.makedirs(os.path.join(args.model_dir, "versions", version), exist_ok=True)
    fitted.write().overwrite().save(os.path.join(args.model_dir, rel_path))

    promote = args.force_promote or (
        test_metrics["f1"] > baseline["f1"] and test_metrics["auc_pr"] > baseline["auc_pr"]
    )
    metadata = {
        "version": version,
        "trained_at": datetime.now(UTC).isoformat(),
        "algorithm": "Spark MLlib GBTClassifier",
        "params": {
            "max_iter": args.max_iter,
            "max_depth": args.max_depth,
            "pos_weight": round(pos_weight, 2),
            "velocity_window_ms": s.velocity_window_ms,
        },
        "data": {
            "orders_simulated": args.orders,
            "rate_per_s": args.rate,
            "history_hours": round(args.orders / args.rate / 3600, 1),
            "seed": args.seed,
            "train_rows": int(n_total),
            "train_fraud_rows": int(n_pos),
        },
        "split": "time-based 70/15/15 (threshold tuned on validation, metrics on test)",
        "threshold": threshold,
        "test_metrics": test_metrics,
        "rules_baseline_test_metrics": baseline,
        "feature_importances": dict(importances),
        "features": {"numeric": NUMERIC_FEATURES, "categorical": CATEGORICAL_FEATURES},
        "promoted": promote,
        "train_seconds": round(time.time() - started, 1),
    }
    with open(os.path.join(args.model_dir, "versions", version, "metadata.json"), "w") as fh:
        json.dump(metadata, fh, indent=2)

    if promote:  # atomic pointer flip -> the running stream hot-reloads this version
        tmp = os.path.join(args.model_dir, POINTER_FILE + ".tmp")
        with open(tmp, "w") as fh:
            json.dump({"version": version, "path": rel_path, "threshold": threshold}, fh)
        os.replace(tmp, os.path.join(args.model_dir, POINTER_FILE))

    print("\n================ ShopMetric fraud model ================")
    print(f"version            {version}  ({'PROMOTED to LATEST' if promote else 'NOT promoted'})")
    print(f"threshold          {threshold}")
    for name, m in (("GBT model", test_metrics), ("rules baseline", baseline)):
        print(
            f"{name:<18} AUC-PR={m['auc_pr']:.3f} AUC-ROC={m['auc_roc']:.3f} precision={m['precision']:.3f} "
            f"recall={m['recall']:.3f} F1={m['f1']:.3f} FPR={m['false_positive_rate']:.4f}"
        )
    print(f"recall by scenario {test_metrics['recall_by_scenario']}")
    print(f"top features       {importances[:5]}")
    print(
        f"saved to           {os.path.join(args.model_dir, 'versions', version)}  in {metadata['train_seconds']}s"
    )
    print("=========================================================\n")
    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
