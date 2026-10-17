"""Kafka producer: streams simulated checkout events to `orders.raw`.

Design choices worth talking about in an interview:
  * key = customer_id  -> every order of a customer lands on the same partition, in order,
                          which is what makes per-customer stateful features correct downstream.
  * enable.idempotence + acks=all -> no duplicates/reordering from producer retries.
  * lz4 compression + small linger -> batching for throughput without hurting latency.
  * header `x-sim-kind` marks injected chaos so data-quality catch-rate can be *measured*,
    not assumed. Real pipelines would not have this; it exists because the data is simulated.

Usage:
  python -m shopmetric.producer --rate 5                     # steady stream (≈432k orders/day)
  python -m shopmetric.producer --rate 200 --duration-s 180  # load test
  python -m shopmetric.producer --scenario card_testing --count 3   # live fraud demo
"""

from __future__ import annotations

import argparse
import logging
import signal
import time
from collections import Counter
from datetime import UTC, datetime

from confluent_kafka import KafkaException, Producer

from .config import get_settings
from .generator import FRAUD_SCENARIOS, Emission, OrderSimulator, customers_for_rate

log = logging.getLogger("shopmetric.producer")


def build_producer(bootstrap: str) -> Producer:
    return Producer(
        {
            "bootstrap.servers": bootstrap,
            "client.id": "shopmetric-order-service",
            "enable.idempotence": True,
            "acks": "all",
            "linger.ms": 5,
            "compression.type": "lz4",
            "queue.buffering.max.messages": 200_000,
        }
    )


class OrderPublisher:
    def __init__(self, producer: Producer, topic: str) -> None:
        self.producer = producer
        self.topic = topic
        self.stats: Counter[str] = Counter()

    def _on_delivery(self, err, _msg) -> None:
        self.stats["delivered" if err is None else "failed"] += 1
        if err is not None:
            log.warning("delivery failed: %s", err)

    def send(self, em: Emission) -> None:
        while True:
            try:
                self.producer.produce(
                    self.topic,
                    key=em.key.encode(),
                    value=em.value,
                    headers=[("x-sim-kind", em.kind.encode())],
                    on_delivery=self._on_delivery,
                )
                break
            except BufferError:  # local queue full -> let librdkafka drain, then retry
                self.producer.poll(0.5)
        self.stats["sent"] += 1
        self.stats[f"kind:{em.kind}"] += 1
        if em.label and em.label != "legit" and em.kind == "order":
            self.stats[f"fraud:{em.label}"] += 1
        self.producer.poll(0)


def _sleep_until(ts: datetime, stop: dict) -> None:
    while not stop["flag"]:
        delay = ts.timestamp() - time.time()
        if delay <= 0:
            return
        time.sleep(min(delay, 0.5))


def main(argv: list[str] | None = None) -> int:
    s = get_settings()
    p = argparse.ArgumentParser(description="ShopMetric order event producer")
    p.add_argument("--rate", type=float, default=5.0, help="mean orders per second (Poisson)")
    p.add_argument("--max-events", type=int, default=None)
    p.add_argument("--duration-s", type=float, default=None)
    p.add_argument("--fraud-rate", type=float, default=0.01)
    p.add_argument("--chaos-rate", type=float, default=0.006)
    p.add_argument("--customers", type=int, default=None, help="default: scales with --rate")
    p.add_argument("--seed", type=int, default=None)
    p.add_argument("--scenario", choices=sorted(FRAUD_SCENARIOS), help="inject fraud scenario(s) and exit")
    p.add_argument("--count", type=int, default=1, help="how many scenarios to inject with --scenario")
    args = p.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s | %(message)s")
    stop = {"flag": False}

    def _handle(sig, _frame):
        log.info("signal %s received, flushing and exiting", sig)
        stop["flag"] = True

    signal.signal(signal.SIGTERM, _handle)
    signal.signal(signal.SIGINT, _handle)

    publisher = OrderPublisher(build_producer(s.kafka_bootstrap), s.topic_orders)
    sim = OrderSimulator(
        seed=args.seed,
        n_customers=args.customers or customers_for_rate(args.rate),
        fraud_rate=args.fraud_rate,
        chaos_rate=args.chaos_rate,
    )
    started = time.time()

    try:
        if args.scenario:
            emissions = sorted(
                em for _ in range(args.count) for em in sim.scenario(args.scenario, datetime.now(UTC))
            )
            log.info(
                "injecting %d x %s (%d orders over %.0fs)",
                args.count,
                args.scenario,
                len(emissions),
                (emissions[-1].ts - emissions[0].ts).total_seconds(),
            )
            for em in emissions:
                _sleep_until(em.ts, stop)
                publisher.send(em)
        else:
            log.info(
                "producing to %s @ %.1f orders/s (fraud_rate=%.3f, chaos_rate=%.3f)",
                s.topic_orders,
                args.rate,
                args.fraud_rate,
                args.chaos_rate,
            )
            end = None
            if args.duration_s:
                end = datetime.fromtimestamp(time.time() + args.duration_s, UTC)
            last_log = time.time()
            for em in sim.run(datetime.now(UTC), args.rate, max_events=args.max_events, end=end):
                if stop["flag"]:
                    break
                _sleep_until(em.ts, stop)
                publisher.send(em)
                if time.time() - last_log >= 10:
                    elapsed = time.time() - started
                    log.info(
                        "sent=%d (%.1f/s) delivered=%d failed=%d fraud=%d chaos=%d",
                        publisher.stats["sent"],
                        publisher.stats["sent"] / elapsed,
                        publisher.stats["delivered"],
                        publisher.stats["failed"],
                        sum(v for k, v in publisher.stats.items() if k.startswith("fraud:")),
                        publisher.stats["sent"] - publisher.stats["kind:order"],
                    )
                    last_log = time.time()
    except KafkaException as exc:
        log.error("kafka error: %s", exc)
        return 1
    finally:
        remaining = publisher.producer.flush(15)
        log.info("done: %s (unflushed=%d)", dict(publisher.stats), remaining)
    return 0 if publisher.stats["failed"] == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
