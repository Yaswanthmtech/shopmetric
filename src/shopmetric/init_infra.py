"""One-shot, idempotent bootstrap: lake bucket + warehouse tables/roles.

python -m shopmetric.init_infra             # create what is missing
python -m shopmetric.init_infra --truncate  # also empty the warehouse tables (fresh demo)
"""

from __future__ import annotations

import argparse
import logging
import os
import time
from collections.abc import Callable
from urllib.parse import urlparse

from .config import Settings, get_settings
from .warehouse import get_warehouse

log = logging.getLogger("shopmetric.init")


def retry(fn: Callable[[], object], what: str, timeout_s: int = 120) -> object:
    deadline = time.time() + timeout_s
    while True:
        try:
            return fn()
        except SystemExit:
            raise
        except Exception as exc:  # noqa: BLE001 - startup races (DB/S3 not ready yet)
            if time.time() > deadline:
                raise
            log.info("waiting for %s (%s)", what, exc.__class__.__name__)
            time.sleep(3)


def ensure_bucket(s: Settings) -> None:
    if not s.lake_is_s3:
        os.makedirs(urlparse(s.lake_uri).path, exist_ok=True)
        return
    import boto3
    from botocore.config import Config
    from botocore.exceptions import ClientError

    s3 = boto3.client(
        "s3",
        endpoint_url=s.s3_endpoint,
        region_name=s.aws_region,
        aws_access_key_id=s.aws_access_key_id,
        aws_secret_access_key=s.aws_secret_access_key,
        # S3-compatible stores are addressed by path (http://host:8333/bucket), not by sub-domain.
        config=Config(s3={"addressing_style": "path"}) if s.s3_endpoint else None,
    )
    try:
        s3.head_bucket(Bucket=s.bucket)
        log.info("bucket s3://%s ready", s.bucket)
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") not in ("404", "NoSuchBucket", "NotFound"):
            raise
        if not s.s3_endpoint:
            raise SystemExit(
                f"S3 bucket {s.bucket!r} does not exist. Create it once (see README, cloud setup):\n"
                f"  aws s3 mb s3://{s.bucket} --region {s.aws_region}"
            ) from exc
        s3.create_bucket(Bucket=s.bucket)
        log.info("created bucket s3://%s on %s", s.bucket, s.s3_endpoint)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Bootstrap ShopMetric storage and warehouse")
    ap.add_argument("--truncate", action="store_true", help="empty warehouse tables")
    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s | %(message)s")
    s = get_settings()

    retry(lambda: ensure_bucket(s), "object storage")
    wh = get_warehouse(s)
    retry(wh.init_schema, f"{s.warehouse} warehouse")
    if args.truncate:
        wh.truncate_all()
        log.info("warehouse tables truncated")
    wh.close()
    log.info("bootstrap complete (lake=%s, warehouse=%s)", s.lake_uri, s.warehouse)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
