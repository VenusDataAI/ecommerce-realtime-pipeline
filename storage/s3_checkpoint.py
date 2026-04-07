"""
S3Checkpoint: persists consumer shard sequence numbers.
In local mode, uses a JSON file on disk.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Optional

import structlog

logger = structlog.get_logger(__name__)


class S3Checkpoint:
    """
    Saves/loads consumer checkpoints.

    Parameters
    ----------
    bucket : str | None
        S3 bucket name (used in AWS mode).
    prefix : str
        S3 key prefix.
    local_path : Path | str
        Local file path for checkpoint state in local mode.
    """

    def __init__(
        self,
        bucket: str | None = None,
        prefix: str = "checkpoints/",
        local_path: Path | str = Path("data/checkpoints.json"),
    ) -> None:
        self._mode = os.environ.get("KINESIS_MODE", "local").lower()
        self._bucket = bucket or os.environ.get("S3_CHECKPOINT_BUCKET", "")
        self._prefix = prefix
        self._local_path = Path(local_path)
        self._cache: dict[str, str] = {}

        if self._mode == "local":
            self._load_local()
            logger.info("S3Checkpoint in LOCAL mode", path=str(self._local_path))
        else:
            import boto3
            self._s3 = boto3.client("s3")
            logger.info("S3Checkpoint in S3 mode", bucket=self._bucket)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def save(self, key: str, value: str) -> None:
        """Persist a checkpoint value."""
        self._cache[key] = value
        if self._mode == "local":
            self._save_local()
        else:
            self._put_s3(key, value)
        logger.debug("Checkpoint saved", key=key, value=value)

    def get(self, key: str) -> Optional[str]:
        """Retrieve a checkpoint value. Returns None if not found."""
        if self._mode != "local":
            val = self._get_s3(key)
            if val is not None:
                self._cache[key] = val
        return self._cache.get(key)

    # ------------------------------------------------------------------
    # Local helpers
    # ------------------------------------------------------------------

    def _load_local(self) -> None:
        if self._local_path.exists():
            try:
                self._cache = json.loads(self._local_path.read_text("utf-8"))
            except json.JSONDecodeError:
                self._cache = {}

    def _save_local(self) -> None:
        self._local_path.parent.mkdir(parents=True, exist_ok=True)
        self._local_path.write_text(json.dumps(self._cache, indent=2), "utf-8")

    # ------------------------------------------------------------------
    # S3 helpers
    # ------------------------------------------------------------------

    def _put_s3(self, key: str, value: str) -> None:
        s3_key = f"{self._prefix}{key.replace(':', '_')}.txt"
        try:
            self._s3.put_object(Bucket=self._bucket, Key=s3_key, Body=value.encode())
        except Exception as exc:
            logger.error("S3 checkpoint put failed", error=str(exc))

    def _get_s3(self, key: str) -> Optional[str]:
        s3_key = f"{self._prefix}{key.replace(':', '_')}.txt"
        try:
            resp = self._s3.get_object(Bucket=self._bucket, Key=s3_key)
            return resp["Body"].read().decode("utf-8")
        except self._s3.exceptions.NoSuchKey:
            return None
        except Exception as exc:
            logger.warning("S3 checkpoint get failed", error=str(exc))
            return None
