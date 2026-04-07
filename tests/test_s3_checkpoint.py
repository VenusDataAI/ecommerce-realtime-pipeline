"""Tests for S3Checkpoint in local mode."""
from __future__ import annotations

import json
from pathlib import Path

import pytest


@pytest.fixture(autouse=True)
def local_mode(monkeypatch):
    monkeypatch.setenv("KINESIS_MODE", "local")


@pytest.fixture()
def checkpoint(tmp_path):
    from storage.s3_checkpoint import S3Checkpoint
    return S3Checkpoint(local_path=tmp_path / "checkpoints.json")


def test_save_and_get(checkpoint):
    checkpoint.save("shard:001", "12345")
    assert checkpoint.get("shard:001") == "12345"


def test_get_missing_key_returns_none(checkpoint):
    assert checkpoint.get("nonexistent") is None


def test_save_persists_to_disk(checkpoint, tmp_path):
    checkpoint.save("stream:shard", "seq-abc")
    data = json.loads((tmp_path / "checkpoints.json").read_text())
    assert data["stream:shard"] == "seq-abc"


def test_overwrite_existing_key(checkpoint):
    checkpoint.save("key1", "v1")
    checkpoint.save("key1", "v2")
    assert checkpoint.get("key1") == "v2"


def test_multiple_keys(checkpoint):
    checkpoint.save("k1", "a")
    checkpoint.save("k2", "b")
    checkpoint.save("k3", "c")
    assert checkpoint.get("k1") == "a"
    assert checkpoint.get("k2") == "b"
    assert checkpoint.get("k3") == "c"


def test_load_from_existing_file(tmp_path, monkeypatch):
    monkeypatch.setenv("KINESIS_MODE", "local")
    path = tmp_path / "existing.json"
    path.write_text(json.dumps({"shard:0": "999"}))
    from storage.s3_checkpoint import S3Checkpoint
    cp = S3Checkpoint(local_path=path)
    assert cp.get("shard:0") == "999"


def test_corrupted_file_handled(tmp_path, monkeypatch):
    monkeypatch.setenv("KINESIS_MODE", "local")
    path = tmp_path / "bad.json"
    path.write_text("{not valid json")
    from storage.s3_checkpoint import S3Checkpoint
    cp = S3Checkpoint(local_path=path)  # should not raise
    assert cp.get("anything") is None
