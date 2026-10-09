"""Discriminating checks for the isolated event proof shipped in the wheel."""
import json
from unittest.mock import patch

import pytest

import chaski_verify


def test_real_event_reaches_receiver_and_replay_is_deduplicated(tmp_path):
    result = chaski_verify.verify_event(tmp_path, "round-trip")
    assert result["verified"] is True
    rows = (tmp_path / "chaski-event-proof" / "events.jsonl").read_text().splitlines()
    assert len(rows) == 1
    assert json.loads(rows[0])["event_id"] == "round-trip"


def test_existing_directory_is_preserved(tmp_path):
    work = tmp_path / "chaski-event-proof"
    work.mkdir()
    sentinel = work / "emitter.db"
    sentinel.write_bytes(b"preserve this caller-owned database")
    with pytest.raises(FileExistsError):
        chaski_verify.verify_event(tmp_path, "round-trip")
    assert sentinel.read_bytes() == b"preserve this caller-owned database"


def test_delivery_that_returns_without_a_receipt_is_refused(tmp_path):
    with patch.object(chaski_verify.emitter.JsonlSink, "deliver", return_value=None):
        with pytest.raises(RuntimeError, match="did not reach"):
            chaski_verify.verify_event(tmp_path, "round-trip")


def test_failed_adapter_cannot_produce_a_successful_proof(tmp_path):
    with patch.object(chaski_verify.emitter, "run_adapter", side_effect=RuntimeError("failed adapter")):
        with pytest.raises(RuntimeError, match="did not reach"):
            chaski_verify.verify_event(tmp_path, "round-trip")
