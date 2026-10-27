"""Local state invariants; real Redis atomicity has separate leased evidence."""

import base64
import hashlib
import json
from unittest.mock import MagicMock, patch

import pytest

from src import state
from src.env import settings


def test_same_job_id_is_isolated_between_split_and_rendition_and_delivery_leases():
    with patch.object(settings, "audio_redis_prefix", "owned-unit:"):
        split = state.worker_store("split")
        rendition = state.worker_store("rendition")
        assert split.job_key("same-id") == "owned-unit:job:same-id"
        assert state.job_key("same-id") == split.job_key("same-id")
        assert len({split.job_key("same-id"), rendition.job_key("same-id"),
                    rendition.lease_key("same-id", "execution"),
                    rendition.lease_key("same-id", "callback")}) == 4
        with pytest.raises(ValueError):
            state.worker_store("untrusted-kind")


def test_duplicate_intent_cannot_replace_canceled_tombstone_or_identity():
    store = state.worker_store("rendition")
    db = MagicMock()
    db.set.return_value = None
    tombstone = {"identityHash": "a" * 64, "status": "canceled"}
    db.get.return_value = json.dumps(tombstone)
    with patch("src.state.client", return_value=db):
        created, current = store.save_new("job", {"status": "pending"},
                                        identity_hash="a" * 64, retention_seconds=60)
        assert not created and current == tombstone
        with pytest.raises(state.StateConflict):
            store.save_new("job", {"status": "pending"},
                           identity_hash="b" * 64, retention_seconds=60)
    assert all(call.kwargs["nx"] for call in db.set.call_args_list)


def test_rendition_has_no_blind_save_bypass():
    with patch("src.state.client") as client:
        with pytest.raises(state.StateConflict):
            state.worker_store("rendition").save("job", {"status": "pending"})
    client.assert_not_called()


def test_canceled_phase_prevents_completion_before_any_write():
    db = MagicMock()
    db.get.return_value = '{"status":"canceled"}'
    with patch("src.state.client", return_value=db):
        with pytest.raises(state.StateConflict):
            state.worker_store("rendition").transition(
                "job", expected_statuses={"processing"},
                changes={"status": "callback_pending"}, retention_seconds=60)
    db.eval.assert_not_called()


def test_wrong_job_or_kind_lease_is_rejected_before_read_or_write():
    store = state.worker_store("rendition")
    with patch("src.state.client") as client:
        with pytest.raises(ValueError):
            store.transition("job", expected_statuses={"processing"}, changes={},
                             lease_key=store.lease_key("another-job", "execution"),
                             lease_token="old-token", retention_seconds=60)
    client.assert_not_called()


def test_exact_terminal_bytes_are_retained_and_cannot_be_replaced():
    body = '{"message":"exact café \\n"}\n'.encode()
    terminal = {"terminalBodyBase64": base64.b64encode(body).decode(),
                "terminalSha256": hashlib.sha256(body).hexdigest()}
    db = MagicMock()
    current = {"identityHash": "a" * 64, "status": "processing"}
    db.get.return_value = json.dumps(current)
    db.eval.return_value = 1
    store = state.worker_store("rendition")
    with patch("src.state.client", return_value=db):
        saved = store.transition("job", expected_statuses={"processing"},
                                 changes={**terminal, "status": "callback_pending"},
                                 retention_seconds=60)
        assert base64.b64decode(saved["terminalBodyBase64"]) == body
        db.get.return_value = json.dumps(saved)
        with pytest.raises(state.StateConflict):
            store.transition("job", expected_statuses={"callback_pending"},
                             changes={"terminalSha256": "b" * 64}, retention_seconds=60)


@pytest.mark.parametrize("outcome,error", [(-1, state.StateLeaseLost), (0, state.StateConflict)])
def test_atomic_lease_or_state_conflict_never_reports_success(outcome, error):
    db = MagicMock()
    db.get.return_value = '{"status":"processing"}'
    db.eval.return_value = outcome
    store = state.worker_store("rendition")
    with patch("src.state.client", return_value=db):
        with pytest.raises(error):
            store.transition("job", expected_statuses={"processing"}, changes={},
                             lease_key=store.lease_key("job", "execution"),
                             lease_token=b"stale-owner", retention_seconds=60)


def test_invalid_terminal_integrity_fails_before_redis_write():
    db = MagicMock()
    db.get.return_value = '{"status":"processing"}'
    with patch("src.state.client", return_value=db):
        with pytest.raises(ValueError):
            state.worker_store("rendition").transition(
                "job", expected_statuses={"processing"},
                changes={"terminalBodyBase64": base64.b64encode(b"{}").decode(),
                         "terminalSha256": "0" * 64}, retention_seconds=60)
    db.eval.assert_not_called()
