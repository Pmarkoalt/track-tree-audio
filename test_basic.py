"""Contract/security unit tests. Real inference evidence is recorded separately."""

import json
import io
import time
from pathlib import Path
from unittest.mock import patch
import pytest
from fastapi.testclient import TestClient
from src.models import SplitRequest, WebhookPayload
from src.security import (
    create_hmac_signature,
    verify_hmac_signature,
    is_webhook_url_allowed,
    is_media_url_allowed,
    get_webhook_headers,
)
from src.env import settings
from src.main import app


def test_enqueue_does_not_register_a_celery_result_subscription():
    from unittest.mock import MagicMock
    from celery.backends.base import DisabledBackend
    from src.queues import celery_app
    from src.demucs_runner import process_audio_split

    producer = MagicMock()
    with patch.object(
        celery_app.backend,
        "on_task_call",
        side_effect=AssertionError("Enqueue must not subscribe to Celery results"),
    ) as subscribe:
        result = process_audio_split.apply_async(
            args=["no-result-subscription"],
            task_id="inference:no-result-subscription",
            producer=producer,
        )
    assert isinstance(result.backend, DisabledBackend)
    subscribe.assert_not_called()
    producer.publish.assert_called_once()


def test_checkpoint_rejects_substitution_before_model_deserialization(tmp_path):
    from src.checkpoint import copy_verified, verified_model_repo, CHECKPOINT_NAME

    output = tmp_path / "untrusted.th"
    with pytest.raises(ValueError, match="reviewed SHA256"):
        copy_verified(io.BytesIO(b"malicious replacement checkpoint"), output)
    assert not output.exists()
    legacy = tmp_path / CHECKPOINT_NAME
    legacy.write_bytes(b"wrong cached weights")
    with patch("src.checkpoint.Path.home", return_value=tmp_path):
        with pytest.raises(ValueError, match="reviewed SHA256"):
            with verified_model_repo(legacy):
                raise AssertionError("unverified weights must never reach inference")


FIXTURE = (
    Path(__file__).parents[1]
    / "track-tree-contracts"
    / "fixtures"
    / "audio-protocol.json"
)


def test_cross_language_exact_raw_body_goldens():
    fixture = json.loads(FIXTURE.read_text())
    for case in fixture["cases"]:
        raw = case["rawBody"].encode("utf-8")
        assert (
            "sha256="
            + create_hmac_signature(raw, fixture["timestamp"], fixture["secret"])
            == case["signature"]
        )
        assert verify_hmac_signature(
            raw,
            case["signature"],
            fixture["timestamp"],
            secret=fixture["secret"],
            now=int(fixture["timestamp"]),
        )
        assert not verify_hmac_signature(
            raw + b" ",
            case["signature"],
            fixture["timestamp"],
            secret=fixture["secret"],
            now=int(fixture["timestamp"]),
        )
        assert not verify_hmac_signature(
            raw,
            case["signature"],
            fixture["timestamp"],
            secret=fixture["secret"],
            now=int(fixture["timestamp"]) + 301,
        )
        assert not verify_hmac_signature(
            raw,
            "sha256=abc",
            fixture["timestamp"],
            secret=fixture["secret"],
            now=int(fixture["timestamp"]),
        )
        model = SplitRequest if case["name"] == "request" else WebhookPayload
        parsed = model.model_validate_json(raw)
        assert parsed.model_dump(mode="json", exclude_none=True) == case["payload"]


def test_strict_destinations_and_model_contract():
    with (
        patch.object(
            settings,
            "api_webhook_url_allowlist",
            ["https://api.example.test/webhooks/demucs"],
        ),
        patch.object(settings, "media_url_origins", ["https://media.example.test"]),
    ):
        assert is_webhook_url_allowed("https://api.example.test/webhooks/demucs")
        assert not is_webhook_url_allowed(
            "https://api.example.test/webhooks/demucs/evil"
        )
        assert not is_webhook_url_allowed(
            "https://api.example.test/webhooks/demucs?extra=1"
        )
        assert is_media_url_allowed(
            "https://media.example.test/object?signature=public-test"
        )
        assert not is_media_url_allowed("http://localhost:1234/admin")
        assert not is_media_url_allowed("https://user@media.example.test/object")
    data = json.loads(FIXTURE.read_text())["cases"][0]["payload"]
    with pytest.raises(ValueError):
        SplitRequest.model_validate({**data, "file_key": "legacy"})


def test_health_is_lightweight_and_signed_enqueue_checks_raw_bytes():
    client = TestClient(app)
    assert client.get("/healthz").json() == {"status": "ok"}
    raw = json.loads(FIXTURE.read_text())["cases"][0]["rawBody"].encode()
    with patch.object(settings, "demucssvc_token", "test-secret"):
        assert (
            client.post(
                "/split",
                content=raw,
                headers={
                    "X-Signature": "sha256=bad",
                    "X-Timestamp": str(int(time.time())),
                },
            ).status_code
            == 401
        )
        valid = get_webhook_headers(raw)
        assert (
            client.post("/split", content=raw + b" ", headers=valid).status_code == 401
        )
        # Approved destinations are explicit; a correctly signed arbitrary URL is still forbidden.
        with (
            patch.object(settings, "api_webhook_url_allowlist", []),
            patch.object(settings, "media_url_origins", []),
        ):
            assert client.post("/split", content=raw, headers=valid).status_code == 400


def test_callback_delivery_retries_persisted_payload_without_inference():
    from src.demucs_runner import deliver_callback

    fixture = json.loads(FIXTURE.read_text())
    payload = fixture["cases"][1]["payload"]
    request = fixture["cases"][0]["payload"]
    current = {
        "status": "callback_pending",
        "callback": payload,
        "request": request,
        "callbackAttempts": 0,
    }

    def update(job_id, **values):
        current.update(values)
        return current

    response = type(
        "Response", (), {"status_code": 200, "raise_for_status": lambda self: None}
    )()
    with (
        patch("src.demucs_runner.state.load", return_value=current),
        patch("src.demucs_runner.state.patch", side_effect=update),
        patch.object(settings, "api_webhook_url_allowlist", [request["callbackUrl"]]),
        patch("src.demucs_runner.requests.post", side_effect=RuntimeError("offline")),
        patch.object(
            deliver_callback,
            "retry",
            side_effect=RuntimeError("scheduled callback retry"),
        ),
    ):
        with pytest.raises(RuntimeError, match="scheduled callback retry"):
            deliver_callback.run(request["jobId"])
        assert (
            current["status"] == "callback_pending" and current["callbackAttempts"] == 1
        )
    with (
        patch("src.demucs_runner.state.load", return_value=current),
        patch("src.demucs_runner.state.patch", side_effect=update),
        patch.object(settings, "api_webhook_url_allowlist", [request["callbackUrl"]]),
        patch("src.demucs_runner.requests.post", return_value=response),
        patch(
            "src.demucs_runner.infer_stems",
            side_effect=AssertionError("inference must never rerun"),
        ),
    ):
        deliver_callback.run(request["jobId"])
        assert current["status"] == "completed"
