"""Shared rendition registration and trusted control destination boundaries."""

from unittest.mock import patch

import pytest

from src.env import settings
from src.security import rendition_control_url, is_rendition_control_url_allowed


JOB = "11111111-1111-4111-8111-111111111111"


def test_control_destinations_come_only_from_operator_base():
    with patch.object(settings, "rendition_api_base_url", "https://api.example.test/service/"):
        assert rendition_control_url(JOB, "callback") == "https://api.example.test/service/webhooks/renditions"
        assert rendition_control_url(JOB, "refresh") == f"https://api.example.test/service/internal/renditions/jobs/{JOB}/urls"
        assert rendition_control_url(JOB, "heartbeat") == f"https://api.example.test/service/internal/renditions/jobs/{JOB}/heartbeat"
        assert is_rendition_control_url_allowed(rendition_control_url(JOB, "callback"), JOB, "callback")
        for destination in (
            "https://other.test/webhooks/renditions",
            "https://api.example.test/service/webhooks/renditions?override=1",
            "https://api.example.test/service/webhooks/renditions/extra",
            "https://api.example.test/service/webhooks/demucs",
        ):
            assert not is_rendition_control_url_allowed(destination, JOB, "callback")
        for job_id in ("../admin", JOB + "/admin", "{" + JOB + "}", "11111111111141118111111111111111"):
            with pytest.raises(ValueError):
                rendition_control_url(job_id, "refresh")
        with pytest.raises(ValueError):
            rendition_control_url(JOB, "admin")


@pytest.mark.parametrize("base", [
    "", "file:///private/api", "https:///missing", "https://user@api.example.test",
    "https://api.example.test?", "https://api.example.test#",
    "https://api.example.test/../admin", "https://api.example.test/./admin",
    "https://api.example.test\\evil", "https://api.example.test/\nadmin",
])
def test_unconfigured_or_ambiguous_base_fails_closed(base):
    with patch.object(settings, "rendition_api_base_url", base):
        assert not is_rendition_control_url_allowed("https://api.example.test/webhooks/renditions", JOB, "callback")
        with pytest.raises(ValueError):
            rendition_control_url(JOB, "callback")


def test_rendition_routes_limit_actual_stream_bytes_before_dispatch():
    from fastapi.testclient import TestClient
    from src.main import app

    client = TestClient(app)
    for route in ("/renditions", f"/renditions/{JOB}/cancel"):
        # Generator input intentionally supplies no Content-Length.
        response = client.post(route, content=iter([b"a" * 32768, b"b" * 32769]))
        assert response.status_code == 413
        assert response.headers["content-type"].startswith("application/problem+json")
        body = response.json()
        assert body["code"] == "RENDITION_BODY_TOO_LARGE"
        assert body["status"] == response.status_code
        assert set(body) == {"type", "title", "status", "detail", "requestId", "code"}


def test_registration_has_no_io_and_uses_separate_bounded_queues(tmp_path):
    from unittest.mock import MagicMock
    from src.queues import celery_app
    from src.rendition_service import rendition_runtime, execute_task, callback_task, recover_task

    with (
        patch("src.state.client", side_effect=AssertionError("registration must not access Redis")),
        patch.object(settings, "rendition_work_root", str(tmp_path / "not-created")),
    ):
        runtime = rendition_runtime()
        assert runtime.work_root == tmp_path / "not-created"
        assert not runtime.work_root.exists()
    prefix = settings.audio_redis_prefix
    routes = celery_app.conf.task_routes
    assert routes[execute_task.name]["queue"] == prefix + "rendition-execution"
    assert routes[callback_task.name]["queue"] == prefix + "rendition-callbacks"
    assert routes[recover_task.name]["queue"] == prefix + "rendition-callbacks"
    assert celery_app.conf.beat_schedule["recover-rendition-jobs"]["schedule"] == 10
    assert execute_task.max_retries == callback_task.max_retries == recover_task.max_retries == 0
    for task in (execute_task, callback_task, recover_task):
        producer = MagicMock()
        with patch.object(
            celery_app.backend, "on_task_call",
            side_effect=AssertionError("publishing must not subscribe to task results"),
        ):
            task.apply_async(args=[] if task is recover_task else [JOB], producer=producer)
        producer.publish.assert_called_once()


def test_signed_routes_reject_authentication_before_any_state_access(tmp_path):
    from fastapi.testclient import TestClient
    from src.main import app
    from src.rendition_service import rendition_runtime

    with (
        patch.object(settings, "rendition_work_root", str(tmp_path / "scratch")),
        patch.object(settings, "demucssvc_token", "local-unit-secret"),
        patch("src.state.client", side_effect=AssertionError("unauthenticated requests must not access state")),
    ):
        runtime = rendition_runtime()
        with patch("src.rendition_service.rendition_runtime", return_value=runtime):
            for route in ("/renditions", f"/renditions/{JOB}/cancel"):
                response = TestClient(app).post(route, content=b'{"action":"enqueue"}')
                assert response.status_code == 401
                assert response.json()["code"] == "RENDITION_SIGNATURE_INVALID"
        assert not runtime.work_root.exists()


def test_private_evidence_selector_binds_digest_without_exposing_capabilities():
    import base64
    import hashlib
    import json
    from pathlib import Path
    from unittest.mock import MagicMock
    from src.rendition_models import identity_hash
    from src.rendition_service import rendition_evidence

    fixture = json.loads((Path(__file__).parents[1] / "track-tree-contracts/fixtures/rendition-protocol.json").read_text())
    case = next(case for case in fixture["cases"] if case["payload"].get("action") == "callback")
    raw = case["rawBody"].encode()
    identity = case["payload"]["identity"]
    current = {
        "identity": identity, "identityHash": identity_hash(identity),
        "status": case["payload"]["status"], "executionAttempts": 1,
        "callbackAttempts": 2, "terminalBodyBase64": base64.b64encode(raw).decode(),
        "terminalSha256": hashlib.sha256(raw).hexdigest(), "delivered": True,
    }
    store = MagicMock()
    store.load.return_value = current
    with patch("src.state.worker_store", return_value=store):
        evidence = rendition_evidence(identity["jobId"])
        assert evidence["terminalSha256"] == hashlib.sha256(raw).hexdigest()
        assert evidence["delivered"] is True
        assert set(evidence) == {
            "jobId", "versionId", "allocationId", "status", "executionAttempts",
            "callbackAttempts", "terminalSha256", "terminalStatus", "delivered",
        }
        serialized = json.dumps(evidence)
        assert identity["callbackUrl"] not in serialized
        assert identity["output"]["fileKey"] not in serialized
        current["terminalSha256"] = "0" * 64
        with pytest.raises(ValueError, match="digest"):
            rendition_evidence(identity["jobId"])


def test_http_disabled_admission_and_cancel_tombstone_share_real_dispatch(tmp_path):
    import json
    from pathlib import Path
    from unittest.mock import MagicMock
    from fastapi.testclient import TestClient
    from src.main import app
    from src.rendition_jobs import RenditionRuntime
    from src.rendition_models import timestamp_ms
    from src.security import get_webhook_headers
    from test_rendition_jobs import DB

    fixture = json.loads((Path(__file__).parents[1] / "track-tree-contracts/fixtures/rendition-protocol.json").read_text())
    cases = {case["name"]: case for case in fixture["cases"]}
    identity = cases["enqueue"]["payload"]["identity"]
    clock = timestamp_ms(identity["createdAt"]) / 1000 + 1
    db = DB()  # Controlled state; this proves route composition, not Redis CAS.
    schedule = MagicMock(side_effect=AssertionError("No work may be scheduled"))
    runtime = RenditionRuntime(tmp_path / "scratch", MagicMock(), schedule, schedule,
                               store=db.store, clock=lambda: clock)
    with (
        patch.object(settings, "demucssvc_token", fixture["secret"]),
        patch.object(settings, "rendition_api_base_url", "https://api.example.test"),
        patch.object(settings, "media_url_origins", ["https://media.example.test"]),
        patch.object(settings, "renditions_enabled", False),
        patch("src.rendition_service.rendition_runtime", return_value=runtime),
    ):
        client = TestClient(app)
        def post(route, case):
            raw = cases[case]["rawBody"].encode()
            return client.post(route, content=raw, headers=get_webhook_headers(raw, str(int(clock))))
        response = post("/renditions", "enqueue")
        assert response.status_code == 503
        assert response.json()["code"] == "RENDITION_SERVICE_UNAVAILABLE"
        assert not db.store.rows
        response = post(f"/renditions/{identity['jobId']}/cancel", "cancel")
        assert response.status_code == 200
        assert db.store.load(identity["jobId"])["status"] == "canceled"
        with patch.object(settings, "renditions_enabled", True):
            response = post("/renditions", "enqueue")
        assert response.status_code == 409
        assert response.json()["code"] == "RENDITION_CANCELED"
        assert not runtime.work_root.exists()
