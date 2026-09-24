"""End-to-end tests for authorised-request reversals.

POST /datasets/{dataset_id}/requests/{request_id}/reversals stores an
immutable reversal record (global ``reversal_id`` + reason) inside one SQLite
write transaction; the reversed request stops occupying budget everywhere
while its original record (and any confirmed reservation behind it) stays
untouched.
"""
from __future__ import annotations

import asyncio
import dataclasses
import itertools
import os
import tempfile
from datetime import datetime, timedelta, timezone

import aiosqlite
import pytest
import pytest_asyncio

# Point the ledger at an isolated database before importing the app.
_tmp = tempfile.mkdtemp(prefix="dpledger-test-rev-")
os.environ["DATABASE_PATH"] = os.path.join(_tmp, "reversals.db")

from httpx import ASGITransport, AsyncClient  # noqa: E402

from app import db, rdp  # noqa: E402
from app.config import settings  # noqa: E402
from app.main import app  # noqa: E402

_counter = itertools.count()
_reversal_counter = itertools.count()


def _new_dataset_id(name: str) -> str:
    return f"dsrev-{name}-{next(_counter)}"


def _rev_id(tag: str) -> str:
    # reversal_id is a *global* idempotency key, so every test (and every
    # reversal within a test) needs a fresh one against the shared test DB.
    return f"rev-{tag}-{next(_reversal_counter)}"


@pytest_asyncio.fixture()
async def client():
    await db.init_db()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac


async def _register(client, dataset_id: str, eps: float = 1.0, delta: float = 1e-6):
    return await client.post(
        f"/datasets/{dataset_id}",
        json={"epsilon_cap": eps, "delta_cap": delta,
              "description": "test dataset"},
    )


async def _submit(client, dataset_id: str, request_id: str,
                  epsilon: float = 0.1, delta: float = 1e-7,
                  query_type: str = "q"):
    return await client.post(
        f"/datasets/{dataset_id}/requests",
        json={"request_id": request_id, "query_type": query_type,
              "epsilon": epsilon, "delta": delta},
    )


async def _reverse(client, dataset_id: str, request_id: str,
                   reversal_id: str, reason: str = "no longer needed"):
    return await client.post(
        f"/datasets/{dataset_id}/requests/{request_id}/reversals",
        json={"reversal_id": reversal_id, "reason": reason},
    )


async def _budget(client, dataset_id: str) -> dict:
    r = await client.get(f"/datasets/{dataset_id}/budget")
    assert r.status_code == 200
    return r.json()


# ------------------------------------------------------------- basic flows --


@pytest.mark.asyncio
async def test_reverse_granted_request_releases_budget(client):
    ds = _new_dataset_id("basic")
    await _register(client, ds, eps=1.0, delta=1e-6)
    verdict = (await _submit(client, ds, "r1", epsilon=0.2, delta=1e-8)).json()
    assert verdict["allowed"] is True
    before = await _budget(client, ds)
    assert before["epsilon_consumed"] > 0.0

    rid = _rev_id("basic")
    r = await _reverse(client, ds, "r1", rid, "duplicate submission")
    assert r.status_code == 201
    body = r.json()
    assert body["reversal_id"] == rid
    assert body["dataset_id"] == ds
    assert body["request_id"] == "r1"
    assert body["reason"] == "duplicate submission"
    assert body["epsilon"] == pytest.approx(0.2)
    assert body["delta"] == pytest.approx(1e-8)
    assert body["idempotent_replay"] is False
    assert body["epsilon_committed_before"] == pytest.approx(
        before["epsilon_consumed"], abs=1e-9
    )
    assert body["epsilon_committed_after"] == pytest.approx(0.0, abs=1e-12)
    assert body["epsilon_remaining"] == pytest.approx(1.0, abs=1e-9)
    assert body["created_at"] is not None

    # Budget view reflects the release immediately.
    after = await _budget(client, ds)
    assert after["epsilon_consumed"] == pytest.approx(0.0, abs=1e-12)
    assert after["epsilon_remaining"] == pytest.approx(1.0, abs=1e-9)
    assert after["granted_requests"] == 0

    # The original request record is unchanged, only marked as reversed.
    listing = (await client.get(f"/datasets/{ds}/requests")).json()
    assert listing["count"] == 1
    rec = listing["requests"][0]
    assert rec["request_id"] == "r1"
    assert rec["status"] == "granted"
    assert rec["epsilon_after"] == pytest.approx(verdict["epsilon_after"])
    assert rec["reversed"] is True
    assert rec["reversal_id"] == rid


@pytest.mark.asyncio
async def test_unreversed_requests_are_not_marked(client):
    ds = _new_dataset_id("unmarked")
    await _register(client, ds)
    await _submit(client, ds, "keep", epsilon=0.1)
    await _submit(client, ds, "gone", epsilon=0.1)
    rid = _rev_id("gone")
    assert (await _reverse(client, ds, "gone", rid)).status_code == 201

    listing = (await client.get(f"/datasets/{ds}/requests")).json()
    by_id = {r["request_id"]: r for r in listing["requests"]}
    assert by_id["keep"]["reversed"] is False
    assert by_id["keep"]["reversal_id"] is None
    assert by_id["gone"]["reversed"] is True
    assert by_id["gone"]["reversal_id"] == rid


@pytest.mark.asyncio
async def test_reverse_rejected_request_conflicts(client):
    ds = _new_dataset_id("rej")
    await _register(client, ds, eps=0.01, delta=1e-6)
    verdict = (await _submit(client, ds, "big", epsilon=0.5)).json()
    assert verdict["status"] == "rejected"

    r = await _reverse(client, ds, "big", _rev_id("rej"))
    assert r.status_code == 409
    assert r.json()["error"] == "request_rejected"
    # Nothing was recorded.
    assert (await client.get(f"/datasets/{ds}/reversals")).json()["count"] == 0


@pytest.mark.asyncio
async def test_reverse_twice_with_new_id_conflicts(client):
    ds = _new_dataset_id("twice")
    await _register(client, ds)
    await _submit(client, ds, "r1", epsilon=0.1)
    rid = _rev_id("twice")
    assert (await _reverse(client, ds, "r1", rid)).status_code == 201

    again = await _reverse(client, ds, "r1", _rev_id("twice-2"), "second attempt")
    assert again.status_code == 409
    assert again.json()["error"] == "request_already_reversed"
    # Still exactly one immutable record; budget released exactly once.
    listing = (await client.get(f"/datasets/{ds}/reversals")).json()
    assert listing["count"] == 1
    assert listing["reversals"][0]["reversal_id"] == rid
    assert (await _budget(client, ds))["epsilon_consumed"] == pytest.approx(0.0)


@pytest.mark.asyncio
async def test_reverse_missing_request_and_dataset_404(client):
    ds = _new_dataset_id("missing")
    await _register(client, ds)

    r = await _reverse(client, ds, "nope", _rev_id("missing"))
    assert r.status_code == 404
    assert r.json()["error"] == "request_not_found"

    r2 = await _reverse(client, "no-such-dataset", "nope", _rev_id("missing-ds"))
    assert r2.status_code == 404
    assert r2.json()["error"] == "dataset_not_found"


@pytest.mark.asyncio
async def test_reversal_payload_validation(client):
    ds = _new_dataset_id("valid")
    await _register(client, ds)
    await _submit(client, ds, "r1", epsilon=0.1)

    missing_reason = await client.post(
        f"/datasets/{ds}/requests/r1/reversals",
        json={"reversal_id": _rev_id("valid")},
    )
    assert missing_reason.status_code == 422
    empty_reason = await _reverse(client, ds, "r1", _rev_id("valid"), reason="")
    assert empty_reason.status_code == 422
    bad_id = await _reverse(client, ds, "r1", "rev 1/with spaces")
    assert bad_id.status_code == 422
    # Failed validations recorded nothing.
    assert (await client.get(f"/datasets/{ds}/reversals")).json()["count"] == 0


# -------------------------------------------------------------- idempotency --


@pytest.mark.asyncio
async def test_reversal_replay_is_idempotent(client):
    ds = _new_dataset_id("idem")
    await _register(client, ds)
    await _submit(client, ds, "r1", epsilon=0.2, delta=1e-8)
    rid = _rev_id("idem")

    first = await _reverse(client, ds, "r1", rid, "duplicate")
    assert first.status_code == 201
    assert first.json()["idempotent_replay"] is False

    for _ in range(3):
        again = await _reverse(client, ds, "r1", rid, "duplicate")
        assert again.status_code == 200
        body = again.json()
        assert body["idempotent_replay"] is True
        assert body["created_at"] == first.json()["created_at"]
        assert body["epsilon_committed_after"] == pytest.approx(
            first.json()["epsilon_committed_after"]
        )

    # Exactly one record; the budget was released exactly once.
    assert (await client.get(f"/datasets/{ds}/reversals")).json()["count"] == 1
    assert (await _budget(client, ds))["epsilon_consumed"] == pytest.approx(0.0)


@pytest.mark.asyncio
async def test_same_reversal_id_different_payload_conflicts(client):
    ds = _new_dataset_id("idemconf")
    await _register(client, ds)
    await _submit(client, ds, "r1", epsilon=0.1)
    await _submit(client, ds, "r2", epsilon=0.1)
    rid = _rev_id("idemconf")
    assert (await _reverse(client, ds, "r1", rid, "reason-a")).status_code == 201

    # Same reversal_id, different reason.
    r = await _reverse(client, ds, "r1", rid, "reason-b")
    assert r.status_code == 409
    assert r.json()["error"] == "reversal_id_conflict"
    # Same reversal_id, different target request.
    r2 = await _reverse(client, ds, "r2", rid, "reason-a")
    assert r2.status_code == 409
    assert r2.json()["error"] == "reversal_id_conflict"
    # Same reversal_id, different dataset.
    other = _new_dataset_id("idemconf-other")
    await _register(client, other)
    await _submit(client, other, "r9", epsilon=0.1)
    r3 = await _reverse(client, other, "r9", rid, "reason-a")
    assert r3.status_code == 409
    assert r3.json()["error"] == "reversal_id_conflict"

    # r2 was never reversed by the conflicting attempts.
    assert (await client.get(f"/datasets/{ds}/reversals")).json()["count"] == 1
    budget = await _budget(client, ds)
    assert budget["granted_requests"] == 1


@pytest.mark.asyncio
async def test_original_request_replay_after_reversal_does_not_recharge(client):
    ds = _new_dataset_id("replay")
    await _register(client, ds, eps=1.0, delta=1e-6)
    payload = {"request_id": "r1", "query_type": "histogram",
               "epsilon": 0.3, "delta": 1e-8}
    first = await client.post(f"/datasets/{ds}/requests", json=payload)
    assert first.status_code == 201

    assert (await _reverse(client, ds, "r1", _rev_id("replay"))).status_code == 201
    assert (await _budget(client, ds))["epsilon_consumed"] == pytest.approx(0.0)

    # Re-submitting the original request_id with the same payload replays the
    # original verdict; it must NOT re-occupy the released budget.
    replay = await client.post(f"/datasets/{ds}/requests", json=payload)
    assert replay.status_code == 200
    body = replay.json()
    assert body["idempotent_replay"] is True
    assert body["allowed"] is True
    assert body["status"] == "granted"
    assert body["epsilon_after"] == first.json()["epsilon_after"]

    budget = await _budget(client, ds)
    assert budget["epsilon_consumed"] == pytest.approx(0.0, abs=1e-12)
    assert budget["granted_requests"] == 0
    assert (await client.get(f"/datasets/{ds}/requests")).json()["count"] == 1


# -------------------------------------------------- budget accounting paths --


@pytest.mark.asyncio
async def test_reversal_frees_budget_for_new_requests(client):
    ds = _new_dataset_id("frees")
    # Pure epsilon-DP: composition is the exact sum, easy to reason about.
    await _register(client, ds, eps=1.0, delta=1e-12)
    a = await _submit(client, ds, "a", epsilon=0.8, delta=0.0)
    assert a.json()["allowed"] is True
    blocked = await _submit(client, ds, "b", epsilon=0.5, delta=0.0)
    assert blocked.json()["allowed"] is False

    assert (await _reverse(client, ds, "a", _rev_id("frees"))).status_code == 201
    budget = await _budget(client, ds)
    assert budget["epsilon_remaining"] == pytest.approx(1.0, abs=1e-9)

    ok = await _submit(client, ds, "c", epsilon=0.5, delta=0.0)
    assert ok.status_code == 201
    assert ok.json()["allowed"] is True
    budget = await _budget(client, ds)
    assert budget["epsilon_consumed"] == pytest.approx(0.5, abs=1e-9)
    # The rejected record "b" is untouched and still replays as rejected.
    replay_b = await _submit(client, ds, "b", epsilon=0.5, delta=0.0)
    assert replay_b.status_code == 200
    assert replay_b.json()["allowed"] is False


@pytest.mark.asyncio
async def test_active_reservations_still_count_after_reversal(client):
    ds = _new_dataset_id("reskeep")
    await _register(client, ds, eps=1.0, delta=1e-6)
    await _submit(client, ds, "r1", epsilon=0.2, delta=1e-8)
    rv = await client.post(
        f"/datasets/{ds}/reservations",
        json={"reservation_id": "rv1", "epsilon": 0.3, "delta": 1e-8,
              "ttl_seconds": 3600},
    )
    assert rv.json()["allowed"] is True
    before = await _budget(client, ds)
    assert before["epsilon_reserved"] > 0.0
    # Independent RDP accounting of the reservation alone (what must remain
    # committed once the granted request leaves the composition).
    _, res_alone, _ = rdp.evaluate_entries([(0.3, 1e-8)], 1e-6)
    res_alone = max(0.0, res_alone)

    r = await _reverse(client, ds, "r1", _rev_id("reskeep"))
    assert r.status_code == 201
    body = r.json()
    # The reservation's locked epsilon stays committed after the reversal.
    assert body["epsilon_committed_after"] == pytest.approx(res_alone, abs=1e-9)
    assert body["epsilon_committed_before"] == pytest.approx(
        before["epsilon_consumed"] + before["epsilon_reserved"], abs=1e-9
    )
    assert body["epsilon_committed_before"] > body["epsilon_committed_after"]

    budget = await _budget(client, ds)
    assert budget["epsilon_consumed"] == pytest.approx(0.0, abs=1e-12)
    assert budget["epsilon_reserved"] == pytest.approx(res_alone, abs=1e-9)
    assert budget["active_reservations"] == 1
    assert budget["epsilon_remaining"] == pytest.approx(
        1.0 - res_alone, abs=1e-9
    )


@pytest.mark.asyncio
async def test_reversal_frees_budget_for_reservation_admission(client):
    ds = _new_dataset_id("resadmit")
    await _register(client, ds, eps=1.0, delta=1e-12)
    await _submit(client, ds, "r1", epsilon=0.8, delta=0.0)
    blocked = await client.post(
        f"/datasets/{ds}/reservations",
        json={"reservation_id": "rv1", "epsilon": 0.5, "delta": 0.0,
              "ttl_seconds": 600},
    )
    assert blocked.json()["allowed"] is False

    assert (await _reverse(client, ds, "r1", _rev_id("resadmit"))).status_code == 201
    ok = await client.post(
        f"/datasets/{ds}/reservations",
        json={"reservation_id": "rv2", "epsilon": 0.5, "delta": 0.0,
              "ttl_seconds": 600},
    )
    assert ok.status_code == 201
    assert ok.json()["allowed"] is True


@pytest.mark.asyncio
async def test_reversal_reflected_in_release_forecast(client):
    ds = _new_dataset_id("forecast")
    await _register(client, ds, eps=1.0, delta=1e-6)
    await _submit(client, ds, "r1", epsilon=0.2, delta=1e-8)
    await client.post(
        f"/datasets/{ds}/reservations",
        json={"reservation_id": "rv1", "epsilon": 0.3, "delta": 1e-8,
              "ttl_seconds": 3600},
    )
    assert (await _reverse(client, ds, "r1", _rev_id("forecast"))).status_code == 201

    deadline = (datetime.now(timezone.utc) + timedelta(hours=2)).isoformat()
    fc = await client.get(
        f"/datasets/{ds}/budget/release-forecast",
        params={"deadline": deadline},
    )
    assert fc.status_code == 200
    body = fc.json()
    # The reversed request is gone from the composition; the still-valid
    # reservation keeps counting at every simulated instant.
    assert body["initial"]["epsilon_consumed"] == pytest.approx(0.0, abs=1e-12)
    assert body["initial"]["epsilon_reserved"] > 0.0
    assert body["initial"]["active_reservations"] == 1
    _, expected_reserved, _ = rdp.evaluate_entries([(0.3, 1e-8)], 1e-6)
    assert body["initial"]["epsilon_reserved"] == pytest.approx(
        max(0.0, expected_reserved), abs=1e-9
    )
    assert body["at_deadline"]["epsilon_consumed"] == pytest.approx(0.0, abs=1e-12)


@pytest.mark.asyncio
async def test_reversal_reflected_in_transfer_occupancy(client):
    src = _new_dataset_id("xfer-src")
    dst = _new_dataset_id("xfer-dst")
    await _register(client, src, eps=1.0, delta=1e-12)
    await _register(client, dst, eps=0.5, delta=1e-12)
    # Pure-DP request: source commits exactly 0.8.
    await _submit(client, src, "r1", epsilon=0.8, delta=0.0)

    # Moving 0.5 away would leave cap 0.5 < committed 0.8: rejected.
    blocked = await client.post(
        "/budget-transfers",
        json={"transfer_id": _rev_id("xfer-blocked"), "source_dataset_id": src,
              "target_dataset_id": dst, "epsilon_amount": 0.5},
    )
    assert blocked.json()["allowed"] is False

    assert (await _reverse(client, src, "r1", _rev_id("xfer"))).status_code == 201
    # After the reversal the source occupancy is zero, so the same transfer
    # (new transfer_id) now succeeds and reports the released occupancy.
    ok = await client.post(
        "/budget-transfers",
        json={"transfer_id": _rev_id("xfer-ok"), "source_dataset_id": src,
              "target_dataset_id": dst, "epsilon_amount": 0.5},
    )
    assert ok.status_code == 201
    body = ok.json()
    assert body["allowed"] is True
    assert body["source"]["epsilon_committed"] == pytest.approx(0.0, abs=1e-12)
    assert body["source"]["epsilon_cap"] == pytest.approx(0.5, abs=1e-12)
    assert (await _budget(client, src))["epsilon_cap"] == pytest.approx(0.5)


@pytest.mark.asyncio
async def test_reverse_confirmed_reservation_request_keeps_history(client):
    ds = _new_dataset_id("confirmhist")
    await _register(client, ds, eps=1.0, delta=1e-6)
    await client.post(
        f"/datasets/{ds}/reservations",
        json={"reservation_id": "rv1", "epsilon": 0.2, "delta": 1e-8,
              "ttl_seconds": 3600},
    )
    verdict = await client.post(
        f"/datasets/{ds}/reservations/rv1/confirm",
        json={"request_id": "req-1", "query_type": "gaussian_mean"},
    )
    assert verdict.status_code == 201

    rid = _rev_id("confirmhist")
    r = await _reverse(client, ds, "req-1", rid, "analysis withdrawn")
    assert r.status_code == 201
    assert r.json()["epsilon"] == pytest.approx(0.2)

    # The confirmed reservation's history record is untouched.
    res = (await client.get(f"/datasets/{ds}/reservations/rv1")).json()
    assert res["status"] == "confirmed"
    assert res["confirmed_request_id"] == "req-1"
    assert res["confirmed_at"] is not None

    # The budget is released and the request list marks the reversal.
    assert (await _budget(client, ds))["epsilon_consumed"] == pytest.approx(0.0)
    rec = (await client.get(f"/datasets/{ds}/requests")).json()["requests"][0]
    assert rec["reversed"] is True
    assert rec["reversal_id"] == rid


# --------------------------------------------------------------- queries ----


@pytest.mark.asyncio
async def test_reversal_query_endpoints(client):
    ds = _new_dataset_id("query")
    await _register(client, ds)
    await _submit(client, ds, "r1", epsilon=0.1)
    await _submit(client, ds, "r2", epsilon=0.2)
    rid1, rid2 = _rev_id("query-1"), _rev_id("query-2")
    await _reverse(client, ds, "r1", rid1, "first")
    await _reverse(client, ds, "r2", rid2, "second")

    listing = (await client.get(f"/datasets/{ds}/reversals")).json()
    assert listing["dataset_id"] == ds
    assert listing["count"] == 2
    assert [r["reversal_id"] for r in listing["reversals"]] == [rid1, rid2]
    assert all(r["idempotent_replay"] is False for r in listing["reversals"])
    assert listing["reversals"][0]["reason"] == "first"

    one = await client.get(f"/datasets/{ds}/reversals/{rid2}")
    assert one.status_code == 200
    assert one.json()["request_id"] == "r2"
    assert one.json()["reason"] == "second"

    missing = await client.get(f"/datasets/{ds}/reversals/rev-nope")
    assert missing.status_code == 404
    assert missing.json()["error"] == "reversal_not_found"

    # A reversal_id is global, but the record is scoped to its dataset.
    other = _new_dataset_id("query-other")
    await _register(client, other)
    foreign = await client.get(f"/datasets/{other}/reversals/{rid1}")
    assert foreign.status_code == 404
    assert foreign.json()["error"] == "reversal_not_found"

    nodataset = await client.get("/datasets/no-such/reversals")
    assert nodataset.status_code == 404
    assert nodataset.json()["error"] == "dataset_not_found"


# -------------------------------------------------------------- concurrency --


@pytest.mark.asyncio
async def test_concurrent_reversals_of_same_request_are_serialised(client):
    ds = _new_dataset_id("conc")
    await _register(client, ds, eps=1.0, delta=1e-6)
    await _submit(client, ds, "r1", epsilon=0.2, delta=1e-8)

    n = 15
    results = await asyncio.gather(*[
        _reverse(client, ds, "r1", _rev_id(f"conc-{i:02d}"), f"attempt {i}")
        for i in range(n)
    ])
    created = [r for r in results if r.status_code == 201]
    conflicts = [r for r in results if r.status_code == 409]
    assert len(created) == 1
    assert len(conflicts) == n - 1
    assert all(
        r.json()["error"] == "request_already_reversed" for r in conflicts
    )

    assert (await client.get(f"/datasets/{ds}/reversals")).json()["count"] == 1
    assert (await _budget(client, ds))["epsilon_consumed"] == pytest.approx(0.0)


@pytest.mark.asyncio
async def test_concurrent_same_reversal_id_replays(client):
    ds = _new_dataset_id("concidem")
    await _register(client, ds, eps=1.0, delta=1e-6)
    await _submit(client, ds, "r1", epsilon=0.2, delta=1e-8)
    rid = _rev_id("concidem")

    n = 15
    results = await asyncio.gather(*[
        _reverse(client, ds, "r1", rid, "duplicate")
        for _ in range(n)
    ])
    created = [r for r in results if r.status_code == 201]
    replays = [r for r in results if r.status_code == 200]
    assert len(created) == 1
    assert len(replays) == n - 1
    assert (await client.get(f"/datasets/{ds}/reversals")).json()["count"] == 1
    assert (await _budget(client, ds))["epsilon_consumed"] == pytest.approx(0.0)


@pytest.mark.asyncio
async def test_reversal_races_with_new_submissions_within_cap(client):
    ds = _new_dataset_id("concmix")
    cap_eps, cap_delta = 1.0, 1e-6
    await _register(client, ds, eps=cap_eps, delta=cap_delta)
    await _submit(client, ds, "old", epsilon=0.4, delta=1e-8)

    async def submit(i):
        return await _submit(client, ds, f"new-{i:02d}",
                             epsilon=0.05, delta=1e-8)

    results = await asyncio.gather(
        _reverse(client, ds, "old", _rev_id("concmix"), "superseded"),
        *[submit(i) for i in range(30)],
    )
    assert results[0].status_code == 201
    assert all(r.status_code in (200, 201) for r in results[1:])

    # Independent accounting: every granted, non-reversed request must fit.
    listing = (await client.get(f"/datasets/{ds}/requests")).json()
    live = [
        (r["epsilon"], r["delta"]) for r in listing["requests"]
        if r["status"] == "granted" and not r["reversed"]
    ]
    consumed, _ = rdp.composed_epsilon(
        [rdp.single_rdp_curve(e, d) for e, d in live], cap_delta
    )
    assert consumed <= cap_eps + 1e-12
    budget = await _budget(client, ds)
    assert budget["epsilon_consumed"] == pytest.approx(consumed, abs=1e-9)
    assert budget["epsilon_remaining"] >= -1e-9


# ---------------------------------------------------------------- migration --


@pytest.mark.asyncio
async def test_migration_from_v3_schema(tmp_path, monkeypatch):
    old_db_path = str(tmp_path / "old.db")

    # Build a database with the pre-reversal schema and some data.
    conn = await aiosqlite.connect(old_db_path)
    try:
        await conn.executescript(db.SCHEMA_V1)
        await conn.executescript(db.SCHEMA_V2)
        await conn.executescript(db.SCHEMA_V3)
        await conn.execute("PRAGMA user_version = 3")
        await conn.execute(
            "INSERT INTO datasets "
            "(dataset_id, epsilon_cap, delta_cap, description, created_at) "
            "VALUES ('legacy', 1.0, 1e-6, 'old', '2026-01-01T00:00:00+00:00')"
        )
        await conn.execute(
            "INSERT INTO requests "
            "(dataset_id, request_id, query_type, epsilon, delta, status, "
            "reason, epsilon_before, epsilon_after, rdp_best_order, created_at) "
            "VALUES ('legacy', 'r1', 'q', 0.1, 0.0, 'granted', NULL, "
            "0.0, 0.1, 1.1, '2026-01-01T00:00:01+00:00')"
        )
        await conn.commit()
    finally:
        await conn.close()

    patched = dataclasses.replace(settings, db_path=old_db_path)
    monkeypatch.setattr(db, "settings", patched)
    await db.init_db()

    async with db.connect() as c:
        async with c.execute("PRAGMA user_version") as cur:
            version = (await cur.fetchone())[0]
        assert version == db.LATEST_VERSION
        async with c.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='reversals'"
        ) as cur:
            assert await cur.fetchone() is not None
        # Pre-existing data survives; without reversals the granted entry
        # still occupies budget exactly as before.
        entries = await db.list_granted_entries(c, "legacy")
        assert entries == [(0.1, 0.0)]
        assert await db.list_reversals(c, "legacy") == []
