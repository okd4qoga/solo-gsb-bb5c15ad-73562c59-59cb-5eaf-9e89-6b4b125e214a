"""FastAPI application: differential-privacy budget ledger."""
from __future__ import annotations

from typing import Annotated

from contextlib import asynccontextmanager
from datetime import datetime

from fastapi import FastAPI, Path, Query, Request, status
from fastapi.responses import JSONResponse

from . import db, ledger
from .schemas import (
    AnalysisRequest,
    BudgetTransferRequest,
    BudgetTransferView,
    DatasetInfo,
    DatasetRegister,
    ReleaseForecast,
    RequestList,
    ReservationConfirm,
    ReservationCreate,
    ReservationList,
    ReservationView,
    Verdict,
    BudgetView,
)

DATASET_ID = Annotated[
    str,
    Path(
        min_length=1,
        max_length=200,
        pattern=r"^[A-Za-z0-9._\-]+$",
        description="Client-chosen dataset identifier.",
    ),
]
RESERVATION_ID = Annotated[
    str,
    Path(
        min_length=1,
        max_length=200,
        pattern=r"^[A-Za-z0-9._\-]+$",
        description="Client-chosen reservation identifier.",
    ),
]


@asynccontextmanager
async def lifespan(_: FastAPI):
    await db.init_db()
    yield


app = FastAPI(
    title="Differential-Privacy Budget Ledger",
    version="1.3.0",
    lifespan=lifespan,
    description=(
        "Register epsilon/delta caps per dataset and submit analysis "
        "requests.  Composition is computed on a grid of Rényi-DP orders and "
        "converted back to (epsilon, delta)-DP.  Repeated request_ids are "
        "idempotent; over-budget submissions are atomically rejected under "
        "concurrency.  Budget can additionally be reserved up-front: "
        "reservations lock capacity atomically (with TTL-based expiry), are "
        "idempotent per reservation_id, and are confirmed into analysis "
        "requests or released.  A read-only release forecast simulates future "
        "TTL expiries up to a caller-supplied UTC deadline without changing "
        "any reservation or database state.  Epsilon cap can also be "
        "transferred between two registered datasets in one atomic, "
        "idempotent transaction: the source must stay positive and cover its "
        "current occupancy, the target may never exceed the registered cap "
        "ceiling of 1000, and a rejected attempt leaves both caps unchanged."
    ),
)


@app.exception_handler(db.DatasetNotFound)
async def _not_found(_: Request, exc: db.DatasetNotFound) -> JSONResponse:
    return JSONResponse(
        status_code=status.HTTP_404_NOT_FOUND,
        content={"error": "dataset_not_found", "dataset_id": exc.args[0]},
    )


@app.exception_handler(db.DatasetExists)
async def _exists(_: Request, exc: db.DatasetExists) -> JSONResponse:
    return JSONResponse(
        status_code=status.HTTP_409_CONFLICT,
        content={"error": "dataset_already_exists", "dataset_id": exc.args[0]},
    )


@app.exception_handler(db.RequestConflict)
async def _conflict(_: Request, exc: db.RequestConflict) -> JSONResponse:
    return JSONResponse(
        status_code=status.HTTP_409_CONFLICT,
        content={
            "error": "request_id_conflict",
            "request_id": exc.args[0],
            "detail": (
                "request_id was already used for a different request payload"
            ),
        },
    )


@app.exception_handler(db.ReservationNotFound)
async def _reservation_not_found(_: Request, exc: db.ReservationNotFound) -> JSONResponse:
    return JSONResponse(
        status_code=status.HTTP_404_NOT_FOUND,
        content={"error": "reservation_not_found", "reservation_id": exc.args[0]},
    )


@app.exception_handler(db.ReservationConflict)
async def _reservation_conflict(_: Request, exc: db.ReservationConflict) -> JSONResponse:
    return JSONResponse(
        status_code=status.HTTP_409_CONFLICT,
        content={
            "error": "reservation_id_conflict",
            "reservation_id": exc.args[0],
            "detail": (
                "reservation_id was already used for a different reservation "
                "payload (epsilon/delta/ttl_seconds)"
            ),
        },
    )


@app.exception_handler(db.ReservationStateConflict)
async def _reservation_state_conflict(
    _: Request, exc: db.ReservationStateConflict
) -> JSONResponse:
    return JSONResponse(
        status_code=status.HTTP_409_CONFLICT,
        content={
            "error": exc.args[0],
            "detail": exc.args[1],
        },
    )


@app.exception_handler(ledger.InvalidForecastDeadline)
async def _invalid_forecast_deadline(
    _: Request, exc: ledger.InvalidForecastDeadline
) -> JSONResponse:
    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        content={"error": "invalid_forecast_deadline", "detail": exc.args[0]},
    )


@app.exception_handler(db.TransferConflict)
async def _transfer_conflict(_: Request, exc: db.TransferConflict) -> JSONResponse:
    return JSONResponse(
        status_code=status.HTTP_409_CONFLICT,
        content={
            "error": "transfer_id_conflict",
            "transfer_id": exc.args[0],
            "detail": (
                "transfer_id was already used for a different transfer payload"
            ),
        },
    )


@app.get("/health", tags=["meta"])
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post(
    "/datasets/{dataset_id}",
    response_model=DatasetInfo,
    status_code=status.HTTP_201_CREATED,
    tags=["datasets"],
)
async def register_dataset(dataset_id: DATASET_ID, payload: DatasetRegister) -> dict:
    return await ledger.register_dataset(dataset_id, payload)


@app.get(
    "/datasets/{dataset_id}",
    response_model=DatasetInfo,
    tags=["datasets"],
)
async def get_dataset(dataset_id: DATASET_ID) -> dict:
    return await ledger.get_dataset_info(dataset_id)


@app.get(
    "/datasets/{dataset_id}/budget",
    response_model=BudgetView,
    tags=["datasets"],
)
async def get_budget(dataset_id: DATASET_ID) -> dict:
    info = await ledger.get_dataset_info(dataset_id)
    return info["budget"]


@app.get(
    "/datasets/{dataset_id}/budget/release-forecast",
    response_model=ReleaseForecast,
    tags=["reservations"],
    summary="Simulate future TTL-driven budget releases (read-only)",
)
async def get_release_forecast(
    dataset_id: DATASET_ID,
    deadline: Annotated[
        datetime,
        Query(
            description=(
                "Future UTC instant to forecast up to, as an ISO-8601 "
                "timestamp (e.g. 2026-09-24T00:00:00Z).  Reservations "
                "expiring at or before this instant are simulated as "
                "released; the endpoint never modifies stored state."
            ),
        ),
    ],
) -> dict:
    return await ledger.release_forecast(dataset_id, deadline)


@app.post(
    "/datasets/{dataset_id}/requests",
    response_model=Verdict,
    tags=["requests"],
)
async def submit_request(dataset_id: DATASET_ID, payload: AnalysisRequest):
    verdict, http_status = await ledger.submit_analysis(dataset_id, payload)
    return JSONResponse(status_code=http_status, content=verdict)


@app.get(
    "/datasets/{dataset_id}/requests",
    response_model=RequestList,
    tags=["requests"],
)
async def get_requests(dataset_id: DATASET_ID) -> dict:
    return await ledger.list_requests(dataset_id)


# -------------------------------------------------------------- reservations


@app.post(
    "/datasets/{dataset_id}/reservations",
    response_model=ReservationView,
    tags=["reservations"],
)
async def create_reservation(dataset_id: DATASET_ID, payload: ReservationCreate):
    view, http_status = await ledger.create_reservation(dataset_id, payload)
    return JSONResponse(status_code=http_status, content=view)


@app.get(
    "/datasets/{dataset_id}/reservations",
    response_model=ReservationList,
    tags=["reservations"],
)
async def list_reservations(dataset_id: DATASET_ID) -> dict:
    return await ledger.list_reservations(dataset_id)


@app.get(
    "/datasets/{dataset_id}/reservations/{reservation_id}",
    response_model=ReservationView,
    tags=["reservations"],
)
async def get_reservation(dataset_id: DATASET_ID, reservation_id: RESERVATION_ID) -> dict:
    return await ledger.get_reservation(dataset_id, reservation_id)


@app.post(
    "/datasets/{dataset_id}/reservations/{reservation_id}/confirm",
    response_model=Verdict,
    tags=["reservations"],
)
async def confirm_reservation(
    dataset_id: DATASET_ID, reservation_id: RESERVATION_ID, payload: ReservationConfirm
):
    verdict, http_status = await ledger.confirm_reservation(
        dataset_id, reservation_id, payload
    )
    return JSONResponse(status_code=http_status, content=verdict)


@app.post(
    "/datasets/{dataset_id}/reservations/{reservation_id}/release",
    response_model=ReservationView,
    tags=["reservations"],
)
async def release_reservation(dataset_id: DATASET_ID, reservation_id: RESERVATION_ID):
    view, http_status = await ledger.release_reservation(dataset_id, reservation_id)
    return JSONResponse(status_code=http_status, content=view)


# -------------------------------------------------------------- transfers --


@app.post(
    "/budget-transfers",
    response_model=BudgetTransferView,
    status_code=status.HTTP_201_CREATED,
    tags=["transfers"],
)
async def create_budget_transfer(payload: BudgetTransferRequest):
    view, http_status = await ledger.transfer_budget(payload)
    return JSONResponse(status_code=http_status, content=view)
