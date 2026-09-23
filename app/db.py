"""SQLite persistence layer.

Concurrency model
-----------------
* The database runs in WAL mode with a busy timeout so readers never block
  writers and concurrent writers queue instead of failing immediately.
* Every state-changing operation takes the process-wide ``write_lock``
  (an asyncio.Lock) and opens an explicit ``BEGIN IMMEDIATE`` transaction.
  The check (does the request fit / does request_id already exist?) and the
  insert therefore happen in one serialised, atomic critical section.
* Each event-loop task gets its own aiosqlite connection; nothing is shared
  across threads.

Schema migrations
-----------------
A ``PRAGMA user_version`` integer selects the next migration to apply.
Each migration is an idempotent DDL script wrapped in one transaction, so
both fresh databases and old ledgers created before reservations existed
arrive at the latest schema on startup.
"""
from __future__ import annotations

import os
from contextlib import asynccontextmanager
from typing import AsyncIterator, Optional

import aiosqlite

from .config import settings

# ----------------------------------------------------------------- schema --

SCHEMA_V1 = """
CREATE TABLE IF NOT EXISTS datasets (
    dataset_id    TEXT PRIMARY KEY,
    epsilon_cap   REAL NOT NULL,
    delta_cap     REAL NOT NULL,
    description   TEXT,
    created_at    TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS requests (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    dataset_id     TEXT NOT NULL REFERENCES datasets(dataset_id),
    request_id     TEXT NOT NULL,
    query_type     TEXT NOT NULL,
    epsilon        REAL NOT NULL,
    delta          REAL NOT NULL,
    status         TEXT NOT NULL CHECK (status IN ('granted', 'rejected')),
    reason         TEXT,
    epsilon_before REAL NOT NULL,
    epsilon_after  REAL NOT NULL,
    rdp_best_order REAL NOT NULL,
    created_at     TEXT NOT NULL,
    UNIQUE (dataset_id, request_id)
);

CREATE INDEX IF NOT EXISTS idx_requests_dataset ON requests (dataset_id, id);
"""

# Version 2: budget reservations.  A reservation locks epsilon/delta budget
# until expires_at; "granted" rows are the only ones that occupy budget, and
# only while now < expires_at.  Confirmation turns a granted reservation into
# a regular granted analysis request in ``requests``; release / expiry free
# the locked budget again.
SCHEMA_V2 = """
CREATE TABLE IF NOT EXISTS reservations (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    dataset_id      TEXT NOT NULL REFERENCES datasets(dataset_id),
    reservation_id  TEXT NOT NULL,
    epsilon         REAL NOT NULL,
    delta           REAL NOT NULL,
    ttl_seconds     INTEGER NOT NULL,
    status          TEXT NOT NULL CHECK (status IN (
                        'granted', 'rejected', 'confirmed', 'released',
                        'expired'
                    )),
    reason          TEXT,
    epsilon_before  REAL NOT NULL,
    epsilon_after   REAL NOT NULL,
    rdp_best_order  REAL NOT NULL,
    created_at      TEXT NOT NULL,
    expires_at      TEXT,
    expired_at      TEXT,
    confirmed_at    TEXT,
    confirmed_request_id TEXT,
    released_at     TEXT,
    UNIQUE (dataset_id, reservation_id)
);

CREATE INDEX IF NOT EXISTS idx_reservations_dataset
    ON reservations (dataset_id, id);

CREATE INDEX IF NOT EXISTS idx_reservations_active
    ON reservations (dataset_id)
    WHERE status = 'granted';
"""

# Version 3: epsilon-cap transfers between datasets.  A row is the immutable
# audit record of one POST /budget-transfers call (idempotency key
# ``transfer_id`` is globally unique): rejected attempts are stored too, with
# both *_cap_before == *_cap_after so a replay returns the original verdict and
# caps are demonstrably untouched.  Only epsilon_cap moves; delta_cap and all
# granted requests / reservations of both datasets are left unchanged.
SCHEMA_V3 = """
CREATE TABLE IF NOT EXISTS budget_transfers (
    id                        INTEGER PRIMARY KEY AUTOINCREMENT,
    transfer_id               TEXT NOT NULL UNIQUE,
    source_dataset_id         TEXT NOT NULL REFERENCES datasets(dataset_id),
    target_dataset_id         TEXT NOT NULL REFERENCES datasets(dataset_id),
    epsilon_amount            REAL NOT NULL,
    status                    TEXT NOT NULL CHECK (status IN (
                                  'transferred', 'rejected'
                              )),
    reason                    TEXT,
    source_epsilon_cap_before REAL NOT NULL,
    source_epsilon_cap_after  REAL NOT NULL,
    source_epsilon_committed  REAL NOT NULL,
    target_epsilon_cap_before REAL NOT NULL,
    target_epsilon_cap_after  REAL NOT NULL,
    target_epsilon_committed  REAL NOT NULL,
    created_at                TEXT NOT NULL
);
"""

MIGRATIONS: dict[int, str] = {
    1: SCHEMA_V1,
    2: SCHEMA_V2,
    3: SCHEMA_V3,
}
LATEST_VERSION = max(MIGRATIONS)


# --------------------------------------------------------------- exceptions --


class DatasetExists(Exception):
    """Raised when registering a dataset_id that is already present."""


class DatasetNotFound(Exception):
    """Raised when a dataset_id is missing."""


class RequestConflict(Exception):
    """Same request_id reused with a different payload (HTTP 409)."""


class ReservationConflict(Exception):
    """Same reservation_id reused with a different payload (HTTP 409)."""


class ReservationNotFound(Exception):
    """Raised when a reservation_id is missing (HTTP 404)."""


class ReservationStateConflict(Exception):
    """Operation incompatible with the reservation lifecycle state (HTTP 409).

    ``args[0]`` carries an API error code, ``args[1]`` the human detail.
    """


class TransferConflict(Exception):
    """Same transfer_id reused with a different payload (HTTP 409)."""


# -------------------------------------------------------------- connection --


@asynccontextmanager
async def _connect() -> AsyncIterator[aiosqlite.Connection]:
    conn = aiosqlite.connect(settings.db_path)
    # row_factory must be set on the live connection, i.e. after __aenter__.
    async with conn as db:
        db.row_factory = aiosqlite.Row
        yield db


async def init_db() -> None:
    directory = os.path.dirname(os.path.abspath(settings.db_path))
    os.makedirs(directory, exist_ok=True)
    async with _connect() as db:
        await db.execute(f"PRAGMA busy_timeout = {int(settings.busy_timeout_ms)}")
        await db.execute("PRAGMA journal_mode = WAL")
        await db.execute("PRAGMA foreign_keys = ON")
        await _migrate(db)


async def _migrate(db: aiosqlite.Connection) -> None:
    async with db.execute("PRAGMA user_version") as cur:
        row = await cur.fetchone()
    version = int(row[0])
    while version < LATEST_VERSION:
        next_version = version + 1
        await db.executescript(MIGRATIONS[next_version])
        # PRAGMA user_version accepts only an integer literal, and next_version
        # is an internal constant, never user input.
        await db.execute(f"PRAGMA user_version = {next_version}")
        version = next_version
    await db.commit()


@asynccontextmanager
async def connect() -> AsyncIterator[aiosqlite.Connection]:
    """Connection with Row factory; usage: ``async with db.connect() as c``."""
    conn = aiosqlite.connect(settings.db_path)
    async with conn as c:
        c.row_factory = aiosqlite.Row
        # Read endpoints may perform the lazy reservation-expiry UPDATE, which
        # needs the same write-lock queueing guarantee as explicit writers.
        await c.execute(f"PRAGMA busy_timeout = {int(settings.busy_timeout_ms)}")
        yield c


async def begin_immediate(db: aiosqlite.Connection) -> None:
    await db.execute(f"PRAGMA busy_timeout = {int(settings.busy_timeout_ms)}")
    await db.execute("BEGIN IMMEDIATE")


# ---------------------------------------------------------------- datasets --


async def create_dataset(
    dataset_id: str,
    epsilon_cap: float,
    delta_cap: float,
    description: Optional[str],
    created_at: str,
) -> None:
    async with _connect() as db:
        await begin_immediate(db)
        try:
            await db.execute(
                "INSERT INTO datasets "
                "(dataset_id, epsilon_cap, delta_cap, description, created_at) "
                "VALUES (?, ?, ?, ?, ?)",
                (dataset_id, epsilon_cap, delta_cap, description, created_at),
            )
            await db.commit()
        except aiosqlite.IntegrityError:
            await db.rollback()
            raise DatasetExists(dataset_id)


async def get_dataset_row(db: aiosqlite.Connection, dataset_id: str) -> Optional[aiosqlite.Row]:
    async with db.execute(
        "SELECT dataset_id, epsilon_cap, delta_cap, description, created_at "
        "FROM datasets WHERE dataset_id = ?",
        (dataset_id,),
    ) as cur:
        return await cur.fetchone()


async def require_dataset(db: aiosqlite.Connection, dataset_id: str) -> aiosqlite.Row:
    row = await get_dataset_row(db, dataset_id)
    if row is None:
        raise DatasetNotFound(dataset_id)
    return row


# ---------------------------------------------------------------- requests --


_RESERVATION_SELECT = (
    "SELECT dataset_id, reservation_id, epsilon, delta, ttl_seconds, status, "
    "reason, epsilon_before, epsilon_after, rdp_best_order, created_at, "
    "expires_at, expired_at, confirmed_at, confirmed_request_id, released_at "
    "FROM reservations WHERE dataset_id = ?"
)


async def list_granted_entries(
    db: aiosqlite.Connection, dataset_id: str
) -> list[tuple[float, float]]:
    """All granted mechanisms as ``(epsilon, delta)`` pairs, in grant order."""
    async with db.execute(
        "SELECT epsilon, delta FROM requests "
        "WHERE dataset_id = ? AND status = 'granted' ORDER BY id",
        (dataset_id,),
    ) as cur:
        rows = await cur.fetchall()
    return [(row[0], row[1]) for row in rows]


async def list_active_reservation_entries(
    db: aiosqlite.Connection, dataset_id: str, *, now_iso: str
) -> list[tuple[float, float]]:
    """``(epsilon, delta)`` pairs of non-expired granted reservations.

    Rows whose ``expires_at`` has passed are lazily marked expired first, so
    they stop occupying budget for queries and new admissions alike.
    """
    await db.execute(
        "UPDATE reservations SET status = 'expired', expired_at = ? "
        "WHERE dataset_id = ? AND status = 'granted' AND expires_at <= ?",
        (now_iso, dataset_id, now_iso),
    )
    async with db.execute(
        "SELECT epsilon, delta FROM reservations "
        "WHERE dataset_id = ? AND status = 'granted' "
        "AND (expires_at IS NULL OR expires_at > ?) ORDER BY id",
        (dataset_id, now_iso),
    ) as cur:
        rows = await cur.fetchall()
    return [(row[0], row[1]) for row in rows]


async def list_granted_reservation_rows(
    db: aiosqlite.Connection, dataset_id: str, *, now_iso: str
) -> list[aiosqlite.Row]:
    """Currently valid granted reservations as full rows, without any writes.

    Unlike :func:`list_active_reservation_entries` this performs *no* lazy
    expiry UPDATE: read-only forecasting paths call it against a snapshot
    instant of their own choosing without ever touching reservation state.
    A row is "currently valid" when ``status = 'granted'`` and its
    ``expires_at`` is NULL or still in the future relative to ``now_iso``.
    """
    async with db.execute(
        f"{_RESERVATION_SELECT} AND status = 'granted' "
        "AND (expires_at IS NULL OR expires_at > ?) ORDER BY id",
        (dataset_id, now_iso),
    ) as cur:
        return await cur.fetchall()


async def count_active_reservations(
    db: aiosqlite.Connection, dataset_id: str, *, now_iso: str
) -> int:
    async with db.execute(
        "SELECT COUNT(*) FROM reservations "
        "WHERE dataset_id = ? AND status = 'granted' "
        "AND (expires_at IS NULL OR expires_at > ?)",
        (dataset_id, now_iso),
    ) as cur:
        row = await cur.fetchone()
    return int(row[0])


async def insert_request(
    db: aiosqlite.Connection,
    *,
    dataset_id: str,
    request_id: str,
    query_type: str,
    epsilon: float,
    delta: float,
    status: str,
    reason: Optional[str],
    epsilon_before: float,
    epsilon_after: float,
    rdp_best_order: float,
    created_at: str,
) -> None:
    await db.execute(
        "INSERT INTO requests "
        "(dataset_id, request_id, query_type, epsilon, delta, status, reason, "
        "epsilon_before, epsilon_after, rdp_best_order, created_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            dataset_id, request_id, query_type, epsilon, delta, status, reason,
            epsilon_before, epsilon_after, rdp_best_order, created_at,
        ),
    )


async def get_request(
    db: aiosqlite.Connection, dataset_id: str, request_id: str
) -> Optional[aiosqlite.Row]:
    async with db.execute(
        "SELECT dataset_id, request_id, query_type, epsilon, delta, status, "
        "reason, epsilon_before, epsilon_after, rdp_best_order, created_at "
        "FROM requests WHERE dataset_id = ? AND request_id = ?",
        (dataset_id, request_id),
    ) as cur:
        return await cur.fetchone()


async def list_requests(db: aiosqlite.Connection, dataset_id: str) -> list[aiosqlite.Row]:
    async with db.execute(
        "SELECT dataset_id, request_id, query_type, epsilon, delta, status, "
        "reason, epsilon_before, epsilon_after, rdp_best_order, created_at "
        "FROM requests WHERE dataset_id = ? ORDER BY id",
        (dataset_id,),
    ) as cur:
        return await cur.fetchall()


# ------------------------------------------------------------- reservations --


async def insert_reservation(
    db: aiosqlite.Connection,
    *,
    dataset_id: str,
    reservation_id: str,
    epsilon: float,
    delta: float,
    ttl_seconds: int,
    status: str,
    reason: Optional[str],
    epsilon_before: float,
    epsilon_after: float,
    rdp_best_order: float,
    created_at: str,
    expires_at: Optional[str],
) -> None:
    await db.execute(
        "INSERT INTO reservations "
        "(dataset_id, reservation_id, epsilon, delta, ttl_seconds, status, "
        "reason, epsilon_before, epsilon_after, rdp_best_order, created_at, "
        "expires_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            dataset_id, reservation_id, epsilon, delta, ttl_seconds, status,
            reason, epsilon_before, epsilon_after, rdp_best_order, created_at,
            expires_at,
        ),
    )


async def get_reservation(
    db: aiosqlite.Connection, dataset_id: str, reservation_id: str
) -> Optional[aiosqlite.Row]:
    async with db.execute(
        f"{_RESERVATION_SELECT} AND reservation_id = ?",
        (dataset_id, reservation_id),
    ) as cur:
        return await cur.fetchone()


async def list_reservations(
    db: aiosqlite.Connection, dataset_id: str
) -> list[aiosqlite.Row]:
    async with db.execute(
        f"{_RESERVATION_SELECT} ORDER BY id",
        (dataset_id,),
    ) as cur:
        return await cur.fetchall()


async def expire_reservation(
    db: aiosqlite.Connection,
    *,
    dataset_id: str,
    reservation_id: str,
    expired_at: str,
) -> None:
    """Lazily mark an outstanding granted reservation as expired."""
    await db.execute(
        "UPDATE reservations SET status = 'expired', expired_at = ? "
        "WHERE dataset_id = ? AND reservation_id = ? AND status = 'granted'",
        (expired_at, dataset_id, reservation_id),
    )


async def confirm_reservation(
    db: aiosqlite.Connection,
    *,
    dataset_id: str,
    reservation_id: str,
    confirmed_request_id: str,
    confirmed_at: str,
    epsilon_before: float,
    epsilon_after: float,
    rdp_best_order: float,
) -> None:
    await db.execute(
        "UPDATE reservations SET status = 'confirmed', confirmed_at = ?, "
        "confirmed_request_id = ?, epsilon_before = ?, epsilon_after = ?, "
        "rdp_best_order = ? "
        "WHERE dataset_id = ? AND reservation_id = ? AND status = 'granted'",
        (
            confirmed_at, confirmed_request_id, epsilon_before, epsilon_after,
            rdp_best_order, dataset_id, reservation_id,
        ),
    )


async def release_reservation(
    db: aiosqlite.Connection,
    *,
    dataset_id: str,
    reservation_id: str,
    released_at: str,
) -> None:
    await db.execute(
        "UPDATE reservations SET status = 'released', released_at = ? "
        "WHERE dataset_id = ? AND reservation_id = ? AND status = 'granted'",
        (released_at, dataset_id, reservation_id),
    )


async def backdate_reservation_expiry(
    db: aiosqlite.Connection,
    *,
    dataset_id: str,
    reservation_id: str,
    expires_at: str,
) -> None:
    """Test/admin helper: rewrite a reservation's expiry instant."""
    await db.execute(
        "UPDATE reservations SET expires_at = ? "
        "WHERE dataset_id = ? AND reservation_id = ?",
        (expires_at, dataset_id, reservation_id),
    )


# ------------------------------------------------------------ cap transfers --


async def update_dataset_epsilon_cap(
    db: aiosqlite.Connection,
    *,
    dataset_id: str,
    epsilon_cap: float,
) -> None:
    """Set a dataset's epsilon cap inside the caller's transaction."""
    await db.execute(
        "UPDATE datasets SET epsilon_cap = ? WHERE dataset_id = ?",
        (epsilon_cap, dataset_id),
    )


async def insert_budget_transfer(
    db: aiosqlite.Connection,
    *,
    transfer_id: str,
    source_dataset_id: str,
    target_dataset_id: str,
    epsilon_amount: float,
    status: str,
    reason: Optional[str],
    source_epsilon_cap_before: float,
    source_epsilon_cap_after: float,
    source_epsilon_committed: float,
    target_epsilon_cap_before: float,
    target_epsilon_cap_after: float,
    target_epsilon_committed: float,
    created_at: str,
) -> None:
    await db.execute(
        "INSERT INTO budget_transfers "
        "(transfer_id, source_dataset_id, target_dataset_id, epsilon_amount, "
        "status, reason, source_epsilon_cap_before, source_epsilon_cap_after, "
        "source_epsilon_committed, target_epsilon_cap_before, "
        "target_epsilon_cap_after, target_epsilon_committed, created_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            transfer_id, source_dataset_id, target_dataset_id, epsilon_amount,
            status, reason, source_epsilon_cap_before, source_epsilon_cap_after,
            source_epsilon_committed, target_epsilon_cap_before,
            target_epsilon_cap_after, target_epsilon_committed, created_at,
        ),
    )


_TRANSFER_SELECT = (
    "SELECT transfer_id, source_dataset_id, target_dataset_id, epsilon_amount, "
    "status, reason, source_epsilon_cap_before, source_epsilon_cap_after, "
    "source_epsilon_committed, target_epsilon_cap_before, "
    "target_epsilon_cap_after, target_epsilon_committed, created_at "
    "FROM budget_transfers WHERE transfer_id = ?"
)


async def get_budget_transfer(
    db: aiosqlite.Connection, transfer_id: str
) -> Optional[aiosqlite.Row]:
    async with db.execute(_TRANSFER_SELECT, (transfer_id,)) as cur:
        return await cur.fetchone()
