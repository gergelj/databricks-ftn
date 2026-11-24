"""Live courier map.

Reads the courier GPS pings from Lakebase (Postgres table live.courier_gps, a continuous synced copy of
silver.gps_events) and serves a Leaflet map that refreshes every few seconds.

The app runs as its own service principal. Databricks injects the Lakebase connection settings
(PGHOST, PGDATABASE, PGUSER, LAKEBASE_ENDPOINT, ...) because the app has a "postgres" resource
(resources/courier_map.app.yml). Postgres passwords are short-lived OAuth tokens, refreshed here.
"""
import logging
import os
import threading
import time
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import psycopg
from databricks.sdk import WorkspaceClient
from fastapi import FastAPI, Request
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from psycopg import sql
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

GPS_TABLE = sql.Identifier("live", "courier_gps")
# The demo stores Novi Sad wall-clock time without a time zone (see the generator).
TZ = ZoneInfo("Europe/Belgrade")
ACTIVE_WINDOW = timedelta(minutes=int(os.getenv("ACTIVE_WINDOW_MINUTES", "30")))
TRAIL_WINDOW = timedelta(minutes=int(os.getenv("TRAIL_MINUTES", "3")))
STATIC_DIR = Path(__file__).parent / "static"

# ---------------------------------------------------------------------------
# Lakebase connection settings
# The app's "postgres" resource injects PG* variables; LAKEBASE_ENDPOINT is also set in app.yaml.
# Anything missing is looked up through the API, so a missing variable can't break the app.
# ---------------------------------------------------------------------------
log = logging.getLogger("courier_map")
logging.basicConfig(level=logging.INFO)

workspace = WorkspaceClient()
LAKEBASE_ENDPOINT = os.getenv("LAKEBASE_ENDPOINT", "projects/food-delivery-live/branches/production/endpoints/primary")


def connection_settings() -> dict:
    host = os.getenv("PGHOST") or workspace.postgres.get_endpoint(name=LAKEBASE_ENDPOINT).status.hosts.host
    return {
        "host": host,
        "port": int(os.getenv("PGPORT", "5432")),
        "dbname": os.getenv("PGDATABASE", "databricks_postgres"),
        # The service principal's Postgres role is its application (client) ID.
        "user": os.getenv("PGUSER") or workspace.config.client_id,
        "sslmode": os.getenv("PGSSLMODE", "require"),
    }


_token = {"value": "", "expires_at": 0.0}
_token_lock = threading.Lock()


def lakebase_token() -> str:
    """A Postgres password for the app's service principal. Tokens live 1 hour; renew after 45 minutes."""
    with _token_lock:
        if time.time() >= _token["expires_at"]:
            credential = workspace.postgres.generate_database_credential(endpoint=LAKEBASE_ENDPOINT)
            _token.update(value=credential.token, expires_at=time.time() + 45 * 60)
        return _token["value"]


class LakebaseConnection(psycopg.Connection):
    """Every new pooled connection logs in with a fresh token."""

    @classmethod
    def connect(cls, conninfo: str = "", **kwargs):
        kwargs["password"] = lakebase_token()
        return super().connect(conninfo, **kwargs)


def configure(conn: psycopg.Connection) -> None:
    conn.autocommit = True
    conn.execute("SET TIME ZONE 'UTC'")  # timestamps come back exactly as stored (Novi Sad wall-clock time)


SETTINGS = connection_settings()
log.info("Lakebase: host=%s db=%s user=%s endpoint=%s", SETTINGS["host"], SETTINGS["dbname"], SETTINGS["user"], LAKEBASE_ENDPOINT)

pool = ConnectionPool(
    kwargs=SETTINGS,
    connection_class=LakebaseConnection,
    configure=configure,
    min_size=1,
    max_size=4,
    max_lifetime=40 * 60,  # reconnect (with a new token) before the token expires
    open=False,
)


@asynccontextmanager
async def lifespan(_: FastAPI):
    pool.open(wait=False)  # don't block startup; the first request waits for a connection
    yield
    pool.close()


app = FastAPI(title="Courier live map", lifespan=lifespan)


@app.exception_handler(Exception)
async def show_errors(_: Request, exc: Exception):
    """Demo app: show the real error in the UI instead of a bare HTTP 500."""
    log.exception("request failed")
    return JSONResponse(status_code=500, content={"error": f"{type(exc).__name__}: {exc}"})

# ---------------------------------------------------------------------------
# API
# ---------------------------------------------------------------------------
LATEST_POSITIONS = sql.SQL("""
    SELECT DISTINCT ON (courier_id)
           courier_id, order_id, event_type, event_ts, latitude, longitude, speed_kmh
    FROM {table}
    WHERE event_ts >= %(since)s
    ORDER BY courier_id, event_ts DESC
""").format(table=GPS_TABLE)

RECENT_TRAILS = sql.SQL("""
    SELECT courier_id, order_id, latitude, longitude
    FROM {table}
    WHERE event_ts >= %(since)s AND event_type IN ('to_restaurant', 'pickup', 'en_route')
    ORDER BY courier_id, event_ts
""").format(table=GPS_TABLE)

STATUS = {"to_restaurant": "to_restaurant", "pickup": "delivering", "en_route": "delivering", "delivered": "idle"}


def wall_clock(value: datetime) -> datetime:
    """Drop the (UTC) time zone label: the value already is Novi Sad local time."""
    return value.replace(tzinfo=None) if value.tzinfo else value


@app.get("/api/couriers")
def couriers():
    now = datetime.now(TZ).replace(tzinfo=None)
    params = {"since": now - ACTIVE_WINDOW}
    with pool.connection() as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(LATEST_POSITIONS, params)
        latest = cur.fetchall()
        cur.execute(RECENT_TRAILS, {"since": now - TRAIL_WINDOW})
        trail_rows = cur.fetchall()

    trails: dict[str, list] = {}
    current_order = {row["courier_id"]: row["order_id"] for row in latest}
    for row in trail_rows:
        if row["order_id"] == current_order.get(row["courier_id"]):
            trails.setdefault(row["courier_id"], []).append([row["latitude"], row["longitude"]])

    result = []
    for row in latest:
        seen = wall_clock(row["event_ts"])
        result.append({
            "courier_id": row["courier_id"],
            "order_id": row["order_id"],
            "status": STATUS.get(row["event_type"], row["event_type"]),
            "event_type": row["event_type"],
            "last_seen": seen.isoformat(timespec="seconds"),
            "seconds_ago": max(0, int((now - seen).total_seconds())),
            "lat": row["latitude"],
            "lon": row["longitude"],
            "speed_kmh": row["speed_kmh"],
            "trail": trails.get(row["courier_id"], []),
        })

    newest = max((wall_clock(r["event_ts"]) for r in latest), default=None)
    return {
        "now": now.isoformat(timespec="seconds"),
        "latest_ping": newest.isoformat(timespec="seconds") if newest else None,
        "data_age_seconds": int((now - newest).total_seconds()) if newest else None,
        "counts": {s: sum(1 for c in result if c["status"] == s) for s in ("to_restaurant", "delivering", "idle")},
        "couriers": result,
    }


@app.get("/api/health")
def health():
    with pool.connection() as conn:
        conn.execute("SELECT 1")
    return {"status": "ok"}


# ---------------------------------------------------------------------------
# Frontend
# ---------------------------------------------------------------------------
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


@app.get("/")
def index():
    return FileResponse(STATIC_DIR / "index.html")
