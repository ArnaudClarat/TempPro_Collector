import os, time, asyncio
from typing import List, Dict, Any, Optional
from psycopg_pool import AsyncConnectionPool

from logger import log_msg
from config import EXECUTION_MODE, DB_INSERT_INTERVAL, DB_MAX_BATCH_SIZE

# Global Funnel Queue shared across the application (Queue 2)
db_queue: asyncio.Queue = asyncio.Queue()

# Database connection pool global reference
pool: Optional[AsyncConnectionPool] = None

async def init_db() -> None:
    """
    Initializes the asynchronous PostgreSQL connection pool if required by the execution mode.
    """
    global pool
    
    if EXECUTION_MODE == "OFFLINE_SIMULATION":
        log_msg("Info", "[DATABASE] OFFLINE_SIMULATION active. Bypassing connection pool initialization.")
        return

    user = os.getenv("PG_USER")
    password = os.getenv("PG_PASSWORD")
    host = os.getenv("PG_HOST")
    port = os.getenv("PG_PORT", "5432")
    name = os.getenv("PG_DB")

    url = f"postgresql://{user}:{password}@{host}:{port}/{name}"
    log_msg("Info", f"[DATABASE] Initializing pool connection to: postgresql://{user}@{host}:{port}/{name}")

    pool = AsyncConnectionPool(conninfo=url, min_size=1, max_size=2, open=False)
    await pool.open()
    await pool.wait()
    log_msg("Info", f"[DATABASE] Connected to TimescaleDB pool in [{EXECUTION_MODE}] mode.")

async def close_db() -> None:
    """
    Gracefully terminates the global database connection pool.
    """
    global pool
    if pool:
        await pool.close()
        pool = None
        log_msg("Info", "[DATABASE] Connection pool closed successfully.")

async def insert_measures(buffer: List[Dict[str, Any]]) -> None:
    """
    Inserts a batch of measures into the database, or simulates the insertion
    depending on the APP_EXECUTION_MODE value.
    """
    if not buffer:
        return

    # Routing logic for simulation and partial tracking modes
    if EXECUTION_MODE in ("OFFLINE_SIMULATION", "MOCK_INSERT", "SENSORS_ONLY"):
        log_msg("Info", f"\n[DB SIMULATION - MODE: {EXECUTION_MODE}] Intercepted {len(buffer)} measures:")
        for m in buffer:
            log_msg("Info", f"  - Sensor DB ID: {m.get('sensor_id')} | Temp: {m.get('temperature')}°C | Humidity: {m.get('humidity_raw')}%")
        return

    try:
        async with pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.executemany(
                    """
                    INSERT INTO public.measures (time, sensor_id, temperature, humidity_raw, battery_raw)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (time, sensor_id) DO NOTHING;
                    """,
                    [
                        (
                            m["time"],
                            m["sensor_id"],
                            m["temperature"],
                            m["humidity_raw"],
                            m.get("battery_raw")
                        )
                        for m in buffer
                    ]
                )
        log_msg("Info", f"[DATABASE Info] Successfully flushed batch of {len(buffer)} measures to public.measures.")
    except Exception as e:
        log_msg("Info", f"[DATABASE ERROR] Batch measurement insertion sequence failed: {e}")
        raise e

async def insert_sensor(ble_id: str) -> int:
    """
    Registers a newly discovered hardware sensor, or simulates the registration
    depending on the active execution mode rules.
    """
    if EXECUTION_MODE in ("OFFLINE_SIMULATION", "MOCK_INSERT"):
        fake_id = int(time.time()) & 0xFFFF
        log_msg("Info", f"[DB SIMULATION - MODE: {EXECUTION_MODE}] Generating fake ID {fake_id} for sensor {ble_id}")
        return fake_id

    try:
        async with pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    INSERT INTO public.sensors (ble_id)
                    VALUES (%s)
                    RETURNING id;
                    """,
                    (ble_id,)
                )
                sensor_id = (await cur.fetchone())[0]

        log_msg("Info", f"[DATABASE Info] Registered new hardware signature {ble_id} with database reference ID: {sensor_id}")
        return sensor_id
    except Exception as e:
        log_msg("Info", f"[DATABASE ERROR] Critical sensor hardware registration sequence aborted: {e}")
        raise e

async def close_sensor_assignment(sensor_id: int) -> None:
    """
    Updates the structural sensor assignments table to terminate a device room assignment.
    """
    if EXECUTION_MODE == "OFFLINE_SIMULATION":
        return

    try:
        async with pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    UPDATE public.sensor_assignments 
                    SET removed_at = CURRENT_TIMESTAMP 
                    WHERE sensor_id = %s AND removed_at IS NULL;
                    """,
                    (sensor_id,)
                )
        log_msg("Info", f"[DATABASE Info] Closed active structural assignment record for sensor database reference: {sensor_id}")
    except Exception as e:
        log_msg("Info", f"[DATABASE ERROR] Failed to update and terminate active device tracking assignment: {e}")
        raise e

async def db_worker() -> None:
    """
    Continuous consumer background routine driving the centralized ingestion funnel (Queue 2).
    """
    log_msg("Info", "[DATABASE] Ingestion funnel background pipeline worker initialized.")
    buffer: List[Dict[str, Any]] = []
    last_state: Dict[str, Dict[str, Any]] = {}
    last_insert = time.monotonic()

    try:
        while True:
            try:
                data = await asyncio.wait_for(db_queue.get(), timeout=1.0)
                db_queue.task_done()
            except asyncio.TimeoutError:
                data = None

            if data:
                ble_id = data["ble_id"]
                previous = last_state.get(ble_id)

                # Realtime Deduplicator / Anti-debounce algorithmic filter
                if not previous or (
                    previous["temperature"] != data["temperature"] or
                    previous["humidity_raw"] != data["humidity_raw"] or
                    previous["battery_raw"] != data["battery_raw"]
                ):
                    buffer.append(data)
                    last_state[ble_id] = data.copy()

            now = time.monotonic()

            if len(buffer) >= DB_MAX_BATCH_SIZE or (
                buffer and (now - last_insert) >= DB_INSERT_INTERVAL
            ):
                await insert_measures(buffer)
                buffer.clear()
                last_insert = now

    except asyncio.CancelledError:
        if buffer:
            log_msg("Info", f"[DATABASE] System shutdown signal intercepted. Initiating final data flush of {len(buffer)} measures...")
            await insert_measures(buffer)
        log_msg("Info", "[DATABASE] Ingestion funnel worker offline.")
        raise
    except Exception as e:
        log_msg("Info", f"[DATABASE CRITICAL ERROR] Funnel worker pipeline crashed: {e}")
