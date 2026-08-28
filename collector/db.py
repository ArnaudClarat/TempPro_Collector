import os, time, asyncio
from typing import List, Dict, Any, Optional
from psycopg_pool import AsyncConnectionPool
from datetime import datetime, timezone

from logger import log_msg
from config import EXECUTION_MODE, DB_INSERT_INTERVAL, DB_MAX_BATCH_SIZE
from models import SensorMeasure

class DatabaseBatcher:
    def __init__(self):
        self.db_queue: Optional[asyncio.Queue] = None
        self.pool: Optional[AsyncConnectionPool] = None

    async def init_db(self) -> None:
        """
        Initializes the asynchronous PostgreSQL connection pool if required by the execution mode.
        """
        self.db_queue = asyncio.Queue()
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

        self.pool = AsyncConnectionPool(conninfo=url, min_size=1, max_size=2, open=False)
        await self.pool.open()
        await self.pool.wait()
        log_msg("Info", f"[DATABASE] Connected to TimescaleDB pool in [{EXECUTION_MODE}] mode.")

    async def close_db(self) -> None:
        """
        Gracefully terminates the global database connection pool.
        """
        if self.pool:
            await self.pool.close()
            self.pool = None
            log_msg("Info", "[DATABASE] Connection pool closed successfully.")

    async def insert_measures(self, buffer: List[SensorMeasure]) -> None:
        """
        Inserts a batch of SensorMeasure objects into the database, or simulates
        the insertion depending on the APP_EXECUTION_MODE value.
        """
        if not buffer:
            return

        query = """
            INSERT INTO measures (time, sensor_id, temperature, humidity_raw, battery_raw)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (time, sensor_id) DO NOTHING;
        """

        # Direct attribute access bypassing dictionary overhead
        bindings_matrix = [
            (
                m.time,
                m.sensor_id,
                m.temperature,
                m.humidity_raw,
                m.battery_raw
            )
            for m in buffer
        ]

        # Routing logic for simulation and offline modes
        if EXECUTION_MODE in ("OFFLINE_SIMULATION", "MOCK_INSERT", "SENSORS_ONLY"):
            samples = [f"('{t.strftime('%Y-%m-%d %H:%M:%S')}', {s}, {temp}, {hum}, {bat})" for t, s, temp, hum, bat in bindings_matrix[:3]]
            preview = ", ".join(samples) + (f", ... (+ {len(bindings_matrix) - 3} rows)" if len(bindings_matrix) > 3 else "")
            log_msg("Info", f"[DB MOCK] Simulated query preview : {preview}")
            return

        # Asynchronous batch flush optimized for psycopg3 pipelining
        try:
            async with self.pool.connection() as conn:
                async with conn.cursor() as cur:
                    await cur.executemany(query, bindings_matrix)
            log_msg("Info", f"[DATABASE] Successfully flushed batch of {len(buffer)} measures to measures.")

        except Exception as e:
            log_msg("Error", f"[DATABASE] Batch measurement insertion sequence failed: {e}")
            raise e

    async def insert_sensor(self, ble_id: str, mac_address: str) -> int:
        """
        Registers a newly discovered hardware sensor, or simulates the registration
        depending on the active execution mode rules.
        """
        query = """
            INSERT INTO sensors (ble_id, mac_address)
            VALUES (%s, %s)
            ON CONFLICT (ble_id) DO UPDATE SET mac_address = EXCLUDED.mac_address
            RETURNING id;
        """
        bindings = (ble_id, mac_address)

        # Routing logic for simulation and partial tracking modes
        if EXECUTION_MODE in ("OFFLINE_SIMULATION", "MOCK_INSERT"):
            log_msg("INFO", f"[DB MOCK] Time: {buffer[0]['time'].strftime('%H:%M:%S')} | Executing aggregated insert")
            return int(time.time()) & 0xFFFF

        try:
            async with self.pool.connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(query, bindings)
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
        query = """
            UPDATE sensor_assignments
            SET removed_at = CURRENT_TIMESTAMP
            WHERE sensor_id = %s AND removed_at IS NULL;
        """
        bindings = (sensor_id,)


        if EXECUTION_MODE in ("OFFLINE_SIMULATION", "MOCK_INSERT"):
            log_msg("Info", f"[DB SIMULATION - MODE: {EXECUTION_MODE}] Intercepted query execution simulation:")
            log_msg("Info", f"  RAW TARGET QUERY -> {query.strip()}")
            log_msg("Info", f"  CURSOR BINDINGS EXEC ARGS => {bindings}")
            return

        try:
            async with self.pool.connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(query, bindings)
            log_msg("Info", f"[DATABASE] Closed active structural assignment record for sensor database reference: {sensor_id}")
        except Exception as e:
            log_msg("Error", f"[DATABASE] Failed to update and terminate active device tracking assignment: {e}")
            raise e

    async def get_last_timestamps_per_sensor(self) -> Dict[int, datetime]:
        """
        Retrieves the latest measurement timestamp stored for each unique active sensor.
        Queries the underlying database partition and returns a mapping dictionary.
        """
        if EXECUTION_MODE == "OFFLINE_SIMULATION" or self.pool is None:
            # Simulated environment fallback: bypass structural SQL query execution
            return {}

        query = "SELECT sensor_id, MAX(time) FROM measures GROUP BY sensor_id;"
        timestamps = {}

        try:
            async with self.pool.connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(query)
                    rows = await cur.fetchall()
                    for sensor_id, last_time in rows:
                        if last_time:
                            # Enforce explicit UTC timezone alignment on native datetimes
                            timestamps[sensor_id] = last_time.replace(tzinfo=timezone.utc) if last_time.tzinfo is None else last_time
        except Exception as e:
            log_msg("ERROR", f"[DATABASE ERROR] Failed to fetch per-sensor historical boundaries: {e}")

        return timestamps

    async def db_worker(self) -> None:
        """
        Continuous consumer background routine driving the centralized ingestion funnel (Queue 2).
        """
        log_msg("Info", "[DATABASE] Ingestion funnel background pipeline worker initializing.")
        from datetime import timedelta
        from models import SensorMeasure

        buffer: List[SensorMeasure] = []
        minute_accumulator: Dict[str, Dict[str, List[float]]] = {}
        current_minute = datetime.now(timezone.utc).minute

        try:
            while True:
                try:
                    # Non-blocking pull from ingestion queue with a 1-second heartbeat timeout
                    log_msg("DEBUG", f"[DB WORKER] Listening to queue ID: {id(self.db_queue)} | Current size: {self.db_queue.qsize()}")
                    data = await asyncio.wait_for(self.db_queue.get(), timeout=1.0)
                    self.db_queue.task_done()
                except asyncio.TimeoutError:
                    # Timeout is expected; allows the loop to evaluate the chronological minute trigger below
                    data = None

                if data:
                    ble_id = data.ble_id

                    # Dynamically provision sub-arrays if encountering a sensor for the first time during the current window
                    if ble_id not in minute_accumulator:
                        minute_accumulator[ble_id] = {"temps": [], "hums": [], "bats": [], "db_id": data.sensor_id}

                    minute_accumulator[ble_id]["temps"].append(data.temperature)
                    minute_accumulator[ble_id]["hums"].append(data.humidity_raw)
                    minute_accumulator[ble_id]["bats"].append(data.battery_raw)

                # Absolute trigger condition: time reaches the 00-second mark of a new minute
                now_dt = datetime.now(timezone.utc)
                if now_dt.minute != current_minute:
                    if minute_accumulator:
                        from main import sensor_registry
                        buffer = []

                        for ble_id, metrics in minute_accumulator.items():
                            if not metrics["temps"]: # Defensive guard against zero-division errors
                                continue

                            # Execute arithmetic mean calculations rounded to standard decimals
                            avg_temp = round(sum(metrics["temps"]) / len(metrics["temps"]), 2)
                            avg_hum = round(sum(metrics["hums"]) / len(metrics["hums"]), 2)
                            avg_bat = round(sum(metrics["bats"]) / len(metrics["bats"]), 2)
                            sensor_info = await sensor_registry.get_sensor(ble_id)

                            # Instantiating the typed dataclass object directly inside the buffer
                            buffer.append(SensorMeasure(
                                ble_id=ble_id,
                                sensor_id=sensor_info.sensor_db_id,
                                temperature=avg_temp,
                                humidity_raw=avg_hum,
                                battery_raw=int(avg_bat),
                                time=(now_dt.replace(second=0, microsecond=0) - timedelta(minutes=1)) # Force-truncate metrics timestamp to minute-precision for optimal hypertable bucket alignments
                            ))
                        if buffer:
                            await self.insert_measures(buffer)

                        minute_accumulator.clear()

                    current_minute = now_dt.minute

        except asyncio.CancelledError:
            if buffer:
                log_msg("Info", f"[DATABASE] System shutdown signal intercepted. Initiating final data flush of {len(buffer)} measures...")
                await self.insert_measures(buffer)
            log_msg("Info", "[DATABASE] Ingestion funnel worker offline.")
            raise
        except Exception as e:
            log_msg("Error", f"[DATABASE] Funnel worker pipeline crashed: {e}")

