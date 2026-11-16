import os, time, asyncio, logging
from typing import List, Dict, Any, Optional
from psycopg_pool import AsyncConnectionPool
from psycopg import AsyncConnection, Error
from datetime import datetime, timezone

from config import EXECUTION_MODE, DB_CHUNK_SIZE, DB_INSERT_INTERVAL_SECONDS
from models import Measure, Sensor

class Database:
    def __init__(self):
        self.db_queue: [asyncio.Queue] = asyncio.Queue()
        self.pool: Optional[AsyncConnectionPool] = None
        self.registry = None

    async def get_conn(self) -> AsyncConnection:
        if self.pool is not None:
            if not self.pool.closed:
                return self.pool.connection()
            else:
                logging.error("[DATABASE] Pool was lost or closed. Re-establishing connection...")
        await self._init_db() 
        return self.pool.connection()

    async def _init_db(self) -> None:
        """
        Initializes the asynchronous PostgreSQL connection pool if required by the execution mode.
        """
        if EXECUTION_MODE == "OFFLINE_SIMULATION":
            logging.info("[DATABASE] OFFLINE_SIMULATION active. Bypassing connection pool initialization.")
            return

        user = os.getenv("PG_USER")
        password = os.getenv("PG_PASSWORD")
        host = os.getenv("PG_HOST")
        port = os.getenv("PG_PORT", "5432")
        name = os.getenv("PG_DB")

        url = f"postgresql://{user}:{password}@{host}:{port}/{name}"
        logging.info(f"[DATABASE] Initializing pool connection to: postgresql://{user}@{host}:{port}/{name}")

        self.pool = AsyncConnectionPool(conninfo=url, min_size=1, max_size=2, open=False)
        await self.pool.open()
        await self.pool.wait()
        logging.info(f"[DATABASE] Connected to TimescaleDB pool in [{EXECUTION_MODE}] mode.")

        try:
            await self.registry.load_mapping()
            logging.info("[DATABASE] Sensor mapping cache successfully reloaded after DB connection/reconnection.")
        except Exception as e:
            logging.error(f"[DATABASE] Failed to reload sensor mapping cache after DB connection: {e}")

    async def close_db(self) -> None:
        """
        Gracefully terminates the global database connection pool.
        """
        if self.pool:
            await self.pool.close()
            self.pool = None
            logging.info("[DATABASE] Connection pool closed successfully.")

    async def _insert_measures(self, buffer: List[Measure]) -> None:
        """
        Inserts a batch of Measure objects into the database, or simulates
        the insertion depending on the APP_EXECUTION_MODE value.
        """
        if not buffer:
            return

        bindings_matrix = []
        for m in buffer:
            sensor_id = (
                m.sensor.db_id
                if m.sensor and getattr(m.sensor, "db_id", None) is not None
                else m.sensor_id
            )

            if sensor_id is None:
                logging.warning(f"[DATABASE] Integrity violation: Measure at {m.time} lacks any valid sensor reference.")
                raise ValueError(f"Measure at {m.time} has no associated sensor ID.")

            bindings_matrix.append((
                m.time,
                sensor_id,
                m.temperature,
                m.humidity,
                m.battery_raw
            ))

        # Routing logic for simulation and offline modes
        if EXECUTION_MODE in ("OFFLINE_SIMULATION", "MOCK_INSERT", "SENSORS_ONLY"):
            for i in range(0, len(bindings_matrix), DB_CHUNK_SIZE):
                chunk = bindings_matrix[i:i + DB_CHUNK_SIZE]
                samples = [
                    f"('{t.strftime('%Y-%m-%d %H:%M:%S')}', {s}, {temp}, {hum}, {bat})"
                    for t, s, temp, hum, bat in chunk[:3]
                ]
                preview = ", ".join(samples) + (f", ... (+ {len(chunk) - 3} rows)" if len(chunk) > 3 else "")
                logging.info(f"[DB MOCK] Simulated chunk {i // DB_CHUNK_SIZE + 1} ({len(chunk)} rows) preview: {preview}")
                if i + DB_CHUNK_SIZE < len(bindings_matrix):
                    await asyncio.sleep(DB_INSERT_INTERVAL_SECONDS)
            return

        query = """
            INSERT INTO measures (time, sensor_id, temperature, humidity, battery_raw)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (time, sensor_id) DO NOTHING;
        """

        # Asynchronous batch flush optimized for psycopg3 pipelining
        try:
            async with self.pool.connection() as conn:
                async with conn.cursor() as cur:
                    for i in range(0, len(bindings_matrix), DB_CHUNK_SIZE):
                        await cur.executemany(query, bindings_matrix[i:i + DB_CHUNK_SIZE])
                        if i + DB_CHUNK_SIZE < len(bindings_matrix):
                            await asyncio.sleep(DB_INSERT_INTERVAL_SECONDS)
            logging.info(f"[DATABASE] Successfully flushed batch of {len(buffer)} measures to measures.")

        except Exception as e:
            logging.error(f"[DATABASE] Batch measurement insertion sequence failed: {e}")
            raise e

    async def insert_sensor(self, sensor: Sensor) -> None:
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
        bindings = (sensor.ble_id, sensor.mac_address)

        # Routing logic for simulation and partial tracking modes
        if EXECUTION_MODE in ("OFFLINE_SIMULATION", "MOCK_INSERT"):
            mock_query = query.strip()
            for b in bindings: mock_query = mock_query.replace("%s", repr(b), 1)
            logging.info(f"[DB MOCK] Executing {mock_query}") #Remplace query par la query en remplacant les %s par les bindings.
            return int(time.time()) & 0xFFFF

        try:
            async with self.pool.connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(query, bindings)
                    sensor_id = (await cur.fetchone())[0]

            logging.info(f"[DATABASE Info] Registered new hardware signature {sensor.ble_id} with database reference ID: {sensor_id}")
            return sensor_id
        except Exception as e:
            logging.info(f"[DATABASE ERROR] Critical sensor hardware registration sequence aborted: {e}")
            raise e

    async def close_sensor_assignment(self, sensor_id: int) -> None:
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
            logging.info(f"[DB SIMULATION - MODE: {EXECUTION_MODE}] Intercepted query execution simulation:")
            logging.info(f"  RAW TARGET QUERY -> {query.strip()}")
            logging.info(f"  CURSOR BINDINGS EXEC ARGS => {bindings}")
            return

        try:
            async with self.pool.connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(query, bindings)
            logging.info(f"[DATABASE] Closed active structural assignment record for sensor database reference: {sensor_id}")
        except Exception as e:
            logging.error(f"[DATABASE] Failed to update and terminate active device tracking assignment: {e}")
            raise e

    async def get_last_timestamps_per_sensor(self) -> List[Sensor]:
        """
        Retrieves the latest measurement timestamp stored for each unique active sensor.
        Queries the underlying database partition and returns a mapping dictionary.
        """
        if EXECUTION_MODE == "OFFLINE_SIMULATION" or self.pool is None:
            # Simulated environment fallback: bypass structural SQL query execution
            return {}

        query = "SELECT s.ble_id, s.mac_address, MAX(m.time) FROM measures as m LEFT JOIN sensors as s ON m.sensor_id = s.id GROUP BY s.ble_id, s.mac_address;"
        timestamps = []

        try:
            async with self.pool.connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(query)
                    rows = await cur.fetchall()
                    for ble_id, mac_address, last_time in rows:
                        if last_time:
                            # Enforce explicit UTC timezone alignment on native datetimes
                            timestamps.append(Sensor(
                                db_id=None,
                                ble_id=ble_id,
                                mac_address=mac_address,
                                location_name=None,
                                history_cursor_timestamp=last_time.replace(tzinfo=timezone.utc) if last_time.tzinfo is None else last_time,
                                has_data_gap=False,
                                history_catchup_completed=False
                            )) 
        except Exception as e:
            logging.error(f"[DATABASE] Failed to fetch per-sensor historical boundaries: {e}")

        return timestamps

    async def db_worker(self) -> None:
        """
        Continuous consumer background routine driving the centralized ingestion funnel (Queue 2).
        Pulls pre-packaged batches of measures from the queue and flushes them safely into TimescaleDB.
        """
        logging.info("[DATABASE] Ingestion funnel background pipeline worker initializing.")

        try:
            while True:
                try:
                    # Non-blocking pull from ingestion queue with a 1-second heartbeat timeout
                    logging.debug(f"[DB WORKER] Listening to queue ID: {id(self.db_queue)} | Current size: {self.db_queue.qsize()}")
                    batch = await asyncio.wait_for(self.db_queue.get(), timeout=1.0)
                    
                    try:
                        # Insertion directe du lot de mesures préparé par le décodeur
                        await self._insert_measures(batch)

                    except Error as sql_err:
                        logging.error(f"[DB WORKER] SqlException: {sql_err}")

                    except Exception as e:
                        logging.error("[DB WORKER] An internal error occurred.")
                        raise e
                    finally:
                        self.db_queue.task_done()
                        
                except asyncio.TimeoutError:
                    # Timeout normal de la queue, permet de boucler proprement
                    continue

        except asyncio.CancelledError as ce:
            logging.info("[DATABASE] System shutdown signal intercepted. Draining queue before offline...")
            while not self.db_queue.empty():
                try:
                    batch = self.db_queue.get_nowait()
                    await self._insert_measures(batch)
                except Exception:
                    pass
                finally:
                    try:
                        self.db_queue.task_done()
                    except ValueError:
                        pass
            logging.info("[DATABASE] Ingestion worker offline.")
            raise ce

        except Exception as e:
            logging.error(f"[DB WORKER FATAL] Unexpected queue loop error: {e}")
            raise e

