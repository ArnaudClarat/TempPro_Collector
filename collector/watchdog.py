import time, asyncio, logging
from datetime import datetime, timezone, timedelta

from config import EXECUTION_MODE
from timeutils import get_local_timezone
from models import Sensor

class Watchdog:
    def __init__(self, db, registry):
        self.db = db
        self.database_queue: asyncio.Queue = None
        self.registry = registry
        self.boxcode = "6459"
        self._catchup_tasks: dict[str, asyncio.Task] = {}

    async def start_worker(self) -> None:
        """
        Background tracking watchdog layer. Bypassed in standalone offline testing.
        """
        if EXECUTION_MODE == "OFFLINE_SIMULATION":
            logging.info("[WATCHDOG] Watchdog not running in offline simulation.")
            return

        logging.info("[WATCHDOG] Watchdog starting.")

        # Startup individual data sync verification
        logging.info("[WATCHDOG] Initiating global startup history catchup sequence...")
        mapping = await self.registry.load_mapping()
        last_timestamps = await self.db.get_last_timestamps_per_sensor()

        timestamps_by_ble_id = {
            sensor.ble_id: sensor.history_cursor_timestamp
            for sensor in last_timestamps
        }

        for sensor in mapping:
            cursor = timestamps_by_ble_id.get(sensor.ble_id)
            if cursor is None:
                continue
            sensor.history_cursor_timestamp = (
                cursor.replace(tzinfo=timezone.utc)
                if cursor.tzinfo is None
                else cursor.astimezone(timezone.utc)
            )
            await self._run_history_catchup(sensor)
        try:
            while True:
                await asyncio.sleep(1.0)
                now_datetime = datetime.now(timezone.utc)
                # Nightly window processing clock evaluator
                if (
                    now_datetime.hour == 0 
                    and now_datetime.minute == 15
                    and now_datetime.second == 0
                ):
                    await self._process_nightly_recovery()

        except asyncio.CancelledError:
            raise

    async def _process_nightly_recovery(self) -> None:
        """
        Private chronological task invoking targeted active dumps on standard data holes.
        """
        try:
            sensors = await self.registry.get_sensors()
            for sensor in sensors:
                if sensor.ble_id == self.boxcode:
                    logging.info("[RECOVERY] Triggering scheduled daily fetch for Ernage...")
                    await self.follow_up_history(sensor)

        except Exception as e:
            logging.error(f"[RECOVERY] Nightly recovery window execution aborted: {e}")

    async def _fetch_history(self, sensor) -> None:
        """
        Startup sequence recovering missing history data globally using pytp357s orchestration.
        Uses explicit timezone translation to eliminate historical time-drift.
        """
        from pytp357s.fetcher import process_devices
        from models import Measure

        logging.info(f"[RECOVERY] Initiating history catchup sequence for {sensor.location_name}...")

        # Dynamic timezone extraction from system environment
        local_tz = get_local_timezone()

        try:
            now_utc = datetime.now(timezone.utc)
            mac_address = sensor.mac_address
            ble_id = sensor.ble_id

            if not mac_address:
                logging.warning(f"[RECOVERY] Skipping recovery for virtual asset {ble_id} (No MAC assigned).")
                return

            cursor = sensor.history_cursor_timestamp

            if cursor is None:
                logging.warning(f"[RECOVERY] Skipping recovery for virtual asset {ble_id} (No last time).")
                return
            
            last_utc = (
                cursor.replace(tzinfo=timezone.utc)
                if cursor.tzinfo is None
                else cursor.astimezone(timezone.utc)
            )
            minutes_to_fetch = int((now_utc - last_utc).total_seconds() / 60)
            device = {ble_id: {"mac": mac_address}}

            logging.info(f"[RECOVERY] Submitting pipeline to fetch past {minutes_to_fetch} minutes from {sensor.location_name} ({ble_id})")

            start_fetch = time.perf_counter()
            timeout = 600.0
            try:
                raw_responses = await process_devices(
                    devices=device, live=False, db_path=None, incremental=False,
                    count=minutes_to_fetch, overlap=0, timeout=timeout, scan_timeout=5.0,
                    parallelism=1, force=False, max_fetch_count=0, verbose=False
                )
            except Exception as fetch_err:
                logging.warning(f"[RECOVERY] Sensor {ble_id} link dropped or unreachable: {fetch_err}")
                return

            elapsed_fetch = time.perf_counter() - start_fetch

            result = raw_responses.get(ble_id) if isinstance(raw_responses, dict) else None

            if not result or getattr(result, "status", "error") == "error":
                reason = getattr(result, "message", "Timeout/No response received.")
                logging.warning(f"[RECOVERY] Skipping {sensor.location_name} ({ble_id}) (Unreachable or out of range after {timeout}s. Reason: {reason})")
                return

            records = result.data if (hasattr(result, "data") and result.data is not None) else []
            if not records:
                logging.info(f"[RECOVERY] No historical flash records captured for sensor in {sensor.location_name} ({ble_id}).")
                sensor.history_catchup_completed = True
                sensor.history_cursor_timestamp = datetime.now()
                return

            ratio = minutes_to_fetch / elapsed_fetch if elapsed_fetch > 0 else 0

            logging.info(f"[BENCHMARK] Fetch terminé en {elapsed_fetch:.2f} secondes pour {minutes_to_fetch} minutes demandées.")
            logging.info(f"[BENCHMARK] Vitesse estimée : {ratio:.1f} minutes de données récupérées par seconde réelle.")

            buffer: list[Measure] = []
            for dt, temp, hum in records:
                utc_time = dt.replace(second=0, microsecond=0, tzinfo=local_tz).astimezone(timezone.utc)
                buffer.append(Measure(
                    time=utc_time,
                    sensor= sensor,
                    sensor_id=sensor.db_id,
                    ble_id=ble_id,
                    temperature=round(float(temp), 2),
                    humidity=round(float(hum), 2)
                ))

            if buffer:
                logging.info(f"[RECOVERY] Flushing {len(buffer)} object measures to DB for {sensor.location_name} ({ble_id})...")
                try:
                    await self.database_queue.put(buffer)
                    sensor.history_catchup_completed=True
                    history_cursor_timestamp = datetime.now()
                except Exception as db_err:
                    logging.warning(f"[RECOVERY] Non-blocking database return notification: {db_err}")

            sensor.history_cursor_timestamp = now_utc
            sensor.history_catchup_completed = True
        
        except Exception as e:
            logging.error(f"[RECOVERY] Startup sync evaluation collapsed: {e}")

    async def _execute_irm_history_catchup(self, sensor) -> None:
        """
        Startup sequence recovering missing IRM historical data.
        """
        import aiohttp
        from models import Measure

        logging.info("[IRM] Starting cold-start catchup for Ernage station...")

        try:
            cursor = sensor.history_cursor_timestamp

            if cursor is None:
                logging.warning("[IRM] No historical cursor available.")
                return

            sensor.history_cursor_timestamp = (
                cursor.replace(tzinfo=timezone.utc)
                if cursor.tzinfo is None
                else cursor.astimezone(timezone.utc)
            )

            current_start = sensor.history_cursor_timestamp
            end_dt = datetime.now(timezone.utc)
            
            logging.info("[IRM] Connecting to Ernage station...")
            async with aiohttp.ClientSession() as session:
                while current_start < end_dt:
                    current_end = min(current_start + timedelta(days=3), end_dt)
                    buffer = []

                    async with session.get(
                        "https://opendata.meteo.be/service/ows",
                        params= {
                            "service": "WFS",
                            "version": "2.0.0",
                            "request": "GetFeature",
                            "typeNames": "aws:aws_10min",
                            "outputFormat": "json",
                            "propertyName": "timestamp,temp_dry_shelter_avg,humidity_rel_shelter_avg",
                            "cql_filter": f"timestamp BETWEEN '{current_start.strftime('%Y-%m-%dT%H:%M:%S')}' AND '{current_end.strftime('%Y-%m-%dT%H:%M:%S')}' AND code = {self.boxcode}"
                        },
                        timeout=30
                    ) as response:
                        if response.status != 200:
                            logging.error(f"[IRM] Data server rejected request with status code: {response.status}")
                            return

                        # Core parsing iteration over the official IRM JSON matrix structure
                        for r in (await response.json()).get("features", []):
                            # Extract and localize the raw timestamp into an aware UTC datetime object
                            p = r.get("properties", {})

                            # Skip if any required weather metric or timestamp is missing
                            if not p.get("timestamp") or p.get("temp_dry_shelter_avg") is None or p.get("humidity_rel_shelter_avg") is None:
                                logging.warning(f"[RECOVERY] Skipping incomplete record. Payload: {p}")
                                continue

                            buffer.append(Measure(
                                time=datetime.fromisoformat(p["timestamp"].replace("Z", "+00:00")),
                                sensor=sensor,
                                ble_id="",
                                temperature=round(float(p["temp_dry_shelter_avg"]), 1),
                                humidity=int(round(float(p["humidity_rel_shelter_avg"])))
                            ))

                        if buffer:
                            logging.info(f"[IRM] Successfully parsed {len(buffer)} records. Flushing to database...")
                            try:
                                await self.database_queue.put(buffer)
                            except Exception as db_err:
                                logging.warning(f"[IRM] Non-blocking database insertion warning: {db_err}")
                        else:
                            logging.warning("[IRM] Sync sequence completed but zero valid station metrics were captured.")

                    current_start = current_end

            sensor.history_cursor_timestamp = end_dt
            sensor.history_catchup_completed = True
        except Exception as network_error:
            logging.error(f"[IRM] Asynchronous network extraction sequence collapsed: {network_error}")

    async def _run_history_catchup(self, sensor: Sensor) -> None:
        try:
            if sensor.ble_id == self.boxcode:
                await self._execute_irm_history_catchup(sensor)
            else:
                await self._fetch_history(sensor)
        finally:
            self._catchup_tasks.pop(sensor.ble_id, None)
    
    async def follow_up_history(self, sensor: Sensor) -> None:
        existing_task = self._catchup_tasks.get(sensor.ble_id)
    
        if existing_task and not existing_task.done():
            logging.debug(f"[WATCHDOG] Catchup already running for {sensor.ble_id}.")
            return
    
        self._catchup_tasks[sensor.ble_id] = asyncio.create_task(
            self._run_history_catchup(sensor),
            name=f"history-catchup-{sensor.ble_id}"
        )