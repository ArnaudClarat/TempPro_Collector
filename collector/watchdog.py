import time, asyncio, logging
from datetime import datetime, timezone, timedelta

from config import EXECUTION_MODE

class Watchdog:
    def __init__(self, db, registry):
        self._last_health_check = time.monotonic()
        self._recovery_executed_today = False
        self._ble_active_lock = asyncio.Lock()
        self.db = db
        self.registry = registry
        self.boxcode = "6459"

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
        last_sensors_times = await self.db.get_last_timestamps_per_sensor()
        for partial_sensor in last_sensors_times:
            full_sensor = next((s for s in mapping if s.ble_id == partial_sensor.ble_id), None)
            if full_sensor:
                full_sensor.last_seen_timestamp = partial_sensor.last_seen_timestamp
                if full_sensor.ble_id == self.boxcode:
                    await self._execute_irm_history_catchup(full_sensor)
                else:
                    await self._fetch_history(full_sensor)
        try:
            while True:
                await asyncio.sleep(1.0)
                now_monotonic = time.monotonic()
                now_datetime = datetime.now(timezone.utc)

                # Daytime monitoring checklist evaluation interval (30 min)
                if now_monotonic - self._last_health_check >= 1800:
                    await self._evaluate_sensor_heartbeats()
                    self._last_health_check = now_monotonic

                # Nightly window processing clock evaluator
                if now_datetime.hour == 0 and now_datetime.minute == 15:
                    if not self._recovery_executed_today:
                        await self._process_nightly_recovery(now_datetime)
                        self._recovery_executed_today = True
                else:
                    if now_datetime.minute != 15:
                        self._recovery_executed_today = False

        except asyncio.CancelledError:
            raise

    async def _evaluate_sensor_heartbeats(self) -> None:
        """
        Private periodic handler verifying live incoming peripheral signal freshness.
        """
        try:
            cached_sensors = await self.registry.get_all_cached_sensors()
            for ble_id, state in cached_sensors.items():
                if time.monotonic() - state["last_seen_timestamp"] > 60:
                    await self.registry.flag_data_gap(ble_id, True)

        except Exception as e:
            logging.error(f"[WATCHDOG ERROR] Heartbeat verification step failed: {e}")

    async def _process_nightly_recovery(self, current_time: datetime) -> None:
        """
        Private chronological task invoking targeted active dumps on standard data holes.
        """
        try:
            cached_sensors = await self.registry.get_all_cached_sensors()
            for ble_id, state in cached_sensors.items():
                if state.get("has_data_gap"):
                    yesterday = current_time.date() - timedelta(days=1)
                    await self._trigger_history_recovery(ble_id, yesterday)
                    await self.registry.flag_data_gap(ble_id, False)

        except Exception as e:
            logging.error(f"[WATCHDOG ERROR] Nightly recovery window execution aborted: {e}")

        try:
            logging.info("[IRM] Triggering scheduled daily fetch for Ernage...")
            now_utc = datetime.now(timezone.utc)
            await self._fetch_and_store_irm_data(start_dt=now_utc - timedelta(days=1), end_dt=now_utc)
        except Exception as e:
            logging.error(f"[WATCHDOG ERROR] Nightly IRM external fetch failed: {e}")

    async def _fetch_history(self, sensor) -> None:
        """
        Startup sequence recovering missing history data globally using pytp357s orchestration.
        Uses explicit timezone translation to eliminate historical time-drift.
        """
        import os, time
        from datetime import datetime, timezone
        from zoneinfo import ZoneInfo
        from pytp357s.fetcher import process_devices
        from models import Measure

        logging.info("[WATCHDOG] Initiating global startup history catchup sequence...")

        # Dynamic timezone extraction from system environment
        local_tz = ZoneInfo(os.getenv("APP_TIMEZONE", "UTC"))

        try:
            mapping = await self.registry.load_mapping()
            now_utc = datetime.now(timezone.utc)

            mac = sensor.mac_address
            ble_id = sensor.ble_id

            if not mac:
                logging.warning(f"[RECOVERY] Skipping recovery for virtual asset {ble_id} (No MAC assigned).")
                return

            if sensor.last_seen_timestamp:
                last_utc = sensor.last_seen_timestamp.replace(tzinfo=timezone.utc) if sensor.last_seen_timestamp.tzinfo is None else sensor.last_seen_timestamp.astimezone(timezone.utc)
                diff_seconds = (now_utc - last_utc).total_seconds()
                minutes_to_fetch = max(1, int(diff_seconds / 60))
            else:
                logging.warning(f"[RECOVERY] Skipping recovery for virtual asset {ble_id} (No last time).")
                return

            device = {ble_id: {"mac": mac}}

            logging.info(f"[RECOVERY] Submitting pipeline to fetch past {minutes_to_fetch} minutes from {ble_id}")

            start_fetch = time.perf_counter()
            timeout = 30.0
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
                logging.warning(f"[RECOVERY] Skipping {ble_id} (Unreachable or out of range after {timeout}s. Reason: {reason})")
                return

            records = result.data if (hasattr(result, "data") and result.data is not None) else []
            if not records:
                logging.info(f"[RECOVERY] No historical flash records captured for sensor {ble_id}.")
                return

            ratio = minutes_to_fetch / elapsed_fetch if elapsed_fetch > 0 else 0

            logging.info(f"[BENCHMARK] Fetch terminé en {elapsed_fetch:.2f} secondes pour {minutes_to_fetch} minutes demandées.")
            logging.info(f"[BENCHMARK] Vitesse estimée : {ratio:.1f} minutes de données récupérées par seconde réelle.")

            buffer: list[Measure] = []
            for dt, temp, hum in records:
                utc_time = dt.replace(second=0, microsecond=0, tzinfo=local_tz).astimezone(timezone.utc)
                buffer.append(Measure(
                    time=utc_time,
                    sensor= await self.registry.get_sensor(ble_id),
                    ble_id=ble_id,
                    temperature=round(float(temp), 2),
                    humidity_raw=round(float(hum), 2)
                ))

            if buffer:
                logging.info(f"[RECOVERY] Flushing {len(buffer)} object measures to DB for {ble_id}...")
                try:
                    await self.db.insert_measures(buffer)
                except Exception as db_err:
                    logging.warning(f"[RECOVERY] Non-blocking database return notification: {db_err}")

        except Exception as e:
            logging.error(f"[RECOVERY ERROR] Startup sync evaluation collapsed: {e}")

    async def _execute_irm_history_catchup(self, sensor) -> None:
        """
        Startup sequence recovering missing IRM historical data.
        """
        import aiohttp
        from models import Measure

        logging.info("[IRM] Starting cold-start catchup for Ernage station...")

        try:
            # Enforce UTC awareness for safe datetime arithmetic
            sensor.last_seen_timestamp = sensor.last_seen_timestamp.replace(tzinfo=timezone.utc) if not sensor.last_seen_timestamp.tzinfo else sensor.last_seen_timestamp.astimezone(timezone.utc)

            current_start = sensor.last_seen_timestamp
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
                                humidity_raw=int(round(float(p["humidity_rel_shelter_avg"])))
                            ))

                        if buffer:
                            logging.info(f"[IRM] Successfully parsed {len(buffer)} records. Flushing to database...")
                            try:
                                await self.db.insert_measures(buffer)
                            except Exception as db_err:
                                logging.warning(f"[IRM] Non-blocking database insertion warning: {db_err}")
                        else:
                            logging.warning("[IRM] Sync sequence completed but zero valid station metrics were captured.")

                    current_start = current_end

        except Exception as network_error:
            logging.error(f"[IRM ERROR] Asynchronous network extraction sequence collapsed: {network_error}")
