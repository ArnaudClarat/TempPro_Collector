import time, asyncio
from datetime import datetime, timezone, timedelta

from config import EXECUTION_MODE
from logger import log_msg

class Watchdog:
    def __init__(self, db, registry):
        self._last_health_check = time.monotonic()
        self._recovery_executed_today = False
        self._ble_active_lock = asyncio.Lock()
        self.db = db
        self.registry = registry
        self.boxcode = 6459

    async def start_worker(self) -> None:
        """
        Background tracking watchdog layer. Bypassed in standalone offline testing.
        """
        if EXECUTION_MODE == "OFFLINE_SIMULATION":
            log_msg("Info", "[WATCHDOG] Watchdog not running in offline simulation.")
            return


        log_msg("Info", "[WATCHDOG] Watchdog starting.")

        # Startup individual data sync verification
        last_sensor_times = await self.db.get_last_timestamps_per_sensor()
        await self._execute_startup_history_catchup(last_sensor_times)
        await self._execute_irm_history_catchup(last_sensor_times.get(self.boxcode))

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
            log_msg("ERROR", f"[WATCHDOG ERROR] Heartbeat verification step failed: {e}")

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
            log_msg("ERROR", f"[WATCHDOG ERROR] Nightly recovery window execution aborted: {e}")

        try:
            log_msg("INFO", "[IRM] Triggering scheduled daily fetch for Ernage...")
            now_utc = datetime.now(timezone.utc)
            await self._fetch_and_store_irm_data(start_dt=now_utc - timedelta(days=1), end_dt=now_utc)
        except Exception as e:
            log_msg("ERROR", f"[WATCHDOG ERROR] Nightly IRM external fetch failed: {e}")

    async def _execute_startup_history_catchup(self) -> None:
        """
        Startup sequence recovering missing history data globally using pytp357s orchestration.
        Uses explicit timezone translation to eliminate historical time-drift.
        """
        import os
        from zoneinfo import ZoneInfo
        from pytp357s.fetcher import process_devices
        from models import SensorMeasure

        log_msg("INFO", "[WATCHDOG] Initiating global startup history catchup sequence...")

        # Dynamic timezone extraction from system environment
        local_tz = ZoneInfo(os.getenv("APP_TIMEZONE", "UTC"))

        try:
            last_times = await self.db.get_last_timestamps_per_sensor()
            mapping = await self.registry.load_mapping()

            devices = {
                ble_id: {"mac": meta["mac_address"]}
                for ble_id, meta in mapping.items()
                if meta.get("mac_address")
            }

            if not devices:
                log_msg("WARN", "[WATCHDOG] No valid active devices mapped for history recovery.")
                return

            last_time = min(last_times.values()) if last_times else None

            # Safe defensive delta calculation handling naive or aware database datetimes
            if last_time:
                last_time_utc = last_time.replace(tzinfo=timezone.utc) if last_time.tzinfo is None else last_time.astimezone(timezone.utc)
                diff_seconds = (datetime.now(timezone.utc) - last_time_utc).total_seconds()
                minutes_to_fetch = max(1, int(diff_seconds / 60))
            else:
                minutes_to_fetch = 10080  # Default fallback: 7 days in minutes

            log_msg("INFO", f"[WATCHDOG] Submitting pipeline to fetch past {minutes_to_fetch} minutes...")

            raw_responses = await process_devices(
                devices=devices, live=False, db_path=None, incremental=False,
                count=minutes_to_fetch, overlap=0, timeout=30.0, scan_timeout=5.0,
                parallelism=1, force=False, max_fetch_count=0, verbose=False
            )

            if not isinstance(raw_responses, dict):
                log_msg("WARN", "[WATCHDOG] Global pipeline returned an invalid non-iterable response.")
                return

            # Flush results directly to TimescaleDB using object buffers
            for ble_id, result in raw_responses.items():
                records = result.data if (hasattr(result, "data") and result.data is not None) else []

                if not records:
                    log_msg("WARN", f"[RECOVERY] No historical flash records captured for sensor {ble_id}.")
                    continue

                sensor_db_id = mapping.get(ble_id, {}).get("sensor_db_id")
                buffer: List[SensorMeasure] = []

                for dt, temp, hum in records:
                    # Anchor native naive datetime into local space, then translate to clean UTC
                    utc_time = dt.replace(second=0, microsecond=0, tzinfo=local_tz)

                    buffer.append(SensorMeasure(
                        time=utc_time,
                        sensor_id=sensor_db_id,
                        ble_id=ble_id,
                        temperature=round(float(temp), 2),
                        humidity_raw=round(float(hum), 2)
                    ))

                if buffer:
                    log_msg("INFO", f"[RECOVERY] Flushing {len(buffer)} object measures to DB for {ble_id}...")
                    try:
                        print(buffer)
                        await self.db.insert_measures(buffer)
                    except Exception as db_err:
                        log_msg("WARN", f"[RECOVERY] Non-blocking database return notification: {db_err}")

        except Exception as e:
            log_msg("ERROR", f"[WATCHDOG ERROR] Startup sync evaluation collapsed: {e}")

    async def _execute_irm_history_catchup(self, last_time: datetime) -> None:
        """
        Startup sequence recovering missing IRM historical data.
        """
        # Enforce UTC awareness for safe datetime arithmetic
        lt_utc = last_time.replace(tzinfo=timezone.utc) if not last_time.tzinfo else last_time.astimezone(timezone.utc)

        log_msg("INFO", "[IRM] Starting cold-start catchup for Ernage station...")
        await self._fetch_and_store_irm_data(start_dt=lt_utc)

    async def _fetch_and_store_irm_data(self, start_dt: datetime) -> None:
        """
        Executes asynchronous HTTP extraction from IRM servers, parses station records,
        and flushes typed SensorMeasure structures to TimescaleDB.
        """

        import aiohttp
        from models import SensorMeasure

        current_start = start_dt
        end_dt = datetime.now(timezone.utc)

        try:
            log_msg("INFO", "[IRM] Connecting to Ernage station...")
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
                            log_msg("ERROR", f"[IRM] Data server rejected request with status code: {response.status}")
                            return

                        # Core parsing iteration over the official IRM JSON matrix structure
                        for r in (await response.json()).get("features", []):
                            # Extract and localize the raw timestamp into an aware UTC datetime object
                            p = r.get("properties", {})

                            # Skip if any required weather metric or timestamp is missing
                            if not p.get("timestamp") or p.get("temp_dry_shelter_avg") is None or p.get("humidity_rel_shelter_avg") is None:
                                log_msg("WARN", f"[IRM] Skipping incomplete record. Payload: {p}")
                                continue

                            buffer.append(SensorMeasure(
                                time=datetime.fromisoformat(p["timestamp"].replace("Z", "+00:00")),
                                sensor_id=boxcode,
                                ble_id="IRM_ERNAGE",
                                temperature=round(float(p["temp_dry_shelter_avg"]), 1),
                                humidity_raw=int(round(float(p["humidity_rel_shelter_avg"])))
                            ))

                        if buffer:
                            log_msg("INFO", f"[IRM] Successfully parsed {len(buffer)} records. Flushing to database...")
                            try:
                                await self.db.insert_measures(buffer)
                            except Exception as db_err:
                                log_msg("WARN", f"[IRM] Non-blocking database insertion warning: {db_err}")
                        else:
                            log_msg("WARN", "[IRM] Sync sequence completed but zero valid station metrics were captured.")

                    current_start = current_end

        except Exception as network_error:
            log_msg("ERROR", f"[IRM ERROR] Asynchronous network extraction sequence collapsed: {network_error}")
