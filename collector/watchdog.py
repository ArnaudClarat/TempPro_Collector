import time, asyncio
from datetime import datetime, timezone

from config import EXECUTION_MODE
from logger import log_msg

# import os, matplotlib
# matplotlib.use('Agg')
# from pytp357s.fetcher import process_devices

class Watchdog:
    def __init__(self, db, registry):
        self._last_health_check = time.monotonic()
        self._recovery_executed_today = False
        self._ble_active_lock = asyncio.Lock()
        self.db = db
        self.registry = registry

    async def start_worker(self) -> None:
        """
        Background tracking watchdog layer. Bypassed in standalone offline testing.
        """
        if EXECUTION_MODE == "OFFLINE_SIMULATION":
            log_msg("Info", "[WATCHDOG] Watchdog not running in offline simulation.")
            return


        log_msg("Info", "[WATCHDOG] Watchdog starting.")

        # Startup individual data sync verification
        await self._execute_startup_history_catchup()

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

    async def _execute_startup_history_catchup(self) -> None:
        """
        Startup sequence recovering missing history data globally using pytp357s orchestration.
        """
        from datetime import datetime, timezone
        from pytp357s.fetcher import process_devices

        log_msg("INFO", "[WATCHDOG] Initiating global startup history catchup sequence...")
        try:
            last_sensor_times = await self.db.get_last_timestamps_per_sensor()
            mapping = await self.registry.load_mapping()

            # Filter active devices and map them using the key expected by the library
            input_devices = {
                ble_id: {"mac": meta["mac_address"]}
                for ble_id, meta in mapping.items()
                if meta.get("mac_address") and meta.get("location_name") != "Ernage"
            }

            if not input_devices:
                log_msg("WARN", "[WATCHDOG] No valid active devices mapped for history recovery.")
                return

            # Dynamic depth calculation in minutes (safely handles None or empty dict from DB)
            last_time = last_sensor_times.get(6) or next(iter(last_sensor_times.values()), None) if last_sensor_times else None

            # Convert to hours or fallback to 7 days (168 hours) if pristine
            delta_hours = (datetime.now(timezone.utc) - last_time.replace(tzinfo=timezone.utc)).total_seconds() / 3600 if last_time else 168
            minutes_to_fetch = max(1, int(delta_hours * 60))

            log_msg("INFO", f"[WATCHDOG] Submitting {len(input_devices)} devices to pytp357s pipeline (Fetching past {minutes_to_fetch} minutes)...")

            # Global hardware fetch execution
            raw_responses = await process_devices(
                devices=input_devices, live=False, db_path=None, incremental=False,
                count=minutes_to_fetch, overlap=0, timeout=30.0, scan_timeout=5.0,
                parallelism=1, force=False, max_fetch_count=0, verbose=False
            )

            # Guard clause: ensure the library response matrix is a valid iterable dictionary
            if not isinstance(raw_responses, dict):
                log_msg("WARN", "[WATCHDOG] Global pipeline returned an invalid non-iterable response.")
                return

            # Flush results directly to TimescaleDB
            for ble_id, fetch_result in raw_responses.items():
                tuple_list = fetch_result.data if (hasattr(fetch_result, "data") and fetch_result.data is not None) else []

                if not tuple_list:
                    log_msg("WARN", f"[RECOVERY] No historical flash records captured for sensor {ble_id}.")
                    continue

                sensor_db_id = mapping.get(ble_id, {}).get("sensor_db_id")

                history_buffer = [
                    {
                        "time": dt.replace(second=0, microsecond=0, tzinfo=timezone.utc),
                        "sensor_id": sensor_db_id,
                        "ble_id": ble_id,
                        "temperature": round(float(temp), 2),
                        "humidity_raw": round(float(hum), 2),
                        "battery_raw": 100
                    }
                    for dt, temp, hum in tuple_list
                ]

                if history_buffer:
                    log_msg("INFO", f"[RECOVERY] Flushing {len(history_buffer)} minutes to DB for {ble_id}...")
                    try:
                        await self.db.insert_measures(history_buffer)
                    except Exception as db_err:
                        log_msg("WARN", f"[RECOVERY] Non-blocking database return notification: {db_err}")

        except Exception as e:
            log_msg("ERROR", f"[WATCHDOG ERROR] Startup sync evaluation collapsed: {e}")


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
        from datetime import timedelta
        try:
            cached_sensors = await self.registry.get_all_cached_sensors()
            for ble_id, state in cached_sensors.items():
                if state.get("has_data_gap"):
                    yesterday = current_time.date() - timedelta(days=1)
                    await self._trigger_history_recovery(ble_id, yesterday)
                    await self.registry.flag_data_gap(ble_id, False)

        except Exception as e:
            log_msg("ERROR", f"[WATCHDOG ERROR] Nightly recovery window execution aborted: {e}")
