import time, asyncio
from datetime import datetime, timezone

from config import EXECUTION_MODE
from logger import log_msg

class Watchdog:
    def __init__(self):
        self._last_health_check = time.monotonic()
        self._recovery_executed_today = False
        self._ble_active_lock = asyncio.Lock()

    async def start_worker(self) -> None:
        """
        Background tracking watchdog layer. Bypassed in standalone offline testing.
        """
        if EXECUTION_MODE == "OFFLINE_SIMULATION":
            log_msg("Info", "[WATCHDOG] Watchdog not running in offline simulation.")
            return

        # 1. Startup individual data sync verification
        await self._execute_startup_checkup()

        try:
            while True:
                await asyncio.sleep(1.0)
                now_monotonic = time.monotonic()
                now_datetime = datetime.now(timezone.utc)

                # 2. Daytime monitoring checklist evaluation interval (30 min)
                if now_monotonic - self._last_health_check >= 1800:
                    await self._evaluate_sensor_heartbeats()
                    self._last_health_check = now_monotonic

                # 3. Nightly window processing clock evaluator
                if now_datetime.hour == 0 and now_datetime.minute == 15:
                    if not self._recovery_executed_today:
                        await self._process_nightly_recovery(now_datetime)
                        self._recovery_executed_today = True
                else:
                    if now_datetime.minute != 15:
                        self._recovery_executed_today = False

        except asyncio.CancelledError:
            raise

    async def _execute_startup_checkup(self) -> None:
        """
        Private startup check querying database milestones per active sensor profile.
        """
        from datetime import timedelta
        from main import database_batcher, sensor_registry
        log_msg("INFO", "[WATCHDOG] Initiating granulated startup checkup sequence per sensor...")
        try:
            last_sensor_times = await database_batcher.get_last_timestamps_per_sensor()
            cached_sensors = await sensor_registry.get_all_cached_sensors()
            now_datetime = datetime.now(timezone.utc)

            for ble_id, state in cached_sensors.keys():
                await self._trigger_history_recovery(ble_id)
                await asyncio.sleep(1.0)

        except Exception as e:
            log_msg("ERROR", f"[WATCHDOG ERROR] Startup sync evaluation collapsed: {e}")

    async def _evaluate_sensor_heartbeats(self) -> None:
        """
        Private periodic handler verifying live incoming peripheral signal freshness.
        """
        try:
            from main import sensor_registry
            cached_sensors = await sensor_registry.get_all_cached_sensors()
            for ble_id, state in cached_sensors.items():
                if time.monotonic() - state["last_seen_timestamp"] > 60:
                    await sensor_registry.flag_data_gap(ble_id, True)

        except Exception as e:
            log_msg("ERROR", f"[WATCHDOG ERROR] Heartbeat verification step failed: {e}")

    async def _process_nightly_recovery(self, current_time: datetime) -> None:
        """
        Private chronological task invoking targeted active dumps on standard data holes.
        """
        from datetime import timedelta
        from main import sensor_registry
        try:
            cached_sensors = await sensor_registry.get_all_cached_sensors()
            for ble_id, state in cached_sensors.items():
                if state.get("has_data_gap"):
                    yesterday = current_time.date() - timedelta(days=1)
                    await self._trigger_history_recovery(ble_id, yesterday)
                    await sensor_registry.flag_data_gap(ble_id, False)

        except Exception as e:
            log_msg("ERROR", f"[WATCHDOG ERROR] Nightly recovery window execution aborted: {e}")

    async def _trigger_history_recovery(self, ble_id: str) -> None:
        """
        Private Routine: Connects via active GATT using tpy357 wrapper, extracts
        the data logs block, and flushes missing records into TimescaleDB.
        """
        import main
        import tpy357
        from bleak import BleakScanner

        cached_sensors = await main.sensor_registry.get_all_cached_sensors()
        sensor_meta = cached_sensors.get(ble_id, {})
        ble_address = sensor_meta.get("ble_address")
        sensor_db_id = sensor_meta.get("sensor_db_id")

        # Guard Clause: Enforce address existence (requires at least one passive advertisement packet captured before)
        if not ble_address:
            log_msg("WARN", f"[RECOVERY] Aborting history sync for {ble_id}. No physical hardware address mapped yet.")
            return

        # Enforce mutual exclusion to prevent concurrent active sessions on the same BLE dongle
        async with self._ble_active_lock:
            try:
                log_msg("INFO", f"[RECOVERY] Locating peripheral hardware for {ble_id} ({ble_address})...")
                device = await BleakScanner.find_device_by_address(ble_address, timeout=8.0)

                if not device:
                    log_msg("WARN", f"[RECOVERY] Sensor {ble_id} is currently unavailable or out of range.")
                    return

                log_msg("INFO", f"[RECOVERY] Active connection opened. Downloading minute-precision logs via tpy357...")
                # Requesting 'day' mode since it fetches high-granularity minute-precision packets natively
                raw_history_data = await asyncio.wait_for(
                    tpy357.query_tp357(dev=device, mode="day"),
                    timeout=45.0
                )

                if not raw_history_data:
                    log_msg("INFO", f"[RECOVERY] Memory flash returned no logged events for sensor {ble_id}.")
                    return

                # Fetch milestones boundaries from public metrics tracking partitions
                last_timestamps = await main.database_batcher.get_last_timestamps_per_sensor()
                last_db_record_time = last_timestamps.get(sensor_db_id)

                history_buffer = []
                for record in raw_history_data:
                    record_time = record["time"]
                    if record_time.tzinfo is None:
                        record_time = record_time.replace(tzinfo=timezone.utc)

                    # Filter: Stream directly if table is empty or record is strictly newer than current milestone
                    if last_db_record_time is None or record_time > last_db_record_time:
                        history_buffer.append({
                            "time": record_time.replace(second=0, microsecond=0),
                            "sensor_id": sensor_db_id,
                            "ble_id": ble_id,
                            "temperature": round(record["temp"], 2),
                            "humidity_raw": round(record["hum_rh"], 2),
                            "battery_raw": 100  # Baseline tracking metric fallback
                        })

                if history_buffer:
                    log_msg("INFO", f"[RECOVERY] Sync success: Found {len(history_buffer)} missing minutes for {ble_id}. Flushing to DB...")
                    await main.database_batcher.insert_measures(history_buffer)
                else:
                    log_msg("INFO", f"[RECOVERY] Database for sensor {ble_id} is already synchronized with device storage.")

            except Exception as e:
                log_msg("ERROR", f"[RECOVERY ERROR] Active synchronization pipeline failed for device {ble_id}: {e}")
