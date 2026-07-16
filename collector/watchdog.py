import time, asyncio
from datetime import datetime, timezone

from config import EXECUTION_MODE
from logger import log_msg

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
