import os, sys, time, asyncio
from datetime import datetime, timezone
from pathlib import Path
from dotenv import load_dotenv
from bleak import BleakScanner

from config import EXECUTION_MODE
from logger import log_msg
from decoder import MeasureParser
from db import DatabaseBatcher
from mapping import SensorRegistry
from watchdog import Watchdog

watchdog = Watchdog()
sensor_registry = SensorRegistry()
database_batcher = DatabaseBatcher()
measure_parser = MeasureParser()

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

class BleScanner:
    def __init__(self):
        self._scanner: Optional[BleakScanner] = None

    def callback(self, device, advertisement_data):
        """
        Filters incoming BLE advertisements from ThermoPro sensors and processes payloads.
        """
        # Quick guard clause: filter out any device that isn't a ThermoPro
        device_name = advertisement_data.local_name or ""
        if "8AE3" not in device_name: #Replace by "TP357" when all tests are finished
            return

        # Extract manufacturer payload (skip empty background advertisements safely)
        for manufacturer_id, payload in advertisement_data.manufacturer_data.items():
            arrival_time = datetime.now(timezone.utc)

            # Robust ID extraction: use MAC address suffix if name split fails
            ble_id = device_name.split("(")[-1].replace(")", "") if "(" in device_name else device.address[-4:].replace(":", "")

            if EXECUTION_MODE == "OFFLINE_SIMULATION":
                # Direct streaming to stdout for standalone local validation
                decoded = measure_parser.decode_tp357(manufacturer_id, payload)
                log_msg("INFO", f"[OFFLINE TEST] Sensor: {ble_id} | Temp: {decoded['temperature']}°C | Hum: {decoded['humidity_raw']}%")
            else:
                # Production pipeline: forward raw packet to the async decoder queue
                packet = {
                    "ble_id": ble_id,
                    "manufacturer_id": manufacturer_id,
                    "payload": payload,
                    "time": arrival_time
                }
                log_msg("INFO", f"[CALLBACK] Verified packet captured for sensor: {ble_id} -> Pushing to decoder queue.")
                measure_parser.raw_data_queue.put_nowait(packet)

    async def start_scanning(self, stop_event: asyncio.Event) -> None:
        """
        Mounts the BleakScanner context manager onto the processing loop thread.
        """
        self._scanner = BleakScanner(detection_callback=self.callback)
        log_msg("INFO", "[SYSTEM] BLE Ingestion Engine online. Monitoring background packets...")
        await self._scanner.start()
        await stop_event.wait()
        await self._scanner.stop()

async def main():
    """
    Main pipeline entrypoint provisioning pools and background streaming threads.
    Features an auto-restart loop to bypass Windows 11 hardware scanning timeout constraints.
    """
    log_msg("Info", f"Active mode : [{EXECUTION_MODE}] (Ctrl+C to terminate)")

    db_worker_task = None
    decoder_task = None
    watchdog_task = None

    # Initialize data infrastructure according to selected execution criteria
    if EXECUTION_MODE != "OFFLINE_SIMULATION":
        await database_batcher.init_db()
        await sensor_registry.load_mapping()

        # Concurrently fire pipeline background processing threads
        db_worker_task = asyncio.create_task(database_batcher.db_worker())
        decoder_task = asyncio.create_task(measure_parser.decoder_loop())
        watchdog_task = asyncio.create_task(watchdog.start_worker())

    stop_event = asyncio.Event()

    def ask_for_shutdown():
        log_msg("INFO", "[SYSTEM] Intercepted Ctrl+C signal! Releasing main thread for cleanup...")
        stop_event.set()

    loop = asyncio.get_running_loop()
    if sys.platform != "win32":
        import signal
        loop.add_signal_handler(signal.SIGINT, ask_for_shutdown)
        loop.add_signal_handler(signal.SIGTERM, ask_for_shutdown)

    # Mount the BleakScanner context manager
    scanner = BleScanner()
    try:
        await scanner.start_scanning(stop_event)
        await stop_event.wait()

    except KeyboardInterrupt:
        # Fallback for Windows CLI manual interruption
        ask_for_shutdown()

    except Exception as runtime_error:
        log_msg("ERROR", f"[SYSTEM CRITICAL BREAKDOWN] Main loop collapsed unexpectedly: {runtime_error}")

    finally:
        # Trigger clean shutdown loop
        log_msg("INFO", "[SYSTEM] Shutdown instruction detected. Tearing down background tasks...")

        tasks_to_cancel = [t for t in [watchdog_task, decoder_task, db_worker_task] if t is not None]
        if tasks_to_cancel:
            for task in tasks_to_cancel:
                task.cancel()
            await asyncio.gather(*tasks_to_cancel, return_exceptions=True)

        if EXECUTION_MODE != "OFFLINE_SIMULATION":
            await database_batcher.close_db()
        log_msg("INFO", "Subsystems terminated cleanly.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
