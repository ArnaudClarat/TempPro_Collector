import os, sys, time, asyncio
from datetime import datetime, timezone
from pathlib import Path
from dotenv import load_dotenv
from bleak import BleakScanner

from config import EXECUTION_MODE
from logger import log_msg
from decoder import decode_tp357, raw_data_queue, decoder_worker
from db import init_db, db_worker, close_db
from mapping import load_mapping

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

def callback(device, advertisement_data):
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
            decoded = decode_tp357(manufacturer_id, payload)
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
            raw_data_queue.put_nowait(packet)

async def watchdog_worker():
    """
    Background tracking watchdog layer. Bypassed in standalone offline testing.
    """
    if EXECUTION_MODE == "OFFLINE_SIMULATION":
        return

    last_health_check = time.monotonic()
    recovery_executed_today = False

    try:
        while True:
            await asyncio.sleep(1.0)
            now_monotonic = time.monotonic()
            now_datetime = datetime.now(timezone.utc)

            # 1. Daytime monitoring checklist evaluation interval (30 min)
            if now_monotonic - last_health_check >= 1800:
                cached_sensors = await mapping.get_all_cached_sensors()
                for ble_id, state in cached_sensors.items():
                    if now_monotonic - state["last_seen_timestamp"] > 60:
                        await mapping.flag_data_gap(ble_id, True)
                last_health_check = now_monotonic

            # 2. Nightly window processing clock evaluator
            if now_datetime.hour == 0 and now_datetime.minute == 15:
                if not recovery_executed_today:
                    cached_sensors = await mapping.get_all_cached_sensors()
                    for ble_id, state in cached_sensors.items():
                        if state["has_data_gap"]:
                            pass
                    recovery_executed_today = True
            else:
                recovery_executed_today = False

    except asyncio.CancelledError:
        raise

async def main():
    """
    Main pipeline entrypoint provisioning pools and background streaming threads.
    Features an auto-restart loop to bypass Windows 11 hardware scanning timeout constraints.
    """
    log_msg("Info", f"Mode actif : [{EXECUTION_MODE}] (Ctrl+C pour arrêter)")

    db_worker_task = None
    decoder_task = None
    watchdog_task = None

    # Initialize data infrastructure according to selected execution criteria
    if EXECUTION_MODE != "OFFLINE_SIMULATION":
        await init_db()
        await load_mapping()

        # Concurrently fire pipeline background processing threads
        db_worker_task = asyncio.create_task(db_worker())
        decoder_task = asyncio.create_task(decoder_worker())
        watchdog_task = asyncio.create_task(watchdog_worker())

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
    scanner = BleakScanner(detection_callback=callback)
    try:
        log_msg("INFO", "[SYSTEM] BLE Ingestion Engine online. Monitoring background packets...")
        await scanner.start()
        await stop_event.wait()

    except KeyboardInterrupt:
        # Fallback for Windows CLI manual interruption
        ask_for_shutdown()

    except Exception as runtime_error:
        log_msg("ERROR", f"[SYSTEM CRITICAL BREAKDOWN] Main loop collapsed unexpectedly: {runtime_error}")

    finally:
        # Trigger clean shutdown loop
        log_msg("INFO", "[SYSTEM] Shutdown instruction detected. Tearing down background tasks...")

        try:
            await scanner.stop()
        except Exception:
            pass

        tasks_to_cancel = [t for t in [watchdog_task, decoder_task, db_worker_task] if t is not None]
        if tasks_to_cancel:
            for task in tasks_to_cancel:
                task.cancel()
            await asyncio.gather(*tasks_to_cancel, return_exceptions=True)

        if EXECUTION_MODE != "OFFLINE_SIMULATION":
            await close_db()
        log_msg("INFO", "Subsystems terminated cleanly.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
