import struct, asyncio, logging, re, time
from typing import Dict, Any
from datetime import datetime

from models import Measure, Sensor
from buckets import BucketsManager


class MeasureParser:
    def __init__(self, registry):
        self.raw_data_queue: asyncio.Queue = asyncio.Queue()
        self.database_queue: asyncio.Queue = None
        self.registry = registry
        self.buckets_manager = None

    def decode_tp357(self, manufacturer_id: int, payload: bytes) -> Dict[str, Any]:
        """
        Decodes raw BLE manufacturer data bytes into standard numerical metrics.
        Utilizes binary unpacking via struct for optimal performance.
        """
        raw = struct.pack("<H4s", manufacturer_id, payload)
        temp_raw, humidity, battery_raw = struct.unpack("=hBB", raw[1:5])

        return {
            "temperature": temp_raw / 10,
            "humidity": humidity,
            "battery_raw": battery_raw
        }

    async def decoder_loop(self) -> None:
        """
        Asynchronous worker that consumes raw BLE packets, decodes them,
        and prepares them for the database funnel.
        """
        logging.info("[DECODER] Asynchronous worker pipeline initialized.")

        try:
            while True:
                # Wait for a raw packet from the scanner callback
                packet = await self.raw_data_queue.get()
                try:
                    # Import for the queue
                    # Decode the raw bytes into human-readable metrics
                    decoded = self.decode_tp357(packet['manufacturer_id'], packet['payload'])
                    ble_id = packet['ble_id']
                    if not ble_id or not re.match(r"^[0-9A-Fa-f]{4}$", str(ble_id)):
                        logging.warning(f"[DECODER] Ignored packet with missing or empty ble_id: {packet}")
                        continue
                    mac_address = packet['mac_address']
                    sensor = await self.registry.get_sensor(ble_id, mac_address)
                    sensor.last_ble_received_monotonic = packet["monotonic_time"]

                    measure = Measure(
                        time=packet["time"],
                        sensor=sensor,
                        ble_id=ble_id,
                        temperature=round(float(decoded["temperature"]), 2),
                        humidity=round(float(decoded["humidity"]), 2),
                        battery_raw=decoded["battery_raw"]
                    )

                    logging.debug(f"[DECODER] Decoded data => Sensor: {measure.ble_id} | Temp: {measure.temperature}°C | Hum: {measure.humidity}%")

                    self.buckets_manager.add_measure(measure)
                    await self.buckets_manager.close_finished_minutes()

                except Exception as e:
                    logging.error(f"[DECODER] Failed to decode packet: {e}")
                finally:
                    self.raw_data_queue.task_done()

        except asyncio.CancelledError:
            logging.warning("[DECODER] Worker pipeline shutdown signal received.")
            await self.buckets_manager.flush_all()
            raise
