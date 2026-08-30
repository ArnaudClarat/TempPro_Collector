import struct, asyncio, logging
from typing import Dict, Any

from models import SensorMeasure

class MeasureParser:
    def __init__(self, registry):
        self.raw_data_queue: asyncio.Queue = asyncio.Queue()
        self.database_queue: asyncio.Queue = None
        self.registry = registry

    def decode_tp357(self, manufacturer_id: int, payload: bytes) -> Dict[str, Any]:
        """
        Decodes raw BLE manufacturer data bytes into standard numerical metrics.
        Utilizes binary unpacking via struct for optimal performance.
        """
        raw = struct.pack("<H4s", manufacturer_id, payload)
        temp_raw, humidity, battery = struct.unpack("=hBB", raw[1:5])

        return {
            "temperature": temp_raw / 10,
            "humidity_raw": humidity,
            "battery_raw": battery
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

                    sensor_metadata = await self.registry.get_sensor(ble_id)
                    sensor_db_id = sensor_metadata.sensor_db_id if sensor_metadata else 0

                    measure = SensorMeasure(
                        time=packet["time"],
                        sensor_id=sensor_db_id,
                        ble_id=ble_id,
                        temperature=round(float(decoded["temperature"]), 2),
                        humidity_raw=round(float(decoded["humidity_raw"]), 2),
                        battery_raw=decoded.get("battery_raw", 100)
                    )

                    logging.info(f"[FUNNEL] Decoded data => Sensor: {measure.ble_id} | Temp: {measure.temperature}°C | Hum: {measure.humidity_raw}%")

                    if self.database_queue:
                        await self.database_queue.put(measure)

                except Exception as e:
                    logging.error(f"[DECODER] Failed to decode packet: {e}")
                finally:
                    self.raw_data_queue.task_done()

        except asyncio.CancelledError:
            logging.warning("[DECODER] Worker pipeline shutdown signal received.")
            raise
