import struct, asyncio
from typing import Dict, Any

import mapping
from db import DatabaseBatcher
from logger import log_msg

class MeasureParser:
    def __init__(self):
        self.raw_data_queue: asyncio.Queue = asyncio.Queue()

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
        log_msg("INFO", "[DECODER] Asynchronous worker pipeline initialized.")
        from main import database_batcher

        try:
            while True:
                # Wait for a raw packet from the scanner callback
                packet = await self.raw_data_queue.get()

                try:
                    # Decode the raw bytes into human-readable metrics
                    decoded = self.decode_tp357(packet['manufacturer_id'], packet['payload'])
                    ble_id = packet['ble_id']

                    payload_out = {
                        "ble_id": ble_id,
                        "temperature": decoded["temperature"],
                        "humidity_raw": decoded["humidity_raw"],
                        "battery_raw": decoded["battery_raw"],
                        "time": packet["time"]
                    }

                    # LOG PIPELINE FUNNEL (Triggers perfectly in MOCK_INSERT and FULL_PRODUCTION)
                    log_msg("INFO", f"[FUNNEL] Decoded data => Sensor: {payload_out['ble_id']} | Temp: {payload_out['temperature']}°C | Hum: {payload_out['humidity_raw']}%")

                    # Next step: push 'decoded' to db_queue for insertion
                    log_msg("DEBUG", f"[DECODER] Pushing to queue ID: {id(database_batcher.db_queue)}")
                    await database_batcher.db_queue.put(payload_out)

                except Exception as e:
                    log_msg("ERROR", f"[DECODER] Failed to decode packet: {e}")
                finally:
                    self.raw_data_queue.task_done()

        except asyncio.CancelledError:
            log_msg("Warning", "[DECODER] Worker pipeline shutdown signal received.")
            raise
