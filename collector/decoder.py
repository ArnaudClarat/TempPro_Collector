import struct, asyncio
from typing import Dict, Any
import db, mapping
from logger import log_msg

# Queue 1: Buffer that receives raw advertisement payloads from the BleScanner
raw_data_queue: asyncio.Queue = asyncio.Queue()

def decode_tp357(manufacturer_id: int, payload: bytes) -> Dict[str, Any]:
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

# From your decoder.py file (Adjust to fit your exact worker loop)
async def decoder_worker() -> None:
    """
    Asynchronous worker that consumes raw BLE packets, decodes them, 
    and prepares them for the database funnel.
    """
    log_msg("INFO", "[DECODER] Asynchronous worker pipeline initialized.")
    
    try:
        while True:
            # Wait for a raw packet from the scanner callback
            packet = await raw_data_queue.get()
            
            try:
                # Decode the raw bytes into human-readable metrics
                decoded = decode_tp357(packet['manufacturer_id'], packet['payload'])
                
                # LOG PIPELINE FUNNEL (Triggers perfectly in MOCK_INSERT and FULL_PRODUCTION)
                log_msg("INFO", f"[FUNNEL] Decoded data => Sensor: {packet['ble_id']} | Temp: {decoded['temperature']}°C | Hum: {decoded['humidity_raw']}%")
                
                # Next step: push 'decoded' to db_queue for insertion
                # db_queue.put_nowait(decoded_packet)
                
            except Exception as decode_error:
                log_msg("ERROR", f"[DECODER] Failed to decode packet: {decode_error}")
            finally:
                raw_data_queue.task_done()
                
    except asyncio.CancelledError:
        log_msg("INFO", "[DECODER] Worker pipeline shutdown signal received.")


    except asyncio.CancelledError:
        log_msg("Warning", "[DECODER] Worker pipeline shutdown signal received.")
        raise
