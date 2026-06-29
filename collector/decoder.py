import struct

def decode_tp357(manufacturer_id: int, payload: bytes):
    raw = struct.pack("<H4s", manufacturer_id, payload)

    temp_raw, humidity, battery = struct.unpack("=hBB", raw[1:5])

    return {
        "temperature": temp_raw / 10,
        "humidity": humidity,
        "battery_raw": battery
    }