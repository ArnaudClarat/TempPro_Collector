import asyncio
import struct
from bleak import BleakScanner
from decoder import decode_tp357

def callback(device, advertisement_data):
    if not device.name or "TP357" not in device.name:
        return

    sensor_id = device.name.split("(")[-1].replace(")", "")
	
    for k, v in advertisement_data.manufacturer_data.items():
        decoded = decode_tp357(k, v)
        print(decoded)

async def main():
    print("Scan BLE en cours... (Ctrl+C pour arrêter)")

    scanner = BleakScanner(callback)
    await scanner.start()

    try:
        while True:
            await asyncio.sleep(1)

    finally:
        try:
            await scanner.stop()
        except:
            pass

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nArrêt demandé par l'utilisateur.")