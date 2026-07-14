import asyncio
from datetime import datetime, timedelta, timezone
import struct
from bleak import BleakClient

# GATT Characteristic Handles extracted via Wireshark for the 'S' revision
WRITE_HANDLE = 0x000B  # Command reception handle (Write Command)
NOTIFY_HANDLE = 0x0008  # Historical records stream handle (Notification)


def _generate_auth_payload() -> bytes:
    """Generates the mandatory 10-byte time synchronization payload.

    Without this sequence, the TP357S firmware ignores history dump
    requests.
    """
    now = datetime.now()

    header = 0xA5  # Fixed ThermoPro protocol header
    opcode = 0x1A  # Time sync and session initialization opcode
    year = (now.year - 2019) & 0xFF  # Epoch offset used by the hardware firmware
    month = now.month
    day = now.day
    hour = now.hour
    minute = now.minute
    second = now.second
    weekday = (now.weekday() + 1) & 0xFF  # Protocol format: Monday = 1
    status_flag = 0x01  # Status/DST baseline tracking metric fallback
    footer = 0x5A  # Fixed command termination marker

    return bytes(
        [
            header,
            opcode,
            year,
            day,  # Note: The 'S' revision flips day and month order in this payload
            month,
            hour,
            minute,
            second,
            weekday,
            status_flag,
            footer,
        ]
    )


async def query_tp357(dev, mode: str = "day") -> list[dict]:
    """Asynchronous API connection pipeline to extract flash memory data blocks

    from a ThermoPro TP357S peripheral hardware. Mimics the legacy tpy357
    library signature for seamless drop-in replacement.
    """
    raw_received_data = bytearray()
    transfer_finished_event = asyncio.Event()

    # Protocol control framing constants identified in the Wireshark stream
    PREFIX_DATA = b"\xcc\xcc\x01\xbb"
    PREFIX_REPLY = b"\xc2\x00\x00"
    FOOTER_EOF = b"\xde\x66\x66"

    def notification_callback(sender: int, data: bytearray):
        """Internal GATT notification collector callback."""
        nonlocal raw_received_data

        # Ignore standalone acknowledgment and handshake validation frames
        if data.startswith(PREFIX_REPLY) or data.startswith(b"\xa5\x01"):
            return

        # If it's the start of a data stream chunk, strip the 10-byte cccc framing header
        if data.startswith(PREFIX_DATA):
            raw_received_data.extend(data[10:])
            return

        # Append subsequent sequential raw data segments
        raw_received_data.extend(data)

        # Trigger completion milestone when the End-of-File marker is captured
        if data.endswith(FOOTER_EOF):
            transfer_finished_event.set()

    # Leverage the connected Bleak device instance provided by the caller
    async with BleakClient(dev) as client:
        if not client.is_connected:
            raise RuntimeError("GATT session establishment failed.")

        # 1. Subscribe to the hardware notification descriptor handle
        await client.start_notify(NOTIFY_HANDLE, notification_callback)

        # 2. Transmit the time synchronization authentication token to wake up the flash register
        auth_payload = _generate_auth_payload()
        await client.write_gatt_char(
            WRITE_HANDLE, auth_payload, response=False
        )
        await asyncio.sleep(0.5)

        # 3. Request the raw history dump sequence (High-granularity minute-precision)
        cmd_dump = b"\xcc\xcc\x02\x01\x00\x00\x01\x04\x66\x66"
        await client.write_gatt_char(WRITE_HANDLE, cmd_dump, response=False)

        # 4. Await complete binary transmission block until EOF marker is raised
        try:
            await asyncio.wait_for(transfer_finished_event.wait(), timeout=40.0)
        except asyncio.TimeoutError:
            # Fallback evaluation: proceed to parsing if data truncation happened but payload exists
            if not raw_received_data:
                raise TimeoutError(
                    "Sensor device responded with an empty history matrix."
                )
        finally:
            await client.stop_notify(NOTIFY_HANDLE)

    # 5. DATA PARSING LAYER
    parsed_history = []

    # Strip the trailing EOF footer from the global byte buffer if present
    if raw_received_data.endswith(FOOTER_EOF):
        raw_received_data = raw_received_data[:-3]

    # Process the stream using 3-byte granulated slices: [Temp LSB, Temp MSB, Humidity]
    base_time = datetime.now(timezone.utc)
    total_triplets = len(raw_received_data) // 3

    for i in range(total_triplets):
        offset = i * 3
        triplet = raw_received_data[offset : offset + 3]

        # ThermoPro 'S' revision math formula: Little-Endian signed short ('<h'), scaled by 10
        temp_raw = struct.unpack("<h", triplet[0:2])[0]
        temperature = round(temp_raw / 10.0, 2)

        # Humidity is a standalone 1-byte unsigned integer ('B') at the end of the triplet
        humidity = int(triplet[2])

        # Reverse time axis generation: each index represents 1 minute backward from execution time
        record_time = base_time - timedelta(minutes=i)

        parsed_history.append(
            {"time": record_time, "temp": temperature, "hum_rh": humidity}
        )

    # Re-order the collection chronologically (oldest timestamps first)
    parsed_history.reverse()
    return parsed_history
