# TempPro Collector

## Overview

**TempPro Collector** is a lightweight Python service that collects temperature and humidity data from ThermoPro TP357 Bluetooth Low Energy (BLE) sensors and prepares it for storage in a PostgreSQL database and visualization in Grafana.

It was built as a minimal, reliable pipeline for real-time environmental monitoring.

---

## Features

* BLE scanning of TP357 sensors (via `bleak`)
* Real-time decoding of temperature, humidity, and battery data
* Stable multi-sensor support
* Sensor ID mapping (renamable display names)
* Optional PostgreSQL persistence
* Grafana-ready time-series output structure
* Graceful shutdown (Ctrl+C safe)

---

## Architecture

```
TP357 Sensors (BLE)
        ↓
Python BLE Scanner (Bleak)
        ↓
Decoder (custom TP357 protocol)
        ↓
Python Collector
        ↓
[Optional] PostgreSQL Database
        ↓
Grafana Dashboard
```

---

## Requirements

* Python 3.11+
* Windows or Linux with BLE support
* Bluetooth enabled
* PostgreSQL (optional, for persistence)

---

## Installation

```bash
pip install bleak psycopg2-binary
```

---

## Usage

### Run collector

```bash
python main.py
```

Expected output:

```
Scan BLE en cours... (Ctrl+C pour arrêter)

{'temperature': 24.2, 'humidity': 46, 'battery_raw': 33, 'sensor_id': '3DE5'}
```

---

## Sensor Mapping

Sensors are identified using their hardware ID:

```python
SENSOR_MAP = {
    "3DE5": "Kitchen",
    "8AE3": "Bedroom children"
}
```

You can rename sensors freely without affecting historical data.

---

## Database (optional)

If PostgreSQL is enabled, the following table is expected:

```sql
CREATE TABLE measures (
    time TIMESTAMP DEFAULT now(),
    sensor_id TEXT,
    temperature REAL,
    humidity INT,
    battery INT
);
```

---

## Data Format

Each measurement contains:

```json
{
    "sensor_id": "3DE5",
    "temperature": 24.2,
    "humidity": 46,
    "battery_raw": 33
}
```

---

## Notes

* BLE advertising packets are used (no active connection required)
* Battery value is raw and may require calibration depending on device firmware
* Designed for continuous streaming into time-series databases

---

## Future Improvements

* Batch inserts to database
* MQTT support
* InfluxDB compatibility
* Web dashboard for sensor status
* Better battery calibration model

---

## License

For personal / educational use. Based on reverse-engineered TP357 BLE protocol behavior.
