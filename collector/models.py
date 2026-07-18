from dataclasses import dataclass
from datetime import datetime

@dataclass(slots=True)
class SensorMeasure:
    """
    Represents a raw environmental reading collected from a ThermoPro S-Series hardware sensor.
    Optimized with slots to minimize memory footprint during high-volume GATT historical backlogs.
    """
    time: datetime
    sensor_id: int
    ble_id: str
    temperature: float
    humidity_raw: float
    battery_raw: int = 100


@dataclass(slots=True)
class SensorMetadata:
    """
    Represents the internal hardware configuration and registered database identity
    of a physical deployment site.
    """
    sensor_db_id: int
    mac_address: str
    location_name: str
