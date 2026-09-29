from dataclasses import dataclass
from datetime import datetime


@dataclass(slots=True)
class Sensor:
    """
    Represents a raw environmental reading collected from a ThermoPro S-Series hardware sensor.
    Optimized with slots to minimize memory footprint during high-volume GATT historical backlogs.
    """
    db_id: int
    ble_id: str
    mac_address: str = None
    location_name: str = None
    last_seen_timestamp: float = None
    has_data_gap: bool = False
    history_catchup_completed: bool = False


@dataclass(slots=True)
class Measure:
    """
    Represents the internal hardware configuration and registered database identity
    of a physical deployment site.
    """
    time: datetime
    temperature: float
    humidity_raw: float
    sensor_id: int = None
    sensor: Sensor = None
    ble_id: str = None
    battery_raw: int = 100