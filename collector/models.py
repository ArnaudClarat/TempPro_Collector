import statistics
from dataclasses import dataclass, field
from datetime import datetime
from typing import List


@dataclass(slots=True)
class Sensor:
    """
    Represents a raw environmental reading collected from a ThermoPro S-Series hardware sensor.
    Optimized with slots to minimize memory footprint during high-volume GATT historical backlogs.
    """
    ble_id: str
    db_id: int = None
    mac_address: str = None
    location_name: str = None
    last_ble_received_monotonic: float = None
    history_cursor_timestamp: datetime = None
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
    humidity: float
    sensor_id: int = None
    sensor: Sensor = None
    ble_id: str = None
    battery_raw: int = None


@dataclass(slots=True)
class Bucket:
    """
    Represents a 1-minute time window bucket collecting raw measures 
    for a specific sensor before database aggregation.
    """
    ble_id: str
    minute_start: datetime
    sensor: Sensor
    temps: List[float] = field(default_factory=list)
    hums: List[float] = field(default_factory=list)
    batteries: List[int] = field(default_factory=list)

    def add(self, measure: 'Measure') -> None:
        """Appends a raw measure's values into the bucket lists."""
        self.temps.append(measure.temperature)
        self.hums.append(measure.humidity)
        self.batteries.append(measure.battery_raw)

    def compute_aggregated_measure(self) -> 'Measure':
        """
        Consolidates bucket data into a single averaged/medianed Measure 
        ready to be flushed into the database queue.
        """
        avg_temp = round(sum(self.temps) / len(self.temps), 2) if self.temps else 0.0
        avg_hum = round(sum(self.hums) / len(self.hums), 2) if self.hums else 0.0
        
        valid_batteries = [b for b in self.batteries if b is not None]
        median_battery = int(statistics.median(valid_batteries)) if valid_batteries else None

        return Measure(
            time=self.minute_start,
            sensor=self.sensor,
            sensor_id=self.sensor.db_id if self.sensor else None,
            ble_id=self.ble_id,
            temperature=avg_temp,
            humidity=avg_hum,
            battery_raw=median_battery
        )