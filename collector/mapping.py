import time, asyncio, json, logging
from typing import List, Any, Optional

from models import Sensor

class SensorRegistry:
    def __init__(self, database_batcher):
        self.db = database_batcher
        self._lock_mapping = asyncio.Lock()
        self._mapping_cache: List[Sensor] = []

    @staticmethod
    def _read_as_json(data: any) -> str:
        """Converts any dictionary or list into a formatted JSON string, handling datetime objects safely."""
        # The lambda function acts as the inline serializer, checking for 'isoformat'
        return json.dumps(data, indent=2, ensure_ascii=False, default=lambda o: o.isoformat() if hasattr(o, 'isoformat') else str(o))


    async def load_mapping(self) -> List[Sensor]:
        """
        Loads all active sensors and assignments from the database into the RAM cache.
        Optimized to bypass database queries if the cache is already initialized.
        """
        if self._mapping_cache:
            return self._mapping_cache

        if self.db.pool is None:
            logging.warning("[MAPPING] Database pool uninitialized, skipping cache load.")
            await self.db.get_conn()
            return []

        try:
            async with self.db.pool.connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        """
                        SELECT
                            s.ble_id,
                            s.id AS db_id,
                            s.mac_address AS mac_address,
                            l.id AS location_id,
                            l.name AS location_name,
                            sa.assigned_at,
                            sa.removed_at
                        FROM sensors s
                        JOIN sensor_assignments sa ON s.id = sa.sensor_id
                        JOIN locations l ON sa.location_id = l.id
                        WHERE sa.removed_at IS NULL OR sa.removed_at > NOW();
                        """
                    )
                    rows = await cur.fetchall()

                    self._mapping_cache = []

                    for ble_id, db_id, mac_address, location_id, location_name, assigned_at, removed_at in rows:
                        self._mapping_cache.append(Sensor(
                            db_id=db_id,
                            ble_id=ble_id,
                            mac_address=mac_address,
                            location_name=location_name,
                            has_data_gap=False,
                            last_ble_received_monotonic=None,
                            history_catchup_completed=False
                        ))

                    logging.info(f"[MAPPING] Successfully cached {len(self._mapping_cache)} active sensors.")
                    logging.debug(f"[MAPPING] List of cached sensors : {SensorRegistry._read_as_json(self._mapping_cache)}")

        except Exception as e:
            logging.error(f"[MAPPING ERROR] Failed to load schema mapping from database: {e}")
            raise e
        return self._mapping_cache

    async def get_sensor(self, ble_id: str, mac_address: str | None = None) -> Sensor:
        """
        Retrieves a sensor record from the RAM cache.
        If the device is unknown, it triggers an automated database registration
        and updates the memory cache dynamically for subsequent lookups.
        """

        async with self._lock_mapping:
            mapping_data = await self.load_mapping()
            sensor = next((s for s in mapping_data if s.ble_id == ble_id), None)

            if sensor is not None:
                if mac_address and not sensor.mac_address:
                    sensor.mac_address = mac_address

            elif mac_address is not None:
                logging.warning(f"[MAPPING] Unknown sensor detected ({ble_id}). Initiating auto-registration...")
                try:
                    sensor = Sensor(ble_id=ble_id, mac_address=mac_address)
                    db_id = await self.db.insert_sensor(sensor)

                    sensor = Sensor(
                        db_id=db_id,
                        ble_id=ble_id,
                        mac_address=mac_address or "",
                        location_name="Unknown",
                        has_data_gap=False,
                        last_ble_received_monotonic=None,
                        history_catchup_completed=False
                    )
                    mapping_data.append(sensor)

                    logging.warning(f"[MAPPING] Sensor {ble_id} registered with internal database ID: {db_id}")

                except Exception as e:
                    logging.error(f"[MAPPING] Automated registration failed for device {ble_id}: {e}")
                    raise e
            else:
                raise ValueError(f"[MAPPING] No mac_address given")

            return sensor

    async def evict_sensor(self, sensor: Sensor) -> None:
        async with self._lock_mapping:
            if sensor in self._mapping_cache:
                self._mapping_cache.remove(sensor)
                logging.warning(f"[MAPPING] Sensor {sensor.ble_id} (ID: {sensor.db_id}) evicted from cache.")

    async def toggle_data_gap(self, sensor: Sensor) -> None:
        """
        Updates the data gap state flag for a specific tracked sensor in memory.
        """
        async with self._lock_mapping:
            if sensor in self._mapping_cache:
                sensor.has_data_gap = not sensor.has_data_gap

    async def get_sensors(self) -> List[Sensor]:
        """
        Returns a safe shallow copy of the active sensors memory mapping cache.
        Prevents concurrent modification exceptions during asynchronous loops iterations.
        """
        async with self._lock_mapping:
            return self._mapping_cache.copy()

    async def get_up_sensors(self) -> List[Sensor]:
        """
        Returns a safe shallow copy of the sensors who didn't finished their history catchup.
        Prevents concurrent modification exceptions during asynchronous loops iterations.
        """
        async with self._lock_mapping:
            return [s for s in self._mapping_cache if not s.history_catchup_completed]