import time, asyncio, json, logging
from typing import List, Dict, Any, Optional

from models import SensorMetadata

class SensorRegistry:
    _mapping_cache: Dict[str, Dict[str, Any]] = {}
    _lock_mapping = asyncio.Lock()

    def __init__(self, database_batcher):
        self.db = database_batcher

    @staticmethod
    def _read_as_json(data: any) -> str:
        """Converts any dictionary or list into a formatted JSON string, handling datetime objects safely."""
        # The lambda function acts as the inline serializer, checking for 'isoformat'
        return json.dumps(data, indent=2, ensure_ascii=False, default=lambda o: o.isoformat() if hasattr(o, 'isoformat') else str(o))


    async def load_mapping(self) -> Dict[str, Dict[str, Any]]:
        """
        Loads all active sensors and assignments from the database into the RAM cache.
        Optimized to bypass database queries if the cache is already initialized.
        """
        if SensorRegistry._mapping_cache:
            return SensorRegistry._mapping_cache

        if self.db.pool is None:
            logging.warning("[MAPPING] Database pool uninitialized, skipping cache load.")
            return {}

        try:
            async with self.db.pool.connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        """
                        SELECT
                            s.ble_id,
                            s.id AS sensor_db_id,
                            s.mac_address AS mac_address,
                            l.id AS location_id,
                            l.name AS location_name,
                            sa.assigned_at,
                            sa.removed_at
                        FROM sensors s
                        JOIN sensor_assignments sa ON s.id = sa.sensor_id
                        JOIN locations l ON sa.location_id = l.id
                        WHERE sa.removed_at IS NULL;
                        """
                    )
                    rows = await cur.fetchall()

                    SensorRegistry._mapping_cache = {}

                    for ble_id, sensor_db_id, mac_address, location_id, location_name, assigned_at, removed_at in rows:
                        SensorRegistry._mapping_cache[ble_id] = {
                            "sensor_db_id": sensor_db_id,
                            "mac_address": mac_address,
                            "location_id": location_id,
                            "location_name": location_name,
                            "assigned_at": assigned_at,
                            "removed_at": removed_at,
                            "has_data_gap": False,
                            "last_seen_timestamp": time.monotonic()
                        }

                    logging.info(f"[MAPPING] Successfully cached {len(SensorRegistry._mapping_cache)} active sensors.")
                    logging.debug(f"[MAPPING] List of cached sensors : {SensorRegistry._read_as_json(SensorRegistry._mapping_cache)}")

        except Exception as e:
            logging.error(f"[MAPPING ERROR] Failed to load schema mapping from database: {e}")
            raise e

        return SensorRegistry._mapping_cache


    async def get_sensor(self, ble_id: str, mac_address: Optional[str] = None) -> SensorMetadata:
        """
        Retrieves a sensor record from the RAM cache.
        If the device is unknown, it triggers an automated database registration
        and updates the memory cache dynamically for subsequent lookups.
        """
        async with SensorRegistry._lock_mapping:
            mapping_data = await self.load_mapping()

            if ble_id not in mapping_data:
                logging.warning(f"[MAPPING] Unknown sensor detected ({ble_id}). Initiating auto-registration...")
                try:
                    sensor_db_id = await self.db.insert_sensor(ble_id, mac_address)

                    mapping_data[ble_id] = {
                        "sensor_db_id": sensor_db_id,
                        "mac_address": mac_address,
                        "location_name": "Unknown",
                        "has_data_gap": False,
                        "last_seen_timestamp": time.monotonic()
                    }
                    logging.warning("Warning", f"[MAPPING] Sensor {ble_id} registered with internal database ID: {sensor_db_id}")

                except Exception as e:
                    logging.error(f"[MAPPING ERROR] Automated registration failed for device {ble_id}: {e}")
                    raise e
            else:
                mapping_data[ble_id]["last_seen_timestamp"] = time.monotonic()
                if mac_address and not mapping_data[ble_id].get("mac_address"):
                    mapping_data[ble_id]["mac_address"] = mac_address

            raw_info = mapping_data[ble_id]
            return SensorMetadata(
                sensor_db_id=raw_info["sensor_db_id"],
                mac_address=raw_info.get("mac_address", ""),
                location_name=raw_info.get("location_name", "Unknown")
            )

    async def evict_sensor(self, ble_id: str) -> None:
        """
        Removes a sensor from the RAM cache instantly.
        Typically invoked by the Watchdog routine upon permanent connection failure.
        """
        async with SensorRegistry._lock_mapping:
            removed = SensorRegistry._mapping_cache.pop(ble_id, None)
            if removed:
                logging.warning(f"[MAPPING] Sensor {ble_id} (ID: {removed['sensor_db_id']}) evicted from cache.")

    async def flag_data_gap(self, ble_id: str, has_gap: bool) -> None:
        """
        Updates the data gap state flag for a specific tracked sensor in memory.
        """
        async with SensorRegistry._lock_mapping:
            if ble_id in SensorRegistry._mapping_cache:
                SensorRegistry._mapping_cache[ble_id]["has_data_gap"] = has_gap

    async def get_all_cached_sensors(self) -> Dict[str, Dict[str, Any]]:
        """
        Returns a safe shallow copy of the active sensors memory mapping cache.
        Prevents concurrent modification exceptions during asynchronous loops iterations.
        """
        async with SensorRegistry._lock_mapping:
            return SensorRegistry._mapping_cache.copy()
