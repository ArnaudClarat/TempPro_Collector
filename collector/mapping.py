import time, asyncio, json
from typing import Dict, Any

import db
from logger import log_msg

# Global in-memory cache tracking active sensors state and runtime metrics
_mapping_cache: Dict[str, Dict[str, Any]] = {}
_lock_mapping = asyncio.Lock()

def read_as_json(data: any) -> str:
    """Converts any dictionary or list into a formatted JSON string, handling datetime objects safely."""
    # The lambda function acts as the inline serializer, checking for 'isoformat'
    return json.dumps(data, indent=2, ensure_ascii=False, default=lambda o: o.isoformat() if hasattr(o, 'isoformat') else str(o))


async def load_mapping() -> Dict[str, Dict[str, Any]]:
    """
    Loads all active sensors and assignments from the database into the RAM cache.
    Optimized to bypass database queries if the cache is already initialized.
    """
    global _mapping_cache
    
    if _mapping_cache:
        return _mapping_cache

    if db.pool is None:
        log_msg("WARN", "[MAPPING] Database pool uninitialized, skipping cache load.")
        return {}

    try:
        async with db.pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    SELECT 
                        s.ble_id, 
                        s.id AS sensor_db_id,
                        l.id AS location_id,
                        l.name AS location_name,
                        sa.assigned_at
                    FROM public.sensors s
                    JOIN public.sensor_assignments sa ON s.id = sa.sensor_id
                    JOIN public.locations l ON sa.location_id = l.id
                    WHERE sa.removed_at IS NULL;
                    """
                )
                rows = await cur.fetchall()
                
                for ble_id, sensor_db_id, location_id, location_name, assigned_at in rows:
                    _mapping_cache[ble_id] = {
                        "sensor_db_id": sensor_db_id,
                        "location_id": location_id,
                        "location_name": location_name,
                        "assigned_at": assigned_at,
                        "has_data_gap": False,
                        "last_seen_timestamp": time.monotonic()
                    }
                
                # Re-aligned with your exact uppercase logging schema
                log_msg("INFO", f"[MAPPING] Successfully cached {len(_mapping_cache)} active sensors.")
                #log_msg("INFO", f"[MAPPING] List of cached sensors : {read_as_json(_mapping_cache)}")
    
    except Exception as e:
        log_msg("ERROR", f"[MAPPING ERROR] Failed to load schema mapping from database: {e}")
        raise e

    return _mapping_cache


async def get_sensor(ble_id: str) -> dict:
    """
    Retrieves a sensor record from the RAM cache.
    If the device is unknown, it triggers an automated database registration 
    and updates the memory cache dynamically for subsequent lookups.
    """
    async with _lock_mapping:
        mapping = await load_mapping()

        if ble_id not in mapping:
            log_msg("Warning", f"[MAPPING] Unknown sensor detected ({ble_id}). Initiating auto-registration...")
            try:
                sensor_db_id = await db.insert_sensor(ble_id)
                
                mapping[ble_id] = {
                    "sensor_db_id": sensor_db_id,
                    "has_data_gap": False,
                    "last_seen_timestamp": time.monotonic()
                }
                log_msg("Warning", f"[MAPPING] Sensor {ble_id} registered with internal database ID: {sensor_db_id}")
            
            except Exception as e:
                log_msg("Error", f"[MAPPING ERROR] Automated registration failed for device {ble_id}: {e}")
                raise e
        else:
            mapping[ble_id]["last_seen_timestamp"] = time.monotonic()

    return mapping[ble_id]

async def evict_sensor(ble_id: str) -> None:
    """
    Removes a sensor from the RAM cache instantly.
    Typically invoked by the Watchdog routine upon permanent connection failure.
    """
    async with _lock_mapping:
        removed = _mapping_cache.pop(ble_id, None)
        if removed:
            log_msg("Warning", f"[MAPPING] Sensor {ble_id} (ID: {removed['sensor_db_id']}) evicted from cache.")

async def flag_data_gap(ble_id: str, has_gap: bool) -> None:
    """
    Updates the data gap state flag for a specific tracked sensor in memory.
    """
    async with _lock_mapping:
        if ble_id in _mapping_cache:
            _mapping_cache[ble_id]["has_data_gap"] = has_gap

async def get_all_cached_sensors() -> Dict[str, Dict[str, Any]]:
    """
    Returns a safe shallow copy of the active sensors memory mapping cache.
    Prevents concurrent modification exceptions during asynchronous loops iterations.
    """
    async with _lock_mapping:
        return _mapping_cache.copy()
