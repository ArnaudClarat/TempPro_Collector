import logging
import statistics
import asyncio
from datetime import datetime, timezone, timedelta
from typing import Dict, Tuple, Optional

from models import Measure, Bucket

CLOSE_DELAY_SECONDS = 2

class BucketsManager:
    """
    Manages per-minute time-window buckets, ensuring reference uniqueness 
    per sensor and minute, and handles aggregation flushing to the database queue.
    """
    def __init__(self):
        """Maps (ble_id, minute_start) -> unique Bucket instance reference"""
        self._minute_buckets: Dict[Tuple[str, datetime], Bucket] = {}
        self.database_queue = None
        self.on_catchup_needed = None

    def add_measure(self, measure: Measure) -> None:
        """Adds a measure into its corresponding per-minute bucket."""
        minute_start = measure.time.replace(second=0, microsecond=0)
        bucket = self._minute_buckets.setdefault(
            (measure.ble_id, minute_start),
            Bucket(
                ble_id=measure.ble_id,
                minute_start=minute_start,
                sensor=measure.sensor))
        bucket.add(measure)

    async def close_finished_minutes(self, now: datetime | None = None) -> None:
        """
        Evaluates active buckets, pops those whose time window has expired past the limit,
        computes their aggregated metrics, and pushes the resulting Measure batch to the DB queue,
        provided the corresponding sensor has completed its historical catch-up.
        """
        now = now or datetime.now(timezone.utc)
        limit = timedelta(seconds=60 + CLOSE_DELAY_SECONDS)

        # Identify buckets older than 1 minute + tolerance delay
        finished = [key for key in self._minute_buckets if key[1] + limit <= now]
        if not finished:
            if self._minute_buckets:
                oldest_key = min(self._minute_buckets.keys(), key=lambda k: k[1])
                trigger_time = oldest_key[1] + limit
                logging.debug(f"[BUCKETS] Waiting... Oldest bucket {oldest_key[1].strftime('%H:%M:%S')} needs {trigger_time.strftime('%H:%M:%S')} (Now: {now.strftime('%H:%M:%S')})")
            return

        logging.info(f"[BUCKETS] Time's up! Closing {len(finished)} finished bucket(s).")
        rows: list[Measure] = []
        
        for key in sorted(finished, key=lambda k: (k[1], k[0])):
            bucket = self._minute_buckets.pop(key)
            measure = bucket.compute_aggregated_measure()

            sensor = getattr(measure, 'sensor', None)
            if sensor and not getattr(sensor, 'history_catchup_completed', False):
                logging.warning(f"[BUCKETS] Skipping live bucket for {sensor.location_name} ({sensor.ble_id}) -> Catch-up not completed yet.")
                if self.on_catchup_needed:
                    await self.on_catchup_needed(sensor)
            else:
                rows.append(measure)

        if self.database_queue and rows:
            await self.database_queue.put(rows)
            logging.debug(f"[BUCKETS] Flushed {len(rows)} aggregated minute(s) to database queue.")

    async def flush_all(self) -> None:
        """Forces the closure and flushing of all remaining buckets during shutdown."""
        if not self._minute_buckets:
            return

        rows: list[Measure] = []
        for key in sorted(self._minute_buckets.keys(), key=lambda k: (k[1], k[0])):
            bucket = self._minute_buckets.pop(key)
            measure = bucket.compute_aggregated_measure()
            
            sensor = getattr(measure, 'sensor', None)
            if sensor and not getattr(sensor, 'history_catchup_completed', False):
                logging.warning(f"[BUCKETS] Skipping shutdown bucket for {sensor.location_name} ({sensor.ble_id}) -> Catch-up not completed yet.")
            else:
                rows.append(measure)

        if self.database_queue and rows:
            await self.database_queue.put(rows)
            logging.info(f"[BUCKETS] Shutdown flush: {len(rows)} remaining bucket(s) flushed.")
        
        self._minute_buckets.clear()