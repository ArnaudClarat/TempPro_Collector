"""
Local-time helpers shared by every part of the project that has to turn a local wall-clock
time (CSV exports, history rebuilt by pytp357s) into an unambiguous UTC datetime.

The timezone comes from the TZ variable of the .env file. If it is missing or invalid, the
machine's own timezone is used instead, with one warning in the logs: nothing in this module
ever raises because of the timezone setting.
"""

import logging
from datetime import datetime, timedelta, timezone
from functools import lru_cache
from typing import List, Optional
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from config import LOCAL_TZ_NAME

UTC = timezone.utc


@lru_cache(maxsize=1)
def get_local_timezone() -> Optional[ZoneInfo]:
    """
    Returns the timezone set by TZ in the .env file, or None when the machine's own timezone
    must be used (TZ missing, unknown, or tzdata not installed). Resolved once per process.
    """
    tz_name = (LOCAL_TZ_NAME or "").strip()
    if not tz_name:
        logging.warning("[TIME] TZ is not set in the .env file: using the machine's own timezone.")
        return None
    try:
        tz = ZoneInfo(tz_name)
    except (ZoneInfoNotFoundError, ValueError):
        logging.warning(f"[TIME] Unknown timezone '{tz_name}' in TZ (or the tzdata package is missing): using the machine's own timezone.")
        return None
    if _differs_from_machine(tz):
        logging.warning(
            f"[TIME] TZ='{tz_name}' differs from the machine's timezone. Dates produced by the machine's clock "
            f"(history rebuilt by pytp357s) will be read in '{tz_name}': make sure the two match."
        )
    return tz


def timezone_label(tz: Optional[ZoneInfo]) -> str:
    """Human-readable name of the zone returned by get_local_timezone()."""
    return tz.key if tz is not None else "the machine's local time"


def _differs_from_machine(tz: ZoneInfo) -> bool:
    """Compares the UTC offsets of a winter and a summer day, which also catches different daylight saving rules."""
    year = datetime.now().year
    for month in (1, 7):
        sample = datetime(year, month, 15, 12, 0)
        if sample.replace(tzinfo=tz).utcoffset() != sample.astimezone().utcoffset():
            return True
    return False


def _instants(naive: datetime, tz: Optional[ZoneInfo]) -> tuple[datetime, datetime]:
    """
    UTC instants of a local wall-clock time as (first pass, second pass).
    They differ only during the hour repeated at the end of daylight saving time (first < second).
    Outside of it they are equal, except in the hour that does not exist at the start of
    daylight saving time (first > second).
    tz=None means the machine's own timezone.
    """
    naive = naive.replace(tzinfo=None)
    if tz is None:
        return naive.replace(fold=0).astimezone(UTC), naive.replace(fold=1).astimezone(UTC)
    return naive.replace(tzinfo=tz, fold=0).astimezone(UTC), naive.replace(tzinfo=tz, fold=1).astimezone(UTC)


def local_series_to_utc(naive_times: List[datetime], tz: Optional[ZoneInfo]) -> List[datetime]:
    """
    Converts local wall-clock times that were WRITTEN DOWN as they appeared on a clock (a CSV export)
    to UTC, keeping the order of the list.

    At the end of daylight saving time one local hour happens twice (02:00-02:59 in Brussels) and the
    clock alone cannot tell the two passes apart. The order of the rows decides: the instants must keep
    moving forward (ascending list) or backward (descending list) from one row to the next.
    A single pass of the repeated hour cannot be recognised: it is read as the first pass.
    """
    ascending = len(naive_times) < 2 or naive_times[0] <= naive_times[-1]
    converted, previous, repeated = [], None, 0

    for naive in naive_times:
        first, second = _instants(naive, tz)
        if first >= second:
            chosen = first  # ordinary time (or non-existent local time, kept as before)
        else:
            repeated += 1
            if previous is None:
                logging.warning("[TIME] The data starts inside the repeated hour of the end of daylight saving time: the order of its first rows cannot be checked.")
            if ascending:
                chosen = first if previous is None or first > previous else second
            else:
                chosen = second if previous is None or second < previous else first
        converted.append(chosen)
        previous = chosen

    if repeated:
        logging.info(f"[TIME] {repeated} row(s) fall in the repeated hour of the end of daylight saving time; the order of the data told the two passes apart.")
    return converted


def backdate_from_newest(naive_times: List[datetime], tz: Optional[ZoneInfo], interval_minutes: int = 1,
                         now_utc: Optional[datetime] = None) -> List[datetime]:
    """
    Converts local times that were COMPUTED by stepping back `interval_minutes` from the newest one on the
    wall clock (what pytp357s does) to UTC. The list is oldest first.

    Stepping back on a wall clock ignores daylight saving changes, so converting each date on its own
    would shift everything older than the change by one hour. The newest date is the only reliable one
    (it is the real time of the fetch): it is converted alone, then the other dates are obtained by
    stepping back in UTC. If the newest date falls in the repeated hour, the pass closest to the
    current UTC time is kept.
    """
    if not naive_times:
        return []

    clean = [t.replace(second=0, microsecond=0, tzinfo=None) for t in naive_times]
    step = timedelta(minutes=interval_minutes)

    if any(b - a != step for a, b in zip(clean, clean[1:])):
        logging.warning("[TIME] The history dates are not evenly spaced as expected: converting them one by one instead.")
        return [_instants(t, tz)[0] for t in clean]

    now_utc = now_utc or datetime.now(UTC)
    newest_utc = min(_instants(clean[-1], tz), key=lambda candidate: abs(candidate - now_utc))
    last = len(clean) - 1
    return [newest_utc - step * (last - position) for position in range(len(clean))]