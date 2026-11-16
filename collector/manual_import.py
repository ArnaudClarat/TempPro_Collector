"""
ThermoPro Historical CSV Importer
Automates memory-bounded historical backfills directly into TimescaleDB partitions.
"""

import asyncio, csv, logging, sys, shutil
from datetime import datetime
from pathlib import Path
from typing import Optional
from zoneinfo import ZoneInfo

# Append project root to allow standalone utility script execution
PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config import EXECUTION_MODE, DB_CHUNK_SIZE, DB_INSERT_INTERVAL_SECONDS
from db import Database
from mapping import SensorRegistry
from models import Measure, Sensor
from timeutils import get_local_timezone, timezone_label, local_series_to_utc

CSV_ENCODING, CSV_COLUMNS = "utf-8-sig", 4
IMPORT_DIR = PROJECT_ROOT / "import"
ARCHIVE_DIR = PROJECT_ROOT / "archive"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("csv-importer")

async def resolve_sensor_id_from_file(registry: SensorRegistry, file_path: Path) -> Sensor:
    """Reads the location name from the CSV file name and looks up its active sensor in the registry cache."""
    parts = file_path.stem.split("_")
    if len(parts) < 2:
        raise ValueError(f"Name format of CSV file is incorrect : {file_path.name}")
    location_name = parts[-2]

    matches = [
        s for s in await registry.get_sensors()
        if s.location_name and s.location_name.lower() == location_name.lower()
    ]
    if not matches:
        raise ValueError(f"No active hardware assignment mapping found for location '{location_name}' in DB.")
    if len(matches) > 1:
        logger.warning(
            "%d active sensors share the location '%s' (%s). Using the first one: %s.",
            len(matches), location_name, ", ".join(s.ble_id for s in matches), matches[0].ble_id
        )
    return matches[0]

def parse_csv(file_path: Path, ble_id: str, sensor_id: int, local_tz: Optional[ZoneInfo]):
    """
    Yields parsed Measure objects in file order.
    Timestamps are read as local time in local_tz (None = the machine's own timezone). The rows are
    collected first so that the order of the whole file can settle the repeated hour of the end of
    daylight saving time (see timeutils.local_series_to_utc).
    """
    if not file_path.is_file():
        raise FileNotFoundError(f"Missing CSV target: {file_path}")

    rows_valid, rows_skipped = 0, 0
    parsed = []  # (naive local datetime, temperature, humidity), in file order
    with file_path.open(mode="r", encoding=CSV_ENCODING, newline="") as f:
        reader = csv.reader(f)
        try:
            next(f)       # Skip ThermoPro system signature export header
            next(reader)  # Skip metrics columns schema line
        except StopIteration as exc:
            raise ValueError("Target CSV data catalog is empty or corrupted.") from exc

        for line_idx, row in enumerate(reader, start=3):
            if not row: continue
            if len(row) != CSV_COLUMNS:
                logger.warning("Line %d corrupted (Expected 4 columns, got %d). Skipped.", line_idx, len(row))
                rows_skipped += 1
                continue

            raw_date = row[0].replace("\ufeff", "").strip()
            raw_time = row[1].replace("\ufeff", "").strip()
            raw_temperature = row[2].strip()
            raw_humidity = row[3].strip()

            # Checks for empty lines
            if raw_temperature == "-" or raw_humidity == "-":
                rows_skipped += 1
                continue

            try:
                naive_dt = datetime.strptime(f"{raw_date} {raw_time}", "%d/%m/%Y %H:%M")
                parsed.append((naive_dt, round(float(raw_temperature), 2), round(float(raw_humidity), 2)))
                rows_valid += 1
            except (ValueError, TypeError) as exc:
                logger.warning("Parsing violation at line %d: %s", line_idx, exc)
                rows_skipped += 1

    utc_times = local_series_to_utc([naive_dt for naive_dt, _, _ in parsed], local_tz)

    for (_, temperature, humidity), utc_dt in zip(parsed, utc_times):
        yield Measure(
            time=utc_dt,
            sensor_id=sensor_id,
            ble_id=ble_id,
            temperature=temperature,
            humidity=humidity,
            battery_raw=None,  # CSV exports carry no battery level; the column accepts NULL
        )

    logger.info("[IMPORTER] Evaluation complete. Valid: %d | Skipped: %d", rows_valid, rows_skipped)

async def insert_in_chunks(db: Database, dataset: list[Measure]) -> None:
    """
    Writes the dataset in chunks of DB_CHUNK_SIZE rows, pausing between chunks like the collector does.
    Each chunk is committed on its own; re-running an import is safe (duplicates are ignored by the database).
    """
    for start in range(0, len(dataset), DB_CHUNK_SIZE):
        await db._insert_measures(dataset[start:start + DB_CHUNK_SIZE])
        if start + DB_CHUNK_SIZE < len(dataset):
            await asyncio.sleep(DB_INSERT_INTERVAL_SECONDS)

def prompt_file_action(file_path: Path) -> None:
    """Interactively prompts the user to select the post-ingestion cleanup strategy."""
    while True:
        choice = input(
            f"\n[ACTION REQUIRED] Choose a post-ingestion strategy for '{file_path.name}':\n"
            f" [D]elete file\n"
            f" [A]rchive file\n"
            f" [L]eft it there\n"
            f"Action (D/A/L): "
        ).strip().upper()

        if choice in ("D", "DELETE"):
            file_path.unlink()
            logger.info("Target file permanently deleted from storage.")
            break
        elif choice in ("A", "ARCHIVE"):
            ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
            shutil.move(str(file_path), str(ARCHIVE_DIR / file_path.name))
            logger.info("Target file successfully relocated to archive directory.")
            break
        elif choice in ("L", "LEFT"):
            logger.info("Target file left unmodified inside the import directory.")
            break

async def main() -> int:
    # Any mode other than FULL_PRODUCTION skips or simulates the writes: the import would look
    # successful while nothing is stored, and the CSV could then be deleted by mistake.
    if EXECUTION_MODE != "FULL_PRODUCTION":
        logger.error("Import aborted: APP_EXECUTION_MODE is '%s'. Only FULL_PRODUCTION really writes to the database.", EXECUTION_MODE)
        return 1

    local_tz = get_local_timezone()
    logger.info("CSV timestamps will be read as local time in %s.", timezone_label(local_tz))

    csv_files = list(IMPORT_DIR.glob("*.csv")) if IMPORT_DIR.is_dir() else []
    if not csv_files:
        logger.error("Data pipeline aborted: No target csv export logs found inside '%s'.", IMPORT_DIR)
        return 1

    db = Database()
    registry = SensorRegistry(db)
    db.registry = registry

    try:
        await db.get_conn()
        await registry.load_mapping()

        for target_csv in csv_files:
            logger.info("Target file acquired: '%s'", target_csv.name)
            try:
                meta = await resolve_sensor_id_from_file(registry, target_csv)
                logger.info("DB Entity resolved -> Hardware Asset ID: %d (Location: %s)", meta.db_id, meta.location_name)

                dataset = list(parse_csv(target_csv, ble_id=meta.ble_id, sensor_id=meta.db_id, local_tz=local_tz))

                if not dataset:
                    logger.warning("No valid data to import.")
                    continue

                logger.info("Insertion de %d lignes dans TimescaleDB (morceaux de %d)...", len(dataset), DB_CHUNK_SIZE)
                await insert_in_chunks(db, dataset)
                logger.info("Pipeline operations processed successfully.")

                prompt_file_action(target_csv)

            except Exception as file_exc:
                logger.error("Processing sequence failed for file %s: %s", target_csv.name, file_exc)
                continue

        logger.info("Pipeline operations processed successfully. All imports completed.")
        return 0

    except Exception as exc:
        logger.error("Historical migration pipeline collapsed: %s", exc)
        return 1
    finally:
        await db.close_db()


if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    sys.exit(asyncio.run(main()))