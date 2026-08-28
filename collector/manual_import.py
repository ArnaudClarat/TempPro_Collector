"""
ThermoPro Historical CSV Importer
Automates memory-bounded historical backfills directly into TimescaleDB partitions.
"""

import asyncio, csv, logging, os, sys
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

# Append project root to allow standalone utility script execution
PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from db import DatabaseBatcher
from models import SensorMeasure

CSV_ENCODING, CSV_COLUMNS, BATCH_SIZE = "utf-8-sig", 4, 5000
LOCAL_TIMEZONE = ZoneInfo("Europe/Brussels")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("thermopro-importer")
IMPORT_DIR = PROJECT_ROOT / "import"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("csv-importer")

async def resolve_sensor_id_from_file(db: DatabaseBatcher, file_path: Path) -> tuple[int, str]:
    """Inspects the CSV metadata header to discover the location name and lookup its physical sensor ID."""
    with file_path.open(mode="r", encoding=CSV_ENCODING) as f:
        _ = next(f) # e.g., "Horodatage pour chaque échantillon de fréquence chaque 1 min"

    # Sémantique de détection : Extrait "Test" à partir de "TempProSensor_Export_Test_27082026.csv"
    parts = file_path.stem.split("_")
    if len(parts) < 2:
        raise ValueError(f"Name format of CSV file is incorrect : {file_path.name}")
    location_name = parts[-2]

    async with db.pool.connection() as conn:
        async with conn.cursor() as cur:
            # Query the temporal layer to find the physical hardware asset ID bound to this location name
            await cur.execute(
                """
                SELECT s.id, s.ble_id
                FROM locations l
                JOIN sensor_assignments sa ON l.id = sa.location_id
                JOIN sensors s ON sa.sensor_id = s.id
                WHERE LOWER(l.name) = LOWER(%s) AND sa.removed_at IS NULL
                LIMIT 1;
                """,
                (location_name,)
            )
            result = await cur.fetchone()
            if not result:
                raise ValueError(f"No active hardware assignment mapping found for location '{location_name}' in DB.")
            return result[0], result[1]


def parse_csv(file_path: Path, ble_id: str, sensor_id: int):
    """Yields parsed SensorMeasure objects sequentially to maintain a strict memory ceiling."""
    if not file_path.is_file():
        raise FileNotFoundError(f"Missing CSV target: {file_path}")

    rows_valid, rows_skipped = 0, 0
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

            try:
                raw_date = row[0].replace("\ufeff", "").strip()
                raw_time = row[1].replace("\ufeff", "").strip()
                raw_temperature = row[2].strip()
                raw_humidity = row[3].strip()

                # Checks for empty lines
                if raw_temperature == "-" or raw_humidity == "-":
                    rows_skipped += 1
                    continue

                # Enforce native timezone parsing to handle winter/summer offsets (CET/CEST) dynamically
                local_dt = datetime.strptime(f"{raw_date} {raw_time}", "%d/%m/%Y %H:%M").replace(tzinfo=LOCAL_TIMEZONE)
                rows_valid += 1

                yield SensorMeasure(
                    time=local_dt.astimezone(timezone.utc),
                    sensor_id=sensor_id,
                    ble_id=ble_id,
                    temperature=round(float(raw_temperature), 2),
                    humidity_raw=round(float(raw_humidity), 2),
                )
            except (ValueError, TypeError) as exc:
                logger.warning("Parsing violation at line %d: %s", line_idx, exc)
                rows_skipped += 1

    logger.info("[STREAM SUMMARY] Evaluation complete. Valid: %d | Skipped: %d", rows_valid, rows_skipped)


async def main() -> int:
    csv_files = list(IMPORT_DIR.glob("*.csv")) if IMPORT_DIR.is_dir() else []
    if not csv_files:
        logger.error("Data pipeline aborted: No target csv export logs found inside '%s'.", IMPORT_DIR)
        return 1

    target_csv = csv_files[0]
    logger.info("Target operational context acquired: '%s'", target_csv.name)

    db = DatabaseBatcher()
    await db.init_db()

    try:
        sensor_id, ble_id = await resolve_sensor_id_from_file(db, target_csv)
        logger.info("DB Entity resolved -> Hardware Asset ID: %d (Signature: %s)", sensor_id, ble_id)

        dataset = list(parse_csv(target_csv, ble_id=ble_id, sensor_id=sensor_id))

        if not dataset:
            logger.warning("Aucune donnée valide à importer.")
            return 0

        logger.info("Insertion unique de %d lignes dans TimescaleDB...", len(dataset))
        await db.insert_measures(dataset)

        logger.info("Pipeline operations processed successfully.")

        confirm = input(f"\n[CONFIRMATION] Proceed to permanently purge historical file '{target_csv.name}'? (y/N): ")
        if confirm.strip().lower() in ("y", "yes"):
            target_csv.unlink()
            logger.info("Target file resource unlinked. File system storage clean.")
        else:
            logger.warning("File deletion skipped by user request.")
        return 0

    except Exception as exc:
        logger.error("Historical migration pipeline collapsed: %s", exc)
        return 1
    finally:
        if hasattr(db, "pool") and db.pool is not None:
            await db.pool.close()


if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    sys.exit(asyncio.run(main()))
