import os
from pathlib import Path
from dotenv import load_dotenv

# Locate and load the environment variables immediately
BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env", override=True)

# Application Execution Settings
EXECUTION_MODE = os.getenv("APP_EXECUTION_MODE", "OFFLINE_SIMULATION").upper()

# Logging Infrastructure Configurations
LOG_LEVELS = ("INFO", "SUCCESS", "WARNING", "ERROR")
CURRENT_LOG_LEVEL = os.getenv("APP_LOG_LEVEL", "INFO").upper()
if CURRENT_LOG_LEVEL not in LOG_LEVELS:
    CURRENT_LOG_LEVEL = "INFO"

CURRENT_LOG_INDEX = LOG_LEVELS.index(CURRENT_LOG_LEVEL)

# Database Funnel & Batching Constraints
DB_INSERT_INTERVAL = float(os.getenv("DB_INSERT_INTERVAL_SECONDS", "10.0"))
DB_MAX_BATCH_SIZE = int(os.getenv("DB_MAX_BATCH_SIZE", "50"))
