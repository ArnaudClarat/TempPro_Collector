import os
from pathlib import Path
from dotenv import load_dotenv

# Locate and load the environment variables immediately
load_dotenv(Path(__file__).resolve().parents[1] / ".env", override=True)

# Application Execution Settings
EXECUTION_MODE = os.getenv("APP_EXECUTION_MODE", "OFFLINE_SIMULATION").upper()

# Logging Infrastructure Configurations
LOG_LEVELS = ("INFO", "DEBG", "WARN", "ERRO")
CURRENT_LOG_LEVEL = LOG_LEVELS.index(
    os.getenv("APP_LOG_LEVEL", "INFO").upper()[:4] if os.getenv("APP_LOG_LEVEL", "INFO").upper()[:4] in LOG_LEVELS else "INFO"
)

# Database Funnel & Batching Constraints
DB_INSERT_INTERVAL = float(os.getenv("DB_INSERT_INTERVAL_SECONDS", "10.0"))
DB_MAX_BATCH_SIZE = int(os.getenv("DB_MAX_BATCH_SIZE", "50"))
