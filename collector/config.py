import os, logging
from pathlib import Path
from dotenv import load_dotenv

# Locate and load the environment variables immediately
load_dotenv(Path(__file__).resolve().parents[1] / ".env", override=True)

# Application Execution Settings
EXECUTION_MODE = os.getenv("APP_EXECUTION_MODE", "OFFLINE_SIMULATION").upper()

# Logging Infrastructure Configurations
CURRENT_LOG_LEVEL = logging.getLevelName(os.getenv("APP_LOG_LEVEL", "INFO").upper())

# Database settings
DB_CHUNK_SIZE = int(os.getenv("DB_CHUNK_SIZE", 1000))
DB_INSERT_INTERVAL_SECONDS = int(os.getenv("DB_INSERT_INTERVAL_SECONDS",1))