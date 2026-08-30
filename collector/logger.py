import os
from config import LOG_LEVELS, CURRENT_LOG_LEVEL

def log_msg(level: str, message: str) -> None:
    """
    Custom print-based logging system to filter outputs based on APP_LOG_LEVEL.
    """
    level_upper = level.upper()
    
    # Check if the level is valid and meets the minimum severity threshold
    if level_upper in LOG_LEVELS and LOG_LEVELS.index(level_upper) >= CURRENT_LOG_LEVEL:
        print(f"[{level_upper}] {message}", flush=True)
