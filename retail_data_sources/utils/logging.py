"""logging.py - Utility functions for logging."""

import logging
from pathlib import Path


def setup_logging() -> logging.Logger:
    """Configure logging system."""
    log_dir = Path("./logs")
    log_dir.mkdir(exist_ok=True)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.FileHandler(f"{log_dir}/census_data_processing.log"), logging.StreamHandler()],
    )
    return logging.getLogger(__name__)
