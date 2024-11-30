"""utils.py - Utility functions for the census data processing pipeline."""

import logging
from pathlib import Path


def setup_logging() -> logging.Logger:
    """Configure logging system."""
    log_dir = Path("../logs")
    log_dir.mkdir(exist_ok=True)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.FileHandler(f"{log_dir}/processing.log"), logging.StreamHandler()],
    )
    return logging.getLogger(__name__)
