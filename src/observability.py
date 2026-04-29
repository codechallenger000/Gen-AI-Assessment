from __future__ import annotations

import json
import logging
import os
from typing import Any

LOGGER_NAME = "assignment.analytics_pipeline"


def get_logger() -> logging.Logger:
    logger = logging.getLogger(LOGGER_NAME)
    if logger.handlers:
        return logger

    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(message)s"))
    logger.addHandler(handler)
    logger.setLevel(os.getenv("PIPELINE_LOG_LEVEL", "INFO").upper())
    logger.propagate = False
    return logger


def log_event(event: str, **fields: Any) -> None:
    payload = {"event": event, **fields}
    get_logger().info(json.dumps(payload, sort_keys=True, default=str))
