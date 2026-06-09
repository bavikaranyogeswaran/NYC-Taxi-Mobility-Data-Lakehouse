"""
Shared structlog configuration for all pipeline stages.
Call get_logger() once per module at module level.

LOG_FORMAT env var:
  "json"    (default) — machine-readable JSON for Docker / Loki ingestion
  "console"           — coloured human-readable output for local dev
"""
import logging
import os
import sys
from pathlib import Path

import structlog

_configured = False


def _configure(log_file: Path | None) -> None:
    global _configured
    if _configured:
        return

    handlers: list[logging.Handler] = [logging.StreamHandler(sys.stdout)]
    if log_file:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        handlers.append(logging.FileHandler(str(log_file), encoding="utf-8"))

    logging.basicConfig(format="%(message)s", level=logging.INFO, handlers=handlers, force=True)

    fmt = os.environ.get("LOG_FORMAT", "json").lower()
    renderer = (
        structlog.dev.ConsoleRenderer(colors=True)
        if fmt == "console"
        else structlog.processors.JSONRenderer()
    )

    structlog.configure(
        processors=[
            structlog.stdlib.add_log_level,
            structlog.stdlib.add_logger_name,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            renderer,
        ],
        wrapper_class=structlog.stdlib.BoundLogger,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )
    _configured = True


def get_logger(name: str, log_file: Path | None = None) -> structlog.stdlib.BoundLogger:
    """Return a structlog logger configured for the current LOG_FORMAT."""
    _configure(log_file)
    return structlog.get_logger(name)
