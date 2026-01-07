"""Structured logging configuration for postmark services.

Sets up structlog with JSON output for production, pretty console output for dev.
"""

import logging
import sys

import structlog


def configure_logging(service_name: str, level: str = "INFO") -> None:
    """Configure structured logging for a service.

    Args:
        service_name: Name of the service (e.g., 'gmail-sync', 'attachment-worker')
        level: Log level ('DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL')
    """
    # Configure standard logging to output through structlog
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=getattr(logging, level.upper()),
    )

    # Determine if we're in a TTY (development) or not (production)
    is_tty = sys.stdout.isatty()

    # Shared processors for all log entries
    shared_processors = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        structlog.processors.StackInfoRenderer(),
    ]

    # Add service name to all log entries
    structlog.contextvars.bind_contextvars(service=service_name)

    if is_tty:
        # Pretty console output for development
        processors = shared_processors + [
            structlog.dev.ConsoleRenderer(),
        ]
    else:
        # JSON output for production (parsed by log aggregators)
        processors = shared_processors + [
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(),
        ]

    structlog.configure(
        processors=processors,
        wrapper_class=structlog.make_filtering_bound_logger(
            getattr(logging, level.upper())
        ),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )


def get_logger(name: str | None = None) -> structlog.BoundLogger:
    """Get a logger instance.

    Args:
        name: Optional logger name (usually __name__ from calling module)

    Returns:
        Configured structlog logger
    """
    return structlog.get_logger(name)
