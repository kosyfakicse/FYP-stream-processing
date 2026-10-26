"""Logging configuration and utilities using loguru."""

import sys
from pathlib import Path
from loguru import logger


def setup_logging(
    log_level: str = "INFO",
    log_file: str | None = None,
    rotation: str = "10 MB",
    retention: str = "10 days",
) -> None:
    """
    Configure logging for the application.
    
    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file: Path to log file. If None, logs only to console.
        rotation: When to rotate log file
        retention: How long to keep old log files
    """
    # Remove default handler
    logger.remove()
    
    # Add console handler
    logger.add(
        sys.stderr,
        level=log_level,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
        colorize=True,
    )
    
    # Add file handler if log_file is specified
    if log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        
        logger.add(
            str(log_path),
            level=log_level,
            format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
            rotation=rotation,
            retention=retention,
            compression="zip",
        )
    
    logger.info(f"Logging configured: level={log_level}, file={log_file}")


def get_logger(name: str | None = None):
    """
    Get a logger instance.
    
    Args:
        name: Optional name for the logger (usually __name__)
    
    Returns:
        logger: Configured loguru logger
    """
    if name:
        return logger.bind(name=name)
    return logger


# Convenience function for quick logging
def configure_default_logging() -> None:
    """Configure default logging for development."""
    setup_logging(log_level="INFO", log_file="logs/app.log")
