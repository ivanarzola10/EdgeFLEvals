import logging
import functools
from typing import Any, Optional
import traceback


class EdgeFLLogger:
    """
    Enhanced logger wrapper that maintains the same interface as Python's logging.Logger
    but adds structured logging with context tracking
    """
    
    def __init__(self, name: str, logger: logging.Logger = None):
        self.name = name
        self._logger = logger or logging.getLogger(name)
        self._context = {}
    
    def _format_message(self, msg: str, extra_context: dict = None) -> str:
        """Format message with context information."""
        context = {**self._context, **(extra_context or {})}
        if context:
            context_str = " | ".join([f"{k}={v}" for k, v in context.items()])
            return f"{msg} | {context_str}"
        return msg
    
    def set_context(self, **kwargs):
        """Set persistent context that will be included in all log messages."""
        self._context.update(kwargs)
    
    def clear_context(self):
        """Clear all persistent context."""
        self._context = {}
    
    def debug(self, msg: str, extra: dict = None):
        """Log debug message with optional extra context."""
        self._logger.debug(self._format_message(msg, extra))
    
    def info(self, msg: str, extra: dict = None):
        """Log info message with optional extra context."""
        self._logger.info(self._format_message(msg, extra))
    
    def warning(self, msg: str, extra: dict = None):
        """Log warning message with optional extra context."""
        self._logger.warning(self._format_message(msg, extra))
    
    def error(self, msg: str, extra: dict = None, exc_info: bool = False):
        """Log error message with optional extra context and exception info."""
        self._logger.error(self._format_message(msg, extra), exc_info=exc_info)
    
    def critical(self, msg: str, extra: dict = None, exc_info: bool = False):
        """Log critical message with optional extra context and exception info."""
        self._logger.critical(self._format_message(msg, extra), exc_info=exc_info)
    
    def exception(self, msg: str, extra: dict = None):
        """Log exception with traceback."""
        self._logger.exception(self._format_message(msg, extra))
    
    def log_exception(self, exception: Exception, context: dict = None):
        """
        Log an exception with full context including traceback.
        
        Args:
            exception: The exception to log
            context: Additional context dictionary
        """
        error_context = {
            "exception_type": type(exception).__name__,
            "exception_msg": str(exception),
        }
        
        if hasattr(exception, 'context'):
            error_context.update(exception.context)
        
        if context:
            error_context.update(context)
        
        if hasattr(exception, 'original_error') and exception.original_error:
            error_context["original_error"] = str(exception.original_error)
        
        self.error(
            f"Exception occurred: {type(exception).__name__}",
            extra=error_context,
            exc_info=True
        )
    
    def setLevel(self, level):
        """Set logging level (maintains compatibility with standard logger)."""
        self._logger.setLevel(level)
    
    @property
    def level(self):
        """Get current logging level."""
        return self._logger.level


def get_logger(name: str) -> EdgeFLLogger:
    """
    Get an EdgeFL logger instance.
    
    This is a drop-in replacement for logging.getLogger() that returns
    an enhanced logger with the same interface.
    
    Args:
        name: Logger name (typically __name__)
    
    Returns:
        EdgeFLLogger instance
    """
    return EdgeFLLogger(name)
