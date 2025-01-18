"""Logging utilities for commercecraft-utils."""

import logging
from typing import Any, Dict, Optional


def configure_logger(name: str) -> logging.Logger:
    """
    Configure a logger with consistent formatting across all environments.
    
    Args:
        name (str): Name for the logger, typically __name__
        
    Returns:
        logging.Logger: Configured logger instance
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    
    # Thanks to stupid Colab!!!
    # If running in Colab (root logger has handlers), don't add our own
    if logging.getLogger().handlers:
        logger.propagate = True
        
        return logger
        
    # Only add handler if not in Colab and no handlers exist
    if not logger.handlers:
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler = logging.StreamHandler()
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    
    return logger


class LoggerUtility:
    """Utility class for consistent logging across modules."""
    
    def __init__(self, name: str):
        """Initialize logger with module name."""
        self.logger = configure_logger(name)
    
    def log_validation_result(
        self,
        severity: str,
        message: str,
        location: Dict[str, Any],
        context: Optional[Dict[str, Any]] = None,
        suggested_fix: Optional[str] = None
    ) -> None:
        """
        Log validation results in a consistent format.
        
        Args:
            severity: Severity level (error, warning, info)
            message: The main message to log
            location: Location details (file, line, etc.)
            context: Additional context information
            suggested_fix: Suggested solution if applicable
        """
        # Log main message with appropriate level
        if severity.lower() == 'error':
            self.logger.error(message)
        elif severity.lower() == 'warning':
            self.logger.warning(message)
        else:
            self.logger.info(message)
            
        # Log location and context as info
        location_str = ', '.join(f'{k}: {v}' for k, v in location.items())
        self.logger.info(f'Location: {location_str}')
        
        if context:
            context_str = ', '.join(f'{k}: {v}' for k, v in context.items())
            self.logger.info(f'Context: {context_str}')
            
        # Log suggested fix if present
        if suggested_fix:
            self.logger.info(f'Fix: {suggested_fix}')


# Create default logger instance for backward compatibility
default_logger = LoggerUtility('commercecraft-utils')
log_validation_result = default_logger.log_validation_result
