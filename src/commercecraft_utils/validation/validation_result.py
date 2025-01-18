"""
Validation result classes and utilities.
"""

from enum import Enum
from dataclasses import dataclass
from typing import Optional, Any, Dict
from ..utils.logger import log_validation_result


class Severity(Enum):
    """Severity levels for validation issues."""
    ERROR = 'error'
    WARNING = 'warning'
    INFO = 'info'


@dataclass
class ValidationResult:
    """
    Represents a single validation result.
    
    Attributes:
        severity: The severity level of the validation result
        message: Description of the validation result
        location: Where the issue was found. e.g.,{'file': 'data.csv', 'row': 5, 'column': 'name'}
        context: Additional context about the validation
        suggested_fix: Optional suggestion for fixing the issue
    """
    severity: Severity
    message: str
    location: Dict[str, Any]
    context: Optional[Dict[str, Any]] = None
    suggested_fix: Optional[str] = None

    def __post_init__(self):
        """Log the validation result after initialization."""
        log_validation_result(
            severity=self.severity.value,
            message=self.message,
            location=self.location,
            context=self.context,
            suggested_fix=self.suggested_fix
        )

    def __str__(self) -> str:
        """String representation of the validation result."""
        location_str = ', '.join(f'{k}: {v}' for k, v in self.location.items())
        
        return f'[{self.severity.value.upper()}] {self.message} (at {location_str})'
