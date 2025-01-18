"""
Base validator class that all validators must inherit from.
"""

from abc import ABC, abstractmethod
from typing import List, Any
from ..validation_result import ValidationResult


class BaseValidator(ABC):
    """
    Abstract base class for all validators.
    
    All validator implementations should inherit from this class
    and implement the validate method.
    """
    
    def __init__(self):
        self.__results: List[ValidationResult] = []
    
    @abstractmethod
    def validate(self, data: Any) -> List[ValidationResult]:
        """
        Validate the input data and return a list of validation results.
        
        Args:
            data: The data to validate (type depends on specific validator)
            
        Returns:
            List of ValidationResult objects
        """
        pass
    
    def add_result(self, result: ValidationResult) -> None:
        """Add a validation result to the results list."""
        self.__results.append(result)
    
    @property
    def results(self) -> List[ValidationResult]:
        """Get all validation results."""
        return self.__results
    
    def clear_results(self) -> None:
        """Clear all validation results."""
        self.__results = []
