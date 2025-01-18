"""
CommerceCraft Utils - A multilingual translation and validation utility for CSV files.

This package provides:
- Translation utilities for CSV files using OpenAI's API
- CSV validation tools for file structure and content
"""

from ._version import __version__
from .commercetools import Auth
from .translation import (
    TranslationEngine,
    TranslationService,
    TranslationProcessor,
)
from .validation import (
    ValidationEngine,
    ValidationResult,
    Severity,
    FileValidator,
    DataFrameValidator,
    FieldValidator,
)


__all__ = [
    # Commercetools components
    'Auth',
    
    # Translation components
    'TranslationEngine',
    'TranslationService',
    'TranslationProcessor',
    
    # Validation components
    'ValidationEngine',
    'ValidationResult',
    'Severity',
    'FileValidator',
    'DataFrameValidator',
    'FieldValidator',
    
    # Version
    '__version__',
]