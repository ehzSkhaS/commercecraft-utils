"""
DataFrame structure validator.
"""

import pandas as pd
from typing import List, Set, Dict, Any, Optional
from .base_validator import BaseValidator
from ..validation_result import ValidationResult, Severity


class DataFrameValidator(BaseValidator):
    """
    Validates pandas DataFrame structure and content.
    
    Checks:
    - Required columns presence
    - Column name format
    - Column data types
    - Missing values
    - Duplicate rows
    - Language code format in columns
    """
    
    def __init__(
        self,
        required_columns: Optional[Set[str]] = None,
        column_types: Optional[Dict[str, Any]] = None,
        language_separator: str = '.',
        allow_duplicates: bool = False,
        allow_missing_values: bool = True
    ):
        super().__init__()
        self.required_columns = required_columns or set()
        self.column_types = column_types or {}
        self.language_separator = language_separator
        self.allow_duplicates = allow_duplicates
        self.allow_missing_values = allow_missing_values
    
    def validate(self, df: pd.DataFrame) -> List[ValidationResult]:
        """
        Validate a pandas DataFrame.
        
        Args:
            df: pandas DataFrame to validate
            
        Returns:
            List of ValidationResult objects
        """
        self.clear_results()
        
        # Check required columns
        missing_columns = self.required_columns - set(df.columns)
        if missing_columns:
            self.add_result(ValidationResult(
                severity=Severity.ERROR,
                message=f'Missing required columns: {", ".join(missing_columns)}',
                location={'dataframe': 'columns'},
                suggested_fix='Add the missing columns to the CSV file'
            ))
        
        # Check column name format
        for col in df.columns:
            if not self._is_valid_column_name(col):
                self.add_result(ValidationResult(
                    severity=Severity.WARNING,
                    message=f'Invalid column name format: {col}',
                    location={'column': col},
                    suggested_fix='Use alphanumeric characters and underscores'
                ))
        
        # Check data types
        for col, expected_type in self.column_types.items():
            if col in df.columns:
                try:
                    df[col].astype(expected_type)
                except Exception as e:
                    self.add_result(ValidationResult(
                        severity=Severity.ERROR,
                        message=f'Invalid data type in column {col}. Expected {expected_type}',
                        location={'column': col},
                        context={'error': str(e)}
                    ))
        
        # Check for missing values
        if not self.allow_missing_values:
            missing_vals = df.isnull().sum()
            cols_with_missing = missing_vals[missing_vals > 0]
            
            for col, count in cols_with_missing.items():
                self.add_result(ValidationResult(
                    severity=Severity.ERROR,
                    message=f'Found {count} missing values in column {col}',
                    location={'column': col},
                    suggested_fix='Fill in missing values'
                ))
        
        # Check for duplicates
        if not self.allow_duplicates:
            duplicate_count = df.duplicated().sum()
            
            if duplicate_count > 0:
                self.add_result(ValidationResult(
                    severity=Severity.WARNING,
                    message=f'Found {duplicate_count} duplicate rows',
                    location={'dataframe': 'rows'},
                    suggested_fix='Remove duplicate rows'
                ))
        
        # Check language code format in columns
        for col in df.columns:
            if self.language_separator in col:
                if not self._is_valid_language_column(col):
                    self.add_result(ValidationResult(
                        severity=Severity.WARNING,
                        message=f'Invalid language column format: {col}',
                        location={'column': col},
                        suggested_fix=f'Use format: field{self.language_separator}lang-REGION'
                    ))
        
        return self.results
    
    def _is_valid_column_name(self, name: str) -> bool:
        """Check if column name follows valid format."""
        import re
        
        return bool(re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', name.split(self.language_separator)[0]))
    
    def _is_valid_language_column(self, name: str) -> bool:
        """Check if language column follows valid format."""
        import re
        
        parts = name.split(self.language_separator)
        
        if len(parts) != 2:
            return False
        
        field, lang = parts
        
        # Check if language code follows format: lang-REGION
        return bool(re.match(r'^[a-z]{2}-[A-Z]{2}$', lang))
