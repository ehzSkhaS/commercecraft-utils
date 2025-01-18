"""
Main validation engine that orchestrates the validation process.
"""

import pandas as pd
from pathlib import Path
from typing import List, Optional
from .validation_result import ValidationResult, Severity
from .validators import FileValidator, DataFrameValidator, FieldValidator
from ..utils import configure_logger


class ValidationEngine:
    """
    Orchestrates the validation process across multiple validators.
    
    This class coordinates the validation pipeline, running only the
    validators that were explicitly provided.
    """
    
    def __init__(
        self,
        file_validator: Optional[FileValidator] = None,
        dataframe_validator: Optional[DataFrameValidator] = None,
        field_validator: Optional[FieldValidator] = None
    ):
        """
        Initialize the validation engine.
        
        Args:
            file_validator: Optional FileValidator instance
            dataframe_validator: Optional DataFrameValidator instance
            field_validator: Optional FieldValidator instance
            
        Note:
            Only the validators that are explicitly provided will be used
            in the validation process.
        """
        self.__logger = configure_logger(__name__)
        self.file_validator = file_validator
        self.dataframe_validator = dataframe_validator
        self.field_validator = field_validator
        self._results: List[ValidationResult] = []
    
    def validate_file(
        self,
        file_path: str | Path,
        encoding: Optional[str] = None,
        **pandas_kwargs
    ) -> List[ValidationResult]:
        """
        Validate a CSV file using the provided validators.
        
        Args:
            file_path: Path to the CSV file
            encoding: Optional file encoding
            **pandas_kwargs: Additional arguments passed to pd.read_csv
            
        Returns:
            List of ValidationResult objects
        """
        self._results = []
        
        # Level 1: File validation
        if self.file_validator:
            file_results = self.file_validator.validate(file_path)
            self._results.extend(file_results)
            
            # Stop if there are critical file errors
            if any(r.severity == Severity.ERROR for r in file_results):
                return self._results
        
        # Only proceed with DataFrame validation if we have DataFrame or Field validators
        if self.dataframe_validator or self.field_validator:
            # Read the DataFrame
            try:
                self.df = pd.read_csv(file_path, encoding=encoding, **pandas_kwargs)
            except Exception as e:
                self._results.append(ValidationResult(
                    severity=Severity.ERROR,
                    message=f'Failed to read CSV file: {str(e)}',
                    location={'file': str(file_path)}
                ))
                return self._results
            
            # Level 2: DataFrame validation
            if self.dataframe_validator:
                df_results = self.dataframe_validator.validate(self.df)
                self._results.extend(df_results)
            
            # Level 3: Field validation
            if self.field_validator:
                field_results = self.field_validator.validate(self.df)
                self._results.extend(field_results)
        
        return self._results
    
    def validate_dataframe(
        self,
        df: pd.DataFrame = None,
    ) -> List[ValidationResult]:
        """
        Validate an existing DataFrame using the provided validators.
        
        Args:
            df: pandas DataFrame to validate
            
        Returns:
            List of ValidationResult objects
        """
        self._results = []
        
        # Level 2: DataFrame validation
        if self.dataframe_validator:
            df_results = self.dataframe_validator.validate(self.df if df is None else df)
            self._results.extend(df_results)
        
        # Level 3: Field validation
        if self.field_validator:
            field_results = self.field_validator.validate(self.df if df is None else df)
            self._results.extend(field_results)
        
        return self._results
    
    @property
    def results(self) -> List[ValidationResult]:
        """Get all validation results."""
        return self._results
    
    def get_errors(self) -> List[ValidationResult]:
        """Get validation results with ERROR severity."""
        return [r for r in self._results if r.severity == Severity.ERROR]
    
    def get_warnings(self) -> List[ValidationResult]:
        """Get validation results with WARNING severity."""
        return [r for r in self._results if r.severity == Severity.WARNING]
    
    def get_info(self) -> List[ValidationResult]:
        """Get validation results with INFO severity."""
        return [r for r in self._results if r.severity == Severity.INFO]
    
    def has_errors(self) -> bool:
        """Check if there are any ERROR severity results."""
        return any(r.severity == Severity.ERROR for r in self._results)
