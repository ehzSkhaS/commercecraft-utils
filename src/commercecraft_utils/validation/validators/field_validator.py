"""
Field content validator.
"""

import re
import pandas as pd
from typing import List, Dict, Any, Callable, Optional
from .base_validator import BaseValidator
from ..validation_result import ValidationResult, Severity


class FieldValidator(BaseValidator):
    """
    Validates individual field contents.
    
    Checks:
    - Data type validation
    - Value range checks
    - Format validation
    - Language-specific validations
    - Custom business rules
    """
    
    def __init__(self):
        super().__init__()
        self.validators: Dict[str, List[Dict[str, Any]]] = {}
    
    def add_rule(
        self,
        column: str,
        rule: Callable[[Any], bool],
        error_message: str,
        severity: Severity = Severity.ERROR,
        suggested_fix: Optional[str] = None
    ) -> None:
        """
        Add a validation rule for a column.
        
        Args:
            column: Column name to validate
            rule: Function that takes a value and returns True if valid
            error_message: Message to show when validation fails
            severity: Severity level of the validation
            suggested_fix: Optional suggestion for fixing the issue
        """
        if column not in self.validators:
            self.validators[column] = []
            
        self.validators[column].append({
            'rule': rule,
            'message': error_message,
            'severity': severity,
            'suggested_fix': suggested_fix
        })
    
    def add_range_rule(
        self,
        column: str,
        min_value: Optional[Any] = None,
        max_value: Optional[Any] = None,
        severity: Severity = Severity.ERROR
    ) -> None:
        """
        Add a range validation rule for a column.
        
        Args:
            column: Column name to validate
            min_value: Minimum allowed value (inclusive)
            max_value: Maximum allowed value (inclusive)
            severity: Severity level of the validation
        """
        def range_check(value: Any) -> bool:
            if pd.isna(value):
                return True
            
            if min_value is not None and value < min_value:
                return False
            
            if max_value is not None and value > max_value:
                return False
            
            return True
        
        range_str = []
        
        if min_value is not None:
            range_str.append(f'>= {min_value}')
            
        if max_value is not None:
            range_str.append(f'<= {max_value}')
            
        range_desc = ' and '.join(range_str)
        
        self.add_rule(
            column=column,
            rule=range_check,
            error_message=f'Value must be {range_desc}',
            severity=severity,
            suggested_fix=f'Ensure value is {range_desc}'
        )
    
    def add_regex_rule(
        self,
        column: str,
        pattern: str,
        error_message: str,
        severity: Severity = Severity.ERROR,
        flags: re.RegexFlag = re.UNICODE
    ) -> None:
        """
        Add a regex validation rule for a column.
        
        Args:
            column: Column name to validate
            pattern: Regular expression pattern
            error_message: Message to show when validation fails
            severity: Severity level of the validation
            flags: Regular expression flags
        """
        regex = re.compile(pattern, flags)
        
        def regex_check(value: Any) -> bool:
            if pd.isna(value):
                return True
            
            return bool(regex.match(str(value)))
        
        self.add_rule(
            column=column,
            rule=regex_check,
            error_message=error_message,
            severity=severity,
            suggested_fix=f'Match pattern: {pattern}'
        )
    
    def validate(self, df: pd.DataFrame) -> List[ValidationResult]:
        """
        Validate DataFrame fields according to defined rules.
        
        Args:
            df: pandas DataFrame to validate
            
        Returns:
            List of ValidationResult objects
        """
        self.clear_results()
        
        for column, rules in self.validators.items():
            if column not in df.columns:
                self.add_result(ValidationResult(
                    severity=Severity.ERROR,
                    message=f'Cannot validate missing column: {column}',
                    location={'dataframe': 'columns'}
                ))
                continue
                
            for idx, value in df[column].items():
                for rule in rules:
                    if not rule['rule'](value):
                        self.add_result(ValidationResult(
                            severity=rule['severity'],
                            message=rule['message'],
                            location={
                                'column': column,
                                'row': idx
                            },
                            context={'value': str(value)},
                            suggested_fix=rule['suggested_fix']
                        ))
        
        return self.results
