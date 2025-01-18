"""
CSV file structure validator.
"""

import csv
import chardet
from pathlib import Path
from typing import List, Optional, Tuple
from ...utils import configure_logger
from .base_validator import BaseValidator
from ..validation_result import ValidationResult, Severity


class FileValidator(BaseValidator):
    """
    Validates CSV file structure and format.

    Checks:
    - File existence and accessibility
    - File encoding
    - Header presence if required, structure 
    - CSV structure (consistent number of fields)
    """

    def __init__(
        self,
        expected_delimiter: str = ',',
        expected_encoding: Optional[str] = None,
        require_header: bool = True,
    ):
        super().__init__()
        self.__logger = configure_logger(__name__)
        self.__expected_delimiter = expected_delimiter
        self.__expected_encoding = expected_encoding
        self.__require_header = require_header

    def __validate_file_exists(self, file_path: Path) -> bool:
        """Check if file exists and is accessible."""
        if not file_path.exists():
            self.add_result(
                ValidationResult(
                    severity=Severity.ERROR,
                    message='File does not exist',
                    location={'file': str(file_path)},
                )
            )
            return False

        if not file_path.is_file():
            self.add_result(
                ValidationResult(
                    severity=Severity.ERROR,
                    message='Path exists but is not a file',
                    location={'file': str(file_path)},
                )
            )
            return False

        return True

    def __detect_encoding(self, file_path: Path) -> Optional[str]:
        """Detect file encoding and validate against expected encoding."""
        try:
            with open(file_path, 'rb') as f:
                raw_data = f.read()
                detected = chardet.detect(raw_data)

            if (
                self.__expected_encoding
                and detected['encoding'] != self.__expected_encoding
            ):
                self.add_result(
                    ValidationResult(
                        severity=Severity.WARNING,
                        message=f'File encoding mismatch. Expected {self.__expected_encoding}, '
                        f'found {detected["encoding"]}',
                        location={'file': str(file_path)},
                        context={'confidence': detected['confidence']},
                        suggested_fix=f'Convert file to {self.__expected_encoding} encoding',
                    )
                )

            return detected['encoding']

        except Exception as e:
            self.add_result(
                ValidationResult(
                    severity=Severity.ERROR,
                    message=f'Failed to detect file encoding: {str(e)}',
                    location={'file': str(file_path)},
                )
            )
            return None

    def __get_csv_reader(self, file_handle) -> csv.reader:
        """Create a properly configured CSV reader."""
        return csv.reader(
            file_handle, delimiter=self.__expected_delimiter, quoting=csv.QUOTE_MINIMAL
        )

    def __validate_headers(self, headers: List[str], file_path: Path) -> bool:
        """
        Validate CSV headers.
        
        Args:
            headers: List of header fields
            file_path: Path to the CSV file
            
        Returns:
            bool indicating if headers are valid
        """
        if not headers:
            self.add_result(
                ValidationResult(
                    severity=Severity.ERROR,
                    message='CSV file has no headers',
                    location={'file': str(file_path)},
                    suggested_fix='Add header row with column names',
                )
            )
            return False
            
        # Check for empty header fields
        empty_headers = [i for i, header in enumerate(headers, 1) if not header.strip()]
        
        if empty_headers:
            positions = ', '.join(str(pos) for pos in empty_headers)
            self.add_result(
                ValidationResult(
                    severity=Severity.ERROR,
                    message=f'Empty header fields found at position(s): {positions}',
                    location={'file': str(file_path), 'line': 1},
                    suggested_fix='Provide names for all header fields',
                )
            )
            return False
            
        # Check for duplicate headers
        header_counts = {}
        
        for header in headers:
            normalized = header.strip().lower()
            header_counts[normalized] = header_counts.get(normalized, 0) + 1
            
        duplicates = [h for h, count in header_counts.items() if count > 1]
        
        if duplicates:
            dup_list = ', '.join(duplicates)
            self.add_result(
                ValidationResult(
                    severity=Severity.ERROR,
                    message=f'Duplicate headers found: {dup_list}',
                    location={'file': str(file_path), 'line': 1},
                    suggested_fix='Ensure all headers are unique',
                )
            )
            return False
        
        # Check for spaces in header fields
        spaced_headers = [i for i, header in enumerate(headers, 1) if ' ' in header]
        
        if spaced_headers:
            positions = ', '.join(str(pos) for pos in spaced_headers)
            self.add_result(
                ValidationResult(
                    severity=Severity.ERROR,
                    message=f'Headers with spaces found at position(s): {positions}',
                    location={'file': str(file_path), 'line': 1},
                    suggested_fix='Replace spaces with any other character or remove them',
                )
            )
            return False
            
        return True

    def __validate_field_consistency(
        self, reader: csv.reader, file_path: Path
    ) -> Tuple[bool, Optional[int]]:
        """
        Validate consistent number of fields across rows.
        
        Returns:
            Tuple of (is_valid, field_count)
        """
        try:
            # Get and validate headers
            headers = next(reader)
            if not self.__validate_headers(headers, file_path):
                return False, len(headers)
                
            expected_fields = len(headers)
            
            # Check remaining rows
            for i, row in enumerate(reader, 2):
                if len(row) != expected_fields:
                    self.add_result(
                        ValidationResult(
                            severity=Severity.ERROR,
                            message=f'Inconsistent number of fields on line {i}',
                            location={'file': str(file_path), 'line': i},
                            suggested_fix='Check for missing or extra delimiters',
                        )
                    )
                    return False, expected_fields
                    
            return True, expected_fields
            
        except StopIteration:
            self.add_result(
                ValidationResult(
                    severity=Severity.ERROR,
                    message='CSV file is empty',
                    location={'file': str(file_path)},
                )
            )
            return False, None
            
        except csv.Error as e:
            self.add_result(
                ValidationResult(
                    severity=Severity.ERROR,
                    message=f'CSV parsing error: {str(e)}',
                    location={'file': str(file_path)},
                )
            )
            return False, None

    def validate(self, file_path: str | Path) -> List[ValidationResult]:
        """
        Validate a CSV file.

        Args:
            file_path: Path to the CSV file

        Returns:
            List of ValidationResult objects
        """
        self.clear_results()
        file_path = Path(file_path)

        # Basic file validation
        if not self.__validate_file_exists(file_path):
            return self.results

        # Encoding detection
        encoding = self.__detect_encoding(file_path)
        if not encoding:
            return self.results

        # CSV structure validation
        try:
            with open(file_path, encoding=encoding) as f:
                reader = self.__get_csv_reader(f)
                self.__validate_field_consistency(reader, file_path)

        except (csv.Error, UnicodeDecodeError) as e:
            self.add_result(
                ValidationResult(
                    severity=Severity.ERROR,
                    message=f'Failed to parse CSV: {str(e)}',
                    location={'file': str(file_path)},
                )
            )

        return self.results
