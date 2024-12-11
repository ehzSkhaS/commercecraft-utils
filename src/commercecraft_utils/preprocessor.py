"""Enhanced preprocessor using Pydantic models for content protection."""
import re
import json
from .models import patterns, Placeholder

class ContentProtector:
    """
    Base class for content protection during translation.
    """

    def __init__(self):
        self.placeholders: list[Placeholder] = []
        self.counter = 0

    def create_placeholder(self, original: str, category: str) -> str:
        """
        Create a unique placeholder for the original text.

        Args:
            original (str): Original text to protect.
            category (str): Category of the protected content.

        Returns:
            str: Unique placeholder string.
        """
        self.counter += 1
        placeholder = f"__PH{category}{self.counter}__"
        self.placeholders.append(Placeholder(
            original=original,
            placeholder=placeholder,
            category=category
        ))
        return placeholder

    def protect(self, text: str) -> str:
        """
        Protect content in text.

        Args:
            text (str): Text to protect.

        Returns:
            str: Text with protected content.

        Raises:
            NotImplementedError: Must be implemented by subclasses.
        """
        raise NotImplementedError

class HTMLProtector(ContentProtector):
    """Protector for HTML tags and entities."""

    def protect(self, text: str) -> str:
        """
        Protect HTML tags and entities.

        Args:
            text (str): Text containing HTML.

        Returns:
            str: Text with protected HTML.
        """
        def replace_tag(match):
            return self.create_placeholder(match.group(0), 'HTML')

        protected = re.sub(patterns.html_field.tag, replace_tag, text)
        protected = re.sub(patterns.html_field.entity, replace_tag, protected)
        return protected

class JSONProtector(ContentProtector):
    """Protector for JSON objects and arrays."""

    def protect(self, text: str) -> str:
        """
        Protect JSON objects and arrays.

        Args:
            text (str): Text containing JSON.

        Returns:
            str: Text with protected JSON.
        """
        def replace_json(match):
            try:
                json.loads(match.group(0))
                return self.create_placeholder(match.group(0), 'JSON')
            except json.JSONDecodeError:
                return match.group(0)

        protected = re.sub(patterns.json_field.object, replace_json, text)
        protected = re.sub(patterns.json_field.array, replace_json, protected)
        return protected

class MeasurementProtector(ContentProtector):
    """Protector for measurements, fractions, and units."""

    def protect(self, text: str) -> str:
        """
        Protect measurements and units.

        Args:
            text (str): Text containing measurements.

        Returns:
            str: Text with protected measurements.
        """
        for field_name, field in patterns.measurement_field.model_fields.items():
            pattern = getattr(patterns.measurement_field, field_name)
            def replace_measurement(match):
                return self.create_placeholder(match.group(0), f'MEASUREMENT_{field_name.upper()}')
            text = re.sub(pattern, replace_measurement, text)
        return text

class TableProtector(ContentProtector):
    """Protector for table-related content."""

    def protect(self, text: str) -> str:
        """
        Protect table structures.

        Args:
            text (str): Text containing tables.

        Returns:
            str: Text with protected tables.
        """
        for field_name, field in patterns.html_table_field.model_fields.items():
            pattern = getattr(patterns.html_table_field, field_name)
            def replace_table(match):
                return self.create_placeholder(match.group(0), f'TABLE_{field_name.upper()}')
            text = re.sub(pattern, replace_table, text, flags=re.DOTALL)
        return text

class TextPreprocessor:
    """
    Text preprocessor that protects special content during translation.
    Includes protectors for HTML, JSON, measurements, and tables.
    """

    def __init__(self):
        self.protectors = [
            HTMLProtector(),
            JSONProtector(),
            MeasurementProtector(),
            TableProtector()
        ]

    def preprocess(self, text: str) -> str:
        """
        Preprocess text by protecting all special content.

        Args:
            text (str): Text to preprocess.

        Returns:
            str: Text with protected content.
        """
        if not text or not isinstance(text, str):
            return text

        protected = text
        for protector in self.protectors:
            protected = protector.protect(protected)
        return protected

    def postprocess(self, text: str) -> str:
        """
        Restore all protected content after translation.

        Args:
            text (str): Text with protected content.

        Returns:
            str: Text with original content restored.
        """
        if not text or not isinstance(text, str):
            return text

        result = text
        # Collect all placeholders from all protectors
        all_placeholders = []
        for protector in self.protectors:
            all_placeholders.extend(protector.placeholders)

        # Sort placeholders by length of placeholder text (longest first)
        all_placeholders.sort(key=lambda x: len(x.placeholder), reverse=True)

        # Restore all placeholders
        for ph in all_placeholders:
            result = result.replace(ph.placeholder, ph.original)

        return result