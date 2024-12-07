from pydantic import BaseModel, Field

class HTMLPatterns(BaseModel):
    """Patterns for HTML content."""
    tag: str = Field(default=r'<[^>]+>', description="Pattern to match any HTML tag")
    entity: str = Field(default=r'&[a-zA-Z]+;|&#\d+;', description="Pattern to match HTML entities")

class JSONPatterns(BaseModel):
    """Patterns for JSON content."""
    object: str = Field(default=r'\{[^{}]*\}', description="Pattern to match JSON objects")
    array: str = Field(default=r'\[[^\[\]]*\]', description="Pattern to match JSON arrays")

class MeasurementPatterns(BaseModel):
    """Patterns for measurements and units."""
    complex_measurement: str = Field(
        default=r'\d+(?:\s*\+\s*\d+/\d+)?\s*(?:cans?)\s*\(\d+\s*g\)',
        description="Pattern for complex measurements with cans and grams"
    )
    simple_measurement: str = Field(
        default=r'(\d+(?:\.\d+)?)\s*(lb|kg|g|cans?)',
        description="Pattern for simple measurements"
    )
    fraction: str = Field(
        default=r'(\d+(?:\s*\+\s*\d+/\d+))',
        description="Pattern for fractions"
    )
    number: str = Field(
        default=r'\b\d+(?:\.\d+)?\b',
        description="Pattern for numbers"
    )
    unit: str = Field(
        default=r'\b(lb|kg|g|cans?)\b',
        description="Pattern for units"
    )

class TablePatterns(BaseModel):
    """Patterns for table structures."""
    table_tag: str = Field(
        default=r'<table[^>]*>.*?</table>',
        description="Pattern for table tags"
    )
    row_tag: str = Field(
        default=r'(?:<tbody[^>]*>.*?</tbody>|<tr[^>]*>.*?</tr>)',
        description="Pattern for row tags including tbody"
    )
    cell_tag: str = Field(
        default=r'(?:<td[^>]*>.*?</td>|<th[^>]*>.*?</th>)',
        description="Pattern for cell tags"
    )
    header_tag: str = Field(
        default=r'(?:<thead[^>]*>.*?</thead>|<th[^>]*>.*?</th>)',
        description="Pattern for header tags"
    )

class ContentPatterns(BaseModel):
    """Collection of all content patterns."""
    html_field: HTMLPatterns = Field(default_factory=HTMLPatterns)
    json_field: JSONPatterns = Field(default_factory=JSONPatterns)
    measurement_field: MeasurementPatterns = Field(default_factory=MeasurementPatterns)
    html_table_field: TablePatterns = Field(default_factory=TablePatterns)

class Placeholder(BaseModel):
    """Model for content placeholders."""
    original: str = Field(..., description="Original content that was replaced")
    placeholder: str = Field(..., description="Placeholder text used for replacement")
    category: str = Field(..., description="Category of the content (HTML, JSON, etc.)")

# Create a singleton instance
patterns = ContentPatterns()