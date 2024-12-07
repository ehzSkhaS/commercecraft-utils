import re
from src.models import ContentPatterns, Placeholder

def test_content_patterns_creation():
    """Test creating ContentPatterns instance."""
    patterns = ContentPatterns()
    assert patterns.html_field is not None
    assert patterns.json_field is not None
    assert patterns.measurement_field is not None
    assert patterns.html_table_field is not None

def test_placeholder_creation():
    """Test creating Placeholder instance."""
    placeholder = Placeholder(
        original="<table>",
        placeholder="__PHTABLE1__",
        category="HTML"
    )
    assert placeholder.original == "<table>"
    assert placeholder.placeholder == "__PHTABLE1__"
    assert placeholder.category == "HTML"

def test_html_patterns():
    """Test HTML patterns."""
    patterns = ContentPatterns()
    assert '<div>' in re.findall(patterns.html_field.tag, '<div>content</div>')
    assert '&amp;' in re.findall(patterns.html_field.entity, 'text &amp; more')

def test_json_patterns():
    """Test JSON patterns."""
    patterns = ContentPatterns()
    assert '{"key": "value"}' in re.findall(patterns.json_field.object, 'text {"key": "value"} more')
    assert '[1,2,3]' in re.findall(patterns.json_field.array, 'text [1,2,3] more')

def test_measurement_patterns():
    """Test measurement patterns."""
    patterns = ContentPatterns()
    assert '2 cans (169 g)' in re.findall(
        patterns.measurement_field.complex_measurement,
        'Weight: 2 cans (169 g)'
    )
    assert ('6.6', 'lb') in re.findall(
        patterns.measurement_field.simple_measurement,
        'Weight: 6.6 lb'
    )

def test_table_patterns():
    """Test table patterns."""
    patterns = ContentPatterns()
    html = """
    <table>
        <thead><tr><th>Header</th></tr></thead>
        <tbody><tr><td>Cell</td></tr></tbody>
    </table>
    """
    assert re.search(patterns.html_table_field.table_tag, html, re.DOTALL) is not None
    assert re.search(patterns.html_table_field.header_tag, html, re.DOTALL) is not None
    assert re.search(patterns.html_table_field.row_tag, html, re.DOTALL) is not None
    assert re.search(patterns.html_table_field.cell_tag, html, re.DOTALL) is not None