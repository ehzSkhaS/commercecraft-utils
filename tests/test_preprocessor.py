from src.preprocessor import TextPreprocessor

def test_html_protection():
    """Test protecting HTML content."""
    preprocessor = TextPreprocessor()
    text = "<table><tr><td>Content</td></tr></table>"
    processed = preprocessor.preprocess(text)
    restored = preprocessor.postprocess(processed)
    assert restored == text

def test_json_protection():
    """Test protecting JSON content."""
    preprocessor = TextPreprocessor()
    text = '{"name": "Product", "price": 10.99}'
    processed = preprocessor.preprocess(text)
    restored = preprocessor.postprocess(processed)
    assert restored == text

def test_complex_table_content():
    """Test protecting complex table content."""
    preprocessor = TextPreprocessor()
    text = """<table>
        <thead><tr><th>Weight</th><th>Amount</th></tr></thead>
        <tbody><tr><td>6.6 lb (3 kg)</td><td>2 cans (169 g)</td></tr></tbody>
    </table>"""
    processed = preprocessor.preprocess(text)
    restored = preprocessor.postprocess(processed)
    assert restored.replace(' ', '') == text.replace(' ', '')

def test_mixed_content():
    """Test protecting mixed content types."""
    preprocessor = TextPreprocessor()
    text = '<div>Weight: 6.6 lb (3 kg) <span>{"unit": "kg"}</span></div>'
    processed = preprocessor.preprocess(text)
    restored = preprocessor.postprocess(processed)
    assert restored == text

def test_nested_structures():
    """Test protecting nested structures."""
    preprocessor = TextPreprocessor()
    text = '<div>{"data": {"weight": "2 cans (169 g)"}}</div>'
    processed = preprocessor.preprocess(text)
    restored = preprocessor.postprocess(processed)
    assert restored == text

def test_html_entities():
    """Test protecting HTML entities."""
    preprocessor = TextPreprocessor()
    text = "Product &amp; Price: &euro;10.99"
    processed = preprocessor.preprocess(text)
    restored = preprocessor.postprocess(processed)
    assert restored == text

def test_multiple_json_objects():
    """Test protecting multiple JSON objects."""
    preprocessor = TextPreprocessor()
    text = '{"id": 1} and {"id": 2}'
    processed = preprocessor.preprocess(text)
    restored = preprocessor.postprocess(processed)
    assert restored == text

def test_empty_input():
    """Test handling empty input."""
    preprocessor = TextPreprocessor()
    assert preprocessor.preprocess("") == ""
    assert preprocessor.preprocess(None) is None

def test_text_without_special_content():
    """Test handling text without special content."""
    preprocessor = TextPreprocessor()
    text = "Regular text without any special content"
    processed = preprocessor.preprocess(text)
    assert processed == text

def test_measurement_in_json():
    """Test protecting measurements within JSON."""
    preprocessor = TextPreprocessor()
    text = '{"weight": "6.6 lb (3 kg)"}'
    processed = preprocessor.preprocess(text)
    restored = preprocessor.postprocess(processed)
    assert restored == text