import pandas as pd
from src.utils import get_base_columns, get_language_columns

def test_get_base_columns_with_language_suffix():
    """Test extracting base column names from columns with language suffixes."""
    columns = ['name.en-US', 'name.fr-FR', 'description.en-US', 'tags.en-US']
    base_cols = get_base_columns(columns)
    
    print(f'\nInput: \n{columns} \nOutput: \n{base_cols}')
    assert base_cols == {'name', 'description', 'tags'}

def test_get_base_columns_with_mixed_columns():
    """Test extracting base columns with a mix of language and non-language columns."""
    columns = ['name.en-US', 'name.fr-FR', 'id', 'price', 'description.fr-FR']
    base_cols = get_base_columns(columns)
    
    print(f'\nInput: \n{columns} \nOutput: \n{base_cols}')
    assert base_cols == {'name', 'description'}

def test_get_base_columns_empty_list():
    """Test handling empty column list."""
    columns = []
    base_cols = get_base_columns(columns)
    
    print(f'\nInput: \n{columns} \nOutput: \n{base_cols}')
    assert base_cols == set()

def test_get_language_columns():
    """Test getting language variations of a column."""
    data = {
        'name.en-US': ['Whitestone'],
        'name.fr-FR': ['Pierre Blanche'],
        'name.es-ES': ['Piedra Blanca'],
        'description.en-US': ['Ancient city of marble']
    }
    df = pd.DataFrame(data)
    
    lang_columns = get_language_columns(df, 'name')
    expected = {
        'en-US': 'name.en-US',
        'fr-FR': 'name.fr-FR',
        'es-ES': 'name.es-ES'
    }
    
    print(f'\nInput: \n{df} \nOutput: \n{lang_columns}')
    assert lang_columns == expected

def test_get_language_columns_no_matches():
    """Test getting language columns when no matches exist."""
    data = {
        'id': [1],
        'price': [10.99],
        'stock': [100]
    }
    df = pd.DataFrame(data)
    
    lang_columns = get_language_columns(df, 'name')
    
    print(f'\nInput: \n{df} \nOutput: \n{lang_columns}')
    assert lang_columns == {}
