import pandas as pd
import json
from typing import Tuple, Dict


def get_base_columns(columns: list[str], fls: str = '.') -> set[str]:
    """
    Extract base column names without language suffix.

    Args:
        columns (list[str]): List of column names.
        fls (str, optional): Field language separator. Defaults to '.'.

    Returns:
        set[str]: Set of base column names.
    """
    base_columns = set()

    for col in columns:
        if fls in col:
            base_name = col.split(fls)[0]
            base_columns.add(base_name)

    return base_columns


def get_language_columns(df: pd.DataFrame, base_name: str, fls: str = '.') -> dict[str, str]:
    """
    Get all language variations of a column.

    Args:
        df (pd.DataFrame): Input dataframe.
        base_name (str): Base column name.
        fls (str, optional): Field language separator. Defaults to '.'.

    Returns:
        dict[str, str]: Dictionary mapping language codes to column names.
    """
    return {
        col.split(fls)[1]: col
        for col in df.columns
        if col.startswith(f"{base_name}{fls}")
    }


def extract_json_for_translation(json_str: str) -> Tuple[Dict[str, str], Dict[str, str]]:
    """
    Extract translatable text from a JSON string while preserving structure.
    
    Args:
        json_str (str): JSON string containing key-value pairs where keys are translatable
                       and values are URLs or other non-translatable content
    
    Returns:
        Tuple[Dict[str, str], Dict[str, str]]: A tuple containing:
            - Dictionary mapping placeholder keys to original text for translation
            - Dictionary mapping placeholder keys to non-translatable values
    """
    try:
        data = json.loads(json_str)
        text_for_translation = {}
        preserved_values = {}
        
        for idx, (key, value) in enumerate(data.items()):
            placeholder = f"__PLACEHOLDER_{idx}__"
            text_for_translation[placeholder] = key
            preserved_values[placeholder] = value
            
        return text_for_translation, preserved_values
    except json.JSONDecodeError:
        return {}, {}


def reconstruct_json_with_translations(translated_texts: Dict[str, str], 
                                     preserved_values: Dict[str, str]) -> str:
    """
    Reconstruct JSON string with translated text and preserved values.
    
    Args:
        translated_texts (Dict[str, str]): Dictionary mapping placeholders to translated text
        preserved_values (Dict[str, str]): Dictionary mapping placeholders to preserved values
    
    Returns:
        str: Reconstructed JSON string with translations
    """
    result = {}
    for placeholder in translated_texts:
        if placeholder in preserved_values:
            result[translated_texts[placeholder]] = preserved_values[placeholder]
    
    return json.dumps(result)
