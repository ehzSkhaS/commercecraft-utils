import pandas as pd


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
