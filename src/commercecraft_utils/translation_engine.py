import os
import json
import pandas as pd
from typing import Dict, List, Optional
from dotenv import load_dotenv
from .preprocessor import TextPreprocessor
from .translation_service import TranslationService
from .utils import get_base_columns, get_language_columns


class JsonTranslationConfig:
    """Configuration for JSON field translation."""
    def __init__(self, translate_keys: bool = True, translate_values: bool = False):
        self.translate_keys = translate_keys
        self.translate_values = translate_values


class TranslationEngine:
    """
    A robust translation engine for handling multilingual translations of values, dataframes, and files.
    Supports batch processing, content protection, and multiple language pairs.

    Args:
        dotenv_path (str, optional): Path to the .env file. Defaults to None.
        source_lang (str, optional): Source language code. Defaults to 'en-US'.

    Raises:
        ValueError: If required environment variables are missing.
    """

    def __init__(self, dotenv_path: str = None, source_lang: str = 'en-US'):
        if not load_dotenv(dotenv_path=dotenv_path if dotenv_path else '.env'):
            raise ValueError('No .env file found')

        self.source_lang = source_lang
        self.preprocessor = TextPreprocessor()
        self.translation_service = TranslationService(dotenv_path=dotenv_path)
        
        # Get environment variables
        if (set_separator := os.getenv('SET_SEPARATOR')) is None:
            raise ValueError('SET_SEPARATOR environment variable is required')
        self.__set_separator = set_separator
            
        if (output_suffix := os.getenv('OUTPUT_SUFFIX')) is None:
            raise ValueError('OUTPUT_SUFFIX environment variable is required')
        self.__output_suffix = output_suffix
            
        if (lang_separator := os.getenv('LANGUAGE_SEPARATOR')) is None:
            raise ValueError('LANGUAGE_SEPARATOR environment variable is required')
        self.__lang_separator = lang_separator  
            
        if (field_lang_separator := os.getenv('FIELD_LANGUAGE_SEPARATOR')) is None:
            raise ValueError('FIELD_LANGUAGE_SEPARATOR environment variable is required')
        self.__field_lang_separator = field_lang_separator

    async def _translate_json_field(self, json_str: str, source_lang: str, target_lang: str, config: JsonTranslationConfig) -> str:
        """
        Translate a JSON string based on configuration.
        
        Args:
            json_str (str): JSON string to translate
            source_lang (str): Source language code
            target_lang (str): Target language code
            config (JsonTranslationConfig): Configuration for what to translate
            
        Returns:
            str: Translated JSON string
        """
        try:
            data = json.loads(json_str)
            if not isinstance(data, dict):
                return json_str
                
            result = {}
            
            # Handle key translation
            if config.translate_keys:
                keys = list(data.keys())
                translated_keys = await self.translation_service.translate_texts(
                    keys,
                    source_lang.split(self.__lang_separator)[0],
                    target_lang.split(self.__lang_separator)[0]
                )
                key_map = dict(zip(keys, translated_keys))
            else:
                key_map = {k: k for k in data.keys()}
                
            # Handle value translation
            if config.translate_values:
                values = [v for v in data.values() if isinstance(v, str) and not v.startswith('http')]
                if values:
                    translated_values = await self.translation_service.translate_texts(
                        values,
                        source_lang.split(self.__lang_separator)[0],
                        target_lang.split(self.__lang_separator)[0]
                    )
                    value_map = dict(zip(values, translated_values))
                else:
                    value_map = {}
            
            # Reconstruct JSON with translations
            for old_key, value in data.items():
                new_key = key_map[old_key]
                new_value = value_map.get(value, value) if config.translate_values else value
                result[new_key] = new_value
                
            return json.dumps(result)
        except (json.JSONDecodeError, Exception):
            return json_str

    async def translate_values(
        self, values: list[str], source_lang: str, target_lang: str, 
        json_fields: Optional[Dict[str, JsonTranslationConfig]] = None
    ) -> list[str]:
        """
        Translate a list of values using the translation service.

        Args:
            values (list[str]): List of values to translate.
            source_lang (str): Source language code.
            target_lang (str): Target language code.
            json_fields (Dict[str, JsonTranslationConfig], optional): Configuration for JSON fields.

        Returns:
            list[str]: List of translated values.
        """

        # Filter out empty values
        valid_values = [v for v in values if pd.notna(v) and str(v).strip()]

        if not valid_values:
            return values

        translations = []
        for value in valid_values:
            try:
                if json_fields and value.strip().startswith('{') and value.strip().endswith('}'):
                    # Use first JSON config if field name not specified
                    config = next(iter(json_fields.values()))
                    translations.append(await self._translate_json_field(value, source_lang, target_lang, config))
                else:
                    # Regular translation
                    translated = await self.translation_service.translate_texts(
                        [value],
                        source_lang.split(self.__lang_separator)[0],
                        target_lang.split(self.__lang_separator)[0]
                    )
                    translations.append(translated[0])
            except Exception:
                translations.append(value)

        translation_map = dict(zip(valid_values, translations))
        return [translation_map.get(str(v).strip(), v) if pd.notna(v) else v for v in values]

    async def translate_dataframe(
        self, df: pd.DataFrame, set_columns: List[str] = None, 
        exclude_columns: List[str] = None,
        json_fields: Dict[str, JsonTranslationConfig] = None
    ) -> pd.DataFrame:
        """
        Translate a dataframe using the translation service.

        Args:
            df (pd.DataFrame): Input dataframe to translate.
            set_columns (List[str], optional): Columns containing comma-separated values.
            exclude_columns (List[str], optional): Columns to exclude from translation.
            json_fields (Dict[str, JsonTranslationConfig], optional): Configuration for JSON fields.

        Returns:
            pd.DataFrame: Translated dataframe.
        """
        if set_columns is None:
            set_columns = []

        if exclude_columns is None:
            exclude_columns = []

        df_translated = df.copy()
        base_columns = get_base_columns(
            df.columns,
            self.__field_lang_separator,
        )
        
        # Remove the excluded columns from translation
        base_columns = list(set(base_columns) - set(exclude_columns))

        for base_col in base_columns:
            lang_columns = get_language_columns(
                df,
                base_col,
                self.__field_lang_separator,
            )

            if self.source_lang not in lang_columns:
                continue

            source_col = lang_columns[self.source_lang]

            for lang, target_col in lang_columns.items():
                if lang == self.source_lang:
                    continue

                if base_col in set_columns:
                    # Split set fields and translate each element
                    set_values = df[source_col].fillna('')
                    all_elements = [
                        elem.strip()
                        for value in set_values
                        for elem in (value.split(self.__set_separator) if value else [])
                    ]
                    
                    if all_elements:
                        translations = await self.translate_values(
                            all_elements,
                            self.source_lang.split(self.__lang_separator)[0],
                            lang.split(self.__lang_separator)[0],
                            json_fields
                        )
                        
                        # Apply translations to original set values
                        df_translated[target_col] = df[source_col].apply(
                            lambda x: self.__set_separator.join(
                                translations[all_elements.index(str(e).strip())]
                                for e in str(x).split(self.__set_separator)
                            )
                            if pd.notna(x)
                            else x
                        )
                else:
                    # Translate regular fields
                    values = df[source_col].fillna('').tolist()
                    translations = await self.translate_values(
                        values,
                        self.source_lang.split(self.__lang_separator)[0],
                        lang.split(self.__lang_separator)[0],
                        json_fields
                    )
                    df_translated[target_col] = translations

        return df_translated

    async def process_file(
        self, input_path: str, output_path: str = None, 
        set_columns: List[str] = None, 
        exclude_columns: List[str] = None,
        json_fields: Dict[str, Dict[str, bool]] = None
    ) -> None:
        """
        Process a CSV file and save the translated version.

        Args:
            input_path (str): Path to input CSV file.
            output_path (str, optional): Path to save translated file.
            set_columns (List[str], optional): Columns containing comma-separated values.
            exclude_columns (List[str], optional): Columns to exclude from translation.
            json_fields (Dict[str, Dict[str, bool]], optional): Configuration for JSON fields.
                Example: {
                    'column_name': {
                        'translate_keys': True,
                        'translate_values': False
                    }
                }
        """
        if output_path is None:
            name_parts = input_path.rsplit('.', 1)
            output_path = f"{name_parts[0]}{self.__output_suffix}.{name_parts[1]}"

        df = pd.read_csv(input_path, encoding='utf-8')
        
        # Convert json_fields dict to JsonTranslationConfig objects
        json_config = {
            col: JsonTranslationConfig(
                translate_keys=config.get('translate_keys', True),
                translate_values=config.get('translate_values', False)
            )
            for col, config in (json_fields or {}).items()
        }
        
        df_translated = await self.translate_dataframe(
            df, set_columns, exclude_columns, json_config
        )
        df_translated.to_csv(output_path, encoding='utf-8', index=False)
