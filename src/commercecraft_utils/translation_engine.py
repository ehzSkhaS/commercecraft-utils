import os
import json
import pandas as pd
from typing import List, Any
from dotenv import load_dotenv
from .translation_service import TranslationService
from .utils import get_base_columns, get_language_columns
from .translation_processor import TranslationProcessor
import logging

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
        self.processor = TranslationProcessor()
        self.translation_service = TranslationService(dotenv_path=dotenv_path)
        
        # Set up logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
        
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

    def __should_translate_string(self, s: str) -> bool:
        """Check if a string should be translated.
        
        Since TranslationProcessor already handles special patterns (URLs, emails, etc.),
        we only need to check for placeholders and invalid input.
        
        Args:
            s: String to check
            
        Returns:
            bool: True if the string should be translated
        """
        # Handle None or non-string input
        if not isinstance(s, str):
            return False
            
        # Don't translate empty strings
        if not s.strip():
            return False
            
        # Don't translate placeholders
        if '<@__PH__' in s and s.endswith('__@>'):
            return False
            
        return True

    def __collect_json_strings(self, obj: Any, strings: list[str]) -> None:
        """Helper function to collect all translatable strings from a JSON object.
        
        Args:
            obj: The JSON object to process (can be dict, list, or primitive).
            strings: List to collect strings into.
        """
        if isinstance(obj, dict):
            # Collect translatable keys and values
            for k, v in obj.items():
                if isinstance(k, str) and self.__should_translate_string(k):
                    strings.append(str(k))
                self.__collect_json_strings(v, strings)
        elif isinstance(obj, list):
            for item in obj:
                self.__collect_json_strings(item, strings)
        elif isinstance(obj, str) and self.__should_translate_string(obj):
            strings.append(obj)

    def __replace_json_strings(self, obj: Any, translations_map: dict[str, str]) -> Any:
        """Helper function to replace strings in a JSON object with their translations.
        
        Args:
            obj: The JSON object to process (can be dict, list, or primitive).
            translations_map: Dictionary mapping original strings to their translations.
            
        Returns:
            The processed object with strings replaced by their translations.
        """
        if isinstance(obj, dict):
            return {
                (translations_map.get(str(k), k) if self.__should_translate_string(str(k)) else k): 
                self.__replace_json_strings(v, translations_map)
                for k, v in obj.items()
            }
        elif isinstance(obj, list):
            return [self.__replace_json_strings(item, translations_map) for item in obj]
        elif isinstance(obj, str):
            return translations_map.get(obj, obj) if self.__should_translate_string(obj) else obj
        return obj

    async def translate_values(
        self, values: list[str], source_lang: str, target_lang: str
    ) -> list[str]:
        """
        Translate a list of values using the translation service.

        Args:
            values (list[str]): List of values to translate.
            source_lang (str): Source language code.
            target_lang (str): Target language code.

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
                # Preprocess text to protect special patterns (including JSON)
                preprocessed_text, extracted = self.processor.preprocess(value)
                
                # Find all JSON placeholders and their content
                json_placeholders = {k: v for k, v in extracted.items() if '__PH__JSON__' in k}
                
                # First translate the main text
                translated = await self.translation_service.translate_texts(
                    [preprocessed_text],
                    source_lang.split(self.__lang_separator)[0],
                    target_lang.split(self.__lang_separator)[0]
                )
                processed_text = translated[0]

                # Now handle each JSON placeholder
                for placeholder, json_content in json_placeholders.items():
                    try:
                        # The JSON content is already preprocessed, so we need to:
                        # 1. Parse it while keeping placeholders intact
                        # 2. Collect translatable strings (non-placeholders)
                        # 3. Translate those strings
                        # 4. Replace them in the JSON structure
                        # 5. Update the placeholder
                        
                        # Parse JSON while preserving placeholders
                        try:
                            json_data = json.loads(json_content)
                        except json.JSONDecodeError:
                            self.logger.error(f"Error parsing JSON content in placeholder {placeholder}")
                            continue

                        # Collect translatable strings (excluding placeholders)
                        to_translate = []
                        self.__collect_json_strings(json_data, to_translate)
                        
                        if to_translate:
                            # Translate collected strings
                            translated_strings = await self.translation_service.translate_texts(
                                to_translate,
                                source_lang.split(self.__lang_separator)[0],
                                target_lang.split(self.__lang_separator)[0]
                            )
                            
                            # Create translation mapping
                            translations_map = dict(zip(to_translate, translated_strings))
                            
                            # Replace strings in JSON while preserving placeholders
                            translated_json = self.__replace_json_strings(json_data, translations_map)
                            
                            # Convert back to string with proper formatting
                            translated_json_str = json.dumps(translated_json)
                            
                            # Update the placeholder content
                            extracted[placeholder] = translated_json_str
                            
                    except Exception as e:
                        self.logger.error(f"Error processing JSON in placeholder {placeholder}: {str(e)}")
                
                # Postprocess to restore special patterns with translated content
                postprocessed_text = self.processor.postprocess(processed_text)
                translations.append(postprocessed_text)
            except Exception as e:
                self.logger.error(f"Error translating value '{value}': {str(e)}")
                translations.append(value)

        # Create a mapping of original values to translations
        translation_map = dict(zip(valid_values, translations))
        
        # Return translations in original order, keeping invalid values unchanged
        return [translation_map.get(str(v).strip(), v) if pd.notna(v) else v for v in values]

    async def translate_dataframe(
        self, df: pd.DataFrame, set_columns: List[str] = None, 
        exclude_columns: List[str] = None,
        save_callback: callable = None
    ) -> pd.DataFrame:
        """
        Translate a dataframe using the translation service.

        Args:
            df (pd.DataFrame): Input dataframe to translate.
            set_columns (List[str], optional): Columns containing comma-separated values.
            exclude_columns (List[str], optional): Columns to exclude from translation.
            save_callback (callable, optional): Callback function to save progress periodically.

        Returns:
            pd.DataFrame: Translated dataframe.
        """
        if set_columns is None:
            set_columns = []

        if exclude_columns is None:
            exclude_columns = []

        df_translated = df.copy()
        
        # Ensure target columns have the same dtype as source columns
        for base_col in get_base_columns(
            df.columns,
            self.__field_lang_separator,
        ):
            lang_columns = get_language_columns(
                df,
                base_col,
                self.__field_lang_separator,
            )
            if self.source_lang in lang_columns:
                source_col = lang_columns[self.source_lang]
                source_dtype = df[source_col].dtype
                for lang, target_col in lang_columns.items():
                    if lang != self.source_lang:
                        df_translated[target_col] = df_translated[target_col].astype(source_dtype)
        
        base_columns = get_base_columns(
            df.columns,
            self.__field_lang_separator,
        )
        
        # Remove the excluded columns from translation
        base_columns = list(set(base_columns) - set(exclude_columns))
        
        self.logger.info(f"Starting translation of {len(base_columns)} base columns for {len(df)} rows")
        
        translation_count = 0
        skipped_count = 0
        
        for idx, base_col in enumerate(base_columns, 1):
            self.logger.info(f"Processing column {idx}/{len(base_columns)}: {base_col}")
            
            lang_columns = get_language_columns(
                df,
                base_col,
                self.__field_lang_separator,
            )

            if self.source_lang not in lang_columns:
                self.logger.warning(f"Source language {self.source_lang} not found in column {base_col}")
                continue

            source_col = lang_columns[self.source_lang]
            target_langs = [lang for lang in lang_columns.keys() if lang != self.source_lang]
            self.logger.info(f"Translating to {len(target_langs)} target languages: {', '.join(target_langs)}")

            for lang, target_col in lang_columns.items():
                if lang == self.source_lang:
                    continue

                if base_col in set_columns:
                    # Split set fields and translate each element
                    rows_to_translate = df[pd.isna(df[target_col]) & pd.notna(df[source_col])].index
                    
                    if not rows_to_translate.empty:
                        all_elements = [
                            elem.strip()
                            for idx in rows_to_translate
                            for elem in (str(df.at[idx, source_col]).split(self.__set_separator) if df.at[idx, source_col] else [])
                        ]
                        
                        if all_elements:
                            self.logger.info(f"Translating {len(all_elements)} unique elements for set column {base_col} to {lang}")
                            translations = await self.translate_values(
                                all_elements,
                                self.source_lang,
                                lang
                            )
                            translation_count += len(all_elements)
                            
                            # Apply translations to original set values
                            for idx in rows_to_translate:
                                if pd.notna(df.at[idx, source_col]):
                                    self.logger.info(f"Processing row {idx} for set column {base_col}")
                                    translated_value = self.__set_separator.join(
                                        translations[all_elements.index(str(e).strip())]
                                        for e in str(df.at[idx, source_col]).split(self.__set_separator)
                                    )
                                    df_translated.at[idx, target_col] = translated_value
                                    
                                    if save_callback and translation_count > 0:
                                        await save_callback(df_translated)
                    else:
                        skipped_rows = len(df[pd.notna(df[target_col]) & pd.notna(df[source_col])])
                        if skipped_rows > 0:
                            self.logger.info(f"Skipped {skipped_rows} rows for column {base_col} to {lang} - translations already exist")
                            skipped_count += skipped_rows
                else:
                    # Translate regular fields
                    rows_to_translate = df[pd.isna(df[target_col]) & pd.notna(df[source_col])].index
                    
                    if not rows_to_translate.empty:
                        for idx in rows_to_translate:
                            value = df.at[idx, source_col]
                            if pd.notna(value):
                                self.logger.info(f"Processing row {idx} for column {base_col}")
                                translations = await self.translate_values(
                                    [str(value)],
                                    self.source_lang,
                                    lang
                                )
                                translation_count += 1
                                
                                # Convert back to source dtype if needed
                                translated_value = translations[0]
                                if pd.api.types.is_numeric_dtype(df[source_col].dtype):
                                    try:
                                        translated_value = df[source_col].dtype.type(translated_value)
                                    except (ValueError, TypeError):
                                        self.logger.warning(f"Could not convert translated value '{translated_value}' to {df[source_col].dtype} for column {base_col}")
                                df_translated.at[idx, target_col] = translated_value
                                
                                if save_callback and translation_count > 0:
                                    await save_callback(df_translated)
                    else:
                        skipped_rows = len(df[pd.notna(df[target_col]) & pd.notna(df[source_col])])
                        if skipped_rows > 0:
                            self.logger.info(f"Skipped {skipped_rows} rows for column {base_col} to {lang} - translations already exist")
                            skipped_count += skipped_rows

        self.logger.info(f"DataFrame translation completed - {translation_count} translations performed, {skipped_count} existing translations skipped")
        return df_translated

    async def process_file(
        self, input_path: str, output_path: str = None, 
        set_columns: List[str] = None, 
        exclude_columns: List[str] = None,
        save_interval: int = 20
    ) -> None:
        """
        Process a CSV file and save the translated version.

        Args:
            input_path (str): Path to input CSV file.
            output_path (str, optional): Path to save translated file.
            set_columns (List[str], optional): Columns containing comma-separated values.
            exclude_columns (List[str], optional): Columns to exclude from translation.
            save_interval (int, optional): Save progress every N translations. If None, only save at the end.
        """
        if output_path is None:
            name_parts = input_path.rsplit('.', 1)
            output_path = f"{name_parts[0]}{self.__output_suffix}.{name_parts[1]}"
            
        # Read the CSV file
        df = pd.read_csv(input_path, encoding='utf-8')
        
        translation_count = 0
        last_save = 0
        
        async def save_progress(current_df: pd.DataFrame):
            nonlocal translation_count, last_save
            translation_count += 1
            
            if save_interval and (translation_count - last_save) >= save_interval:
                self.logger.info(f"Saving progress after {translation_count} translations...")
                current_df.to_csv(output_path, index=False)
                last_save = translation_count
                
        # Translate the dataframe
        df_translated = await self.translate_dataframe(
            df, 
            set_columns=set_columns,
            exclude_columns=exclude_columns,
            save_callback=save_progress if save_interval else None
        )
        
        # Save the final result
        df_translated.to_csv(output_path, index=False)
        self.logger.info(f"Translation completed and saved to {output_path}")
