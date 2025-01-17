import os
import json
import pandas as pd
from typing import List, Any
from .translation_service import TranslationService
from .translation_processor import TranslationProcessor
from .utils import get_base_columns, get_language_columns, configure_logger

class TranslationEngine:
    """
    A robust translation engine for handling multilingual translations of values, dataframes, and files.
    Supports batch processing, content protection, and multiple language pairs.

    Args:
        api_key (str, optional): OpenAI API key for translation service
        source_lang (str, optional): Source language code. Defaults to 'en-US'
        set_separator (str, optional): Separator for set values. Defaults to ';'
        output_suffix (str, optional): Suffix for output files. Defaults to '_translated'
        language_separator (str, optional): Separator for language codes. Defaults to '-'
        field_language_separator (str, optional): Separator for field language. Defaults to '.'
        model (str, optional): OpenAI model for translation service. Defaults to 'gpt-4o-mini'
        max_tokens (int, optional): Maximum tokens for translation service. Defaults to 2000
        temperature (float, optional): Temperature for translation service. Defaults to 0.0
        request_batch_size (int, optional): Number of texts to send in a single API request. Defaults to 50
    """

    def __init__(
        self,
        api_key: str,
        source_lang: str = 'en-US',
        set_separator: str = ';',
        output_suffix: str = '_translated',
        language_separator: str = '-',
        field_language_separator: str = '.',
        model: str = 'gpt-4o-mini',
        max_tokens: int = 2000,
        temperature: float = 0.0,
        request_batch_size: int = 50,
    ):
        self.__logger = configure_logger(__name__)
        
        try:
            # Set parameters
            self.__source_lang = source_lang
            self.__set_separator = set_separator
            self.__output_suffix = output_suffix
            self.__lang_separator = language_separator
            self.__field_lang_separator = field_language_separator
            
            # Initialize translation service
            self.__translation_service = TranslationService(
                api_key=api_key,
                model=model,
                max_tokens=max_tokens,
                temperature=temperature,
                request_batch_size=request_batch_size
            )
            
            self.__processor = TranslationProcessor()
            
        except Exception as e:
            self.__logger.error(f"Error initializing TranslationEngine: {str(e)}")
            raise

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
                preprocessed_text, extracted = self.__processor.preprocess(value)
                
                # Find all JSON placeholders and their content
                json_placeholders = {k: v for k, v in extracted.items() if '__PH__JSON__' in k}
                
                # First translate the main text
                translated = await self.__translation_service.translate_texts(
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
                            self.__logger.error(f"Error parsing JSON content in placeholder {placeholder}")
                            continue

                        # Collect translatable strings (excluding placeholders)
                        to_translate = []
                        self.__collect_json_strings(json_data, to_translate)
                        
                        if to_translate:
                            # Translate collected strings
                            translated_strings = await self.__translation_service.translate_texts(
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
                        self.__logger.error(f"Error processing JSON in placeholder {placeholder}: {str(e)}")
                
                # Postprocess to restore special patterns with translated content
                postprocessed_text = self.__processor.postprocess(processed_text)
                translations.append(postprocessed_text)
            except Exception as e:
                self.__logger.error(f"Error translating value '{value}': {str(e)}")
                translations.append(value)

        # Create a mapping of original values to translations
        translation_map = dict(zip(valid_values, translations))
        
        # Return translations in original order, keeping invalid values unchanged
        return [translation_map.get(str(v).strip(), v) if pd.notna(v) else v for v in values]

    async def translate_dataframe(
        self, df: pd.DataFrame, set_columns: List[str] = None, 
        exclude_columns: List[str] = None,
        save_callback: callable = None,
        chunk_size: int = 50
    ) -> pd.DataFrame:
        """
        Translate a dataframe using the translation service.

        Args:
            df (pd.DataFrame): Input dataframe to translate.
            set_columns (List[str], optional): Columns containing comma-separated values.
            exclude_columns (List[str], optional): Columns to exclude from translation.
            save_callback (callable, optional): Callback function to save progress periodically.
            chunk_size (int, optional): Number of strings to translate in one batch. Defaults to 50.

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
            if self.__source_lang in lang_columns:
                source_col = lang_columns[self.__source_lang]
                source_dtype = df[source_col].dtype
                for lang, target_col in lang_columns.items():
                    if lang != self.__source_lang:
                        df_translated[target_col] = df_translated[target_col].astype(source_dtype)
        
        base_columns = get_base_columns(
            df.columns,
            self.__field_lang_separator,
        )
        
        # Remove the excluded columns from translation
        base_columns = list(set(base_columns) - set(exclude_columns))
        
        self.__logger.info(f"Starting translation of {len(base_columns)} base columns for {len(df)} rows")
        
        translation_count = 0
        skipped_count = 0
        
        for idx, base_col in enumerate(base_columns, 1):
            self.__logger.info(f"Processing column {idx}/{len(base_columns)}: {base_col}")
            
            lang_columns = get_language_columns(
                df,
                base_col,
                self.__field_lang_separator,
            )

            if self.__source_lang not in lang_columns:
                self.__logger.warning(f"Source language {self.__source_lang} not found in column {base_col}")
                continue

            source_col = lang_columns[self.__source_lang]
            target_langs = [lang for lang in lang_columns.keys() if lang != self.__source_lang]
            self.__logger.info(f"Translating to {len(target_langs)} target languages: {', '.join(target_langs)}")
            
            # Get all rows that need translation for any target language
            rows_to_translate = pd.Series(False, index=df.index)
            for lang in target_langs:
                target_col = lang_columns[lang]
                rows_to_translate |= pd.isna(df[target_col]) & pd.notna(df[source_col])
            
            if any(rows_to_translate):
                # Collect all strings to translate
                strings_to_translate = []
                row_indices = []
                
                for row_idx in rows_to_translate[rows_to_translate].index:
                    source_text = str(df.at[row_idx, source_col])
                    if base_col in set_columns:
                        # For set columns, split and add each element
                        elements = [elem.strip() for elem in source_text.split(self.__set_separator) if elem.strip()]
                        strings_to_translate.extend(elements)
                        row_indices.extend([row_idx] * len(elements))
                    else:
                        # For regular columns, add the whole text
                        strings_to_translate.append(source_text)
                        row_indices.append(row_idx)
                
                # Process in batches
                for i in range(0, len(strings_to_translate), chunk_size):
                    batch = strings_to_translate[i:i + chunk_size]
                    batch_indices = row_indices[i:i + chunk_size]
                    
                    # Translate to all target languages in parallel
                    for target_lang in target_langs:
                        target_col = lang_columns[target_lang]
                        
                        try:
                            # Translate batch
                            translated_batch = await self.__translation_service.translate_texts(
                                batch,
                                self.__source_lang.split(self.__lang_separator)[0],
                                target_lang.split(self.__lang_separator)[0]
                            )
                            
                            # Update translations in dataframe
                            for j, (row_idx, translation) in enumerate(zip(batch_indices, translated_batch)):
                                if base_col in set_columns:
                                    # For set columns, collect all translations for the same row
                                    current_translations = df_translated.at[row_idx, target_col]
                                    if pd.isna(current_translations):
                                        current_translations = []
                                    elif isinstance(current_translations, str):
                                        current_translations = current_translations.split(self.__set_separator)
                                    current_translations.append(translation)
                                    df_translated.at[row_idx, target_col] = self.__set_separator.join(current_translations)
                                else:
                                    # For regular columns, just set the translation
                                    df_translated.at[row_idx, target_col] = translation
                            
                            translation_count += len(batch)
                            
                        except Exception as e:
                            self.__logger.error(f"Error translating batch to {target_lang}: {str(e)}")
                            skipped_count += len(batch)
                            continue
                    
                    # Save progress if callback provided
                    if save_callback:
                        await save_callback(df_translated)
                        
        self.__logger.info(f"Translation completed. Translated {translation_count} strings, skipped {skipped_count}")
        return df_translated

    async def process_file(
        self, input_path: str, output_path: str = None, 
        set_columns: List[str] = None, 
        exclude_columns: List[str] = None,
        save_interval: int = 20,
        chunk_size: int = 50
    ) -> None:
        """
        Process a CSV file and save the translated version.

        Args:
            input_path (str): Path to input CSV file.
            output_path (str, optional): Path to save translated file.
            set_columns (List[str], optional): Columns containing comma-separated values.
            exclude_columns (List[str], optional): Columns to exclude from translation.
            save_interval (int, optional): Save progress every N translations. If None, only save at the end.
            chunk_size (int, optional): Number of DataFrame rows to process in one batch. Controls save frequency.
        """
        self.__logger.info(f"Processing file: {input_path}")
        
        if output_path is None:
            filename, ext = os.path.splitext(input_path)
            output_path = f"{filename}{self.__output_suffix}{ext}"
            
        df = pd.read_csv(input_path)
        
        async def save_progress(current_df: pd.DataFrame):
            self.__logger.info(f"Saving progress to {output_path}")
            current_df.to_csv(output_path, index=False)
            
        df_translated = await self.translate_dataframe(
            df,
            set_columns=set_columns,
            exclude_columns=exclude_columns,
            save_callback=save_progress if save_interval else None,
            chunk_size=chunk_size
        )
        
        # Save the final result
        df_translated.to_csv(output_path, index=False)
        self.__logger.info(f"Translation completed and saved to {output_path}")
