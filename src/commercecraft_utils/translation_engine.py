import os
import pandas as pd
from dotenv import load_dotenv
from preprocessor import TextPreprocessor
from translation_service import TranslationService
from utils import get_base_columns, get_language_columns


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
    # TODO: Allow parameters to be passed as arguments to the constructor
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

        # Preprocess values before translation
        # protected_values = [self.preprocessor.preprocess(str(v)) for v in valid_values]

        # Translate using Preprocessesor
        # translations = await self.translation_service.translate_texts(
        #     protected_values, source_lang, target_lang
        # )

        # Translate without using Preprocessesor
        translations = await self.translation_service.translate_texts(
            valid_values, source_lang, target_lang
        )

        # Postprocess translations
        # restored_translations = [
        #     self.preprocessor.postprocess(trs)
        #     for trs in translations
        # ]

        # Create translation dictionary using Postprocessor
        # translation_map = dict(zip(valid_values, restored_translations))

        # Create translation dictionary without using Postprocessor
        translation_map = dict(zip(valid_values, translations))

        return [
            translation_map.get(str(v).strip(), v) if pd.notna(v) else v for v in values
        ]

    async def translate_dataframe(
        self, df: pd.DataFrame, set_columns: list[str] = None, exclude_columns: list[str] = None
    ) -> pd.DataFrame:
        """
        Translate a dataframe using the translation service.

        Args:
            df (pd.DataFrame): Input dataframe to translate.
            set_columns (list[str], optional): Columns containing comma-separated values. Defaults to None.
            exclude_columns (list[str], optional): Columns to exclude from translation. Defaults to None.

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
                        # Preprocess values before translation
                        # protected_elements = [
                        #     self.preprocessor.preprocess(elem)
                        #     for elem in all_elements
                        # ]

                        # Translate using Preprocessesor
                        # translated_elements = await self.translation_service.translate_texts(
                        #     protected_elements,
                        #     self.source_lang.split('-')[0],
                        #     lang.split('-')[0]
                        # )

                        # Translate without using Preprocessesor
                        translated_elements = await self.translate_values(
                            all_elements,
                            self.source_lang.split(self.__lang_separator)[0],
                            lang.split(self.__lang_separator)[0],
                        )
                        
                        # Postprocess translations
                        # restored_elements = [
                        #     self.preprocessor.postprocess(trans)
                        #     for trans in translated_elements
                        # ]

                        # Create translation dictionary using Postprocessor
                        # elem_translations = dict(zip(all_elements, restored_elements))

                        # Create translation dictionary without using Postprocessor
                        elem_translations = dict(zip(all_elements, translated_elements))
                        
                        # TODO: Make sure that translations don't introduce new separators!

                        # Apply translations to original set values
                        df_translated[target_col] = df[source_col].apply(
                            lambda x: self.__set_separator.join(
                                elem_translations.get(str(e).strip(), str(e).strip())
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
                    )
                    df_translated[target_col] = translations

        return df_translated

    async def process_file(
        self, input_path: str, output_path: str = None, set_columns: list[str] = None, exclude_columns: list[str] = None
    ) -> None:
        """
        Process a CSV file and save the translated version.

        Args:
            input_path (str): Path to input CSV file.
            output_path (str, optional): Path to save translated file. Defaults to None.
            set_columns (list[str], optional): Columns containing comma-separated values. Defaults to None.
            exclude_columns (list[str], optional): Columns to exclude from translation. Defaults to None.
        """
        if output_path is None:
            name_parts = input_path.rsplit('.', 1)
            output_path = f'{name_parts[0]}{self.__output_suffix}.{name_parts[1]}'

        df = pd.read_csv(input_path, encoding='utf-8')
        df_translated = await self.translate_dataframe(df, set_columns, exclude_columns)
        df_translated.to_csv(output_path, encoding='utf-8', index=False)
