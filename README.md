# CommerceCraft Utils API Documentation

A Python tool for translating CSV files between different languages.

## Installation

You can install the package in several ways:

### From PyPI (Recommended)
```bash
pip install commercecraft-utils
```

### From Source
1. Clone the repository:
```bash
git clone https://github.com/ehzSkhaS/commercecraft-utils.git
cd commercecraft-utils
```

2. Install in development mode:
```bash
pip install -e .
```

## Configuration

Before using the translation utility, you need to set up your environment variables in a `.env` file. By default, the utility looks for this file in the root of your project, but you can specify a custom path when initializing the TranslationEngine:

```python
# Using default .env in project root
translator = TranslationEngine()

# Using a custom .env file path
translator = TranslationEngine(dotenv_path='/path/to/your/.env')
```

### Environment Variables

Create a `.env` file with the following variables:

```env
# OpenAI Configuration
OPENAI_API_KEY=your-api-key-here
OPENAI_MODEL=gpt-3.5-turbo  # or gpt-4 for better translations
MAX_TOKENS=2000
TEMPERATURE=0.3
BATCH_SIZE=10

# Translation Configuration
SET_SEPARATOR=,             # Separator for set fields (e.g., tags, categories)
OUTPUT_SUFFIX=_translated   # Suffix added to translated files
LANGUAGE_SEPARATOR=-        # Separator for language codes (e.g., en-US)
FIELD_LANGUAGE_SEPARATOR=.  # Separator between field name and language (e.g., name.en-US)
```

### Environment Variables Explained

#### OpenAI Settings
- `OPENAI_API_KEY`: Your OpenAI API key for authentication
- `OPENAI_MODEL`: The model to use for translations
- `MAX_TOKENS`: Maximum number of tokens per API call
- `TEMPERATURE`: Controls randomness in translations (0.0-1.0)
- `BATCH_SIZE`: Number of texts to translate in one batch

#### Translation Settings
- `SET_SEPARATOR`: Used to split and join set fields (e.g., "tag1,tag2,tag3")
- `OUTPUT_SUFFIX`: Added to output files (e.g., "products.csv" → "products_translated.csv")
- `LANGUAGE_SEPARATOR`: Used in language codes (e.g., "en-US", "es-ES")
- `FIELD_LANGUAGE_SEPARATOR`: Separates field names from language codes in columns

## Usage

```python
import asyncio
from commercecraft_utils import TranslationEngine

PROD_PATH = 'data/products.csv'
CAT_PATH = 'data/categories.csv'


async def main():
translator = TranslationEngine()

    # Process categories
    print("Processing categories...")
    await translator.process_file(CAT_PATH, exclude_columns=['slug'])

    # Process products
    print("Processing products...")
    await translator.process_file(PROD_PATH, set_columns=['benefits', ],exclude_columns=['slug', 'recomendations'])


if __name__ == "__main__":
    asyncio.run(main())

```

## Testing

Run the tests using:
```bash
python -m pytest tests/
```

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Translation Engine

A robust translation engine that handles translation of values, dataframes, and files. Supports batch processing, content protection, and multiple language pairs.

### class TranslationEngine

Main class for handling translation of dataframes and files with support for various content types.

#### Constructor

```python
def __init__(self, dotenv_path: str = None, source_lang: str = 'en-US')
```

**Arguments:**
- `dotenv_path` (str, optional): Path to the .env file. Defaults to None.
- `source_lang` (str, optional): Source language code. Defaults to 'en-US'.

**Required Environment Variables:**
- `SET_SEPARATOR`: Separator for set fields
- `OUTPUT_SUFFIX`: Suffix for output files
- `LANGUAGE_SEPARATOR`: Separator for language codes
- `FIELD_LANGUAGE_SEPARATOR`: Separator for field language codes

### Methods

#### translate_values
```python
async def translate_values(self, values: list[str], source_lang: str, target_lang: str) -> list[str]
```
Translate a list of values using the translation service.

**Arguments:**
- `values` (list[str]): List of strings to translate
- `source_lang` (str): Source language code
- `target_lang` (str): Target language code

**Returns:**
- list[str]: List of translated strings

#### translate_dataframe
```python
async def translate_dataframe(self, df: pd.DataFrame, set_columns: list[str] = None, exclude_columns: list[str] = None) -> pd.DataFrame
```
Translate a dataframe using the translation service. Handles both regular fields and set fields (comma-separated values).

- `set_columns`: List of columns containing comma-separated values that need to be translated individually
- `exclude_columns`: List of columns to exclude from translation

**Arguments:**
- `df` (pd.DataFrame): Input dataframe
- `set_columns` (list[str], optional): Columns to treat as sets. Defaults to None.
- `exclude_columns` (list[str], optional): Columns to exclude from translation. Defaults to None.

**Returns:**
- pd.DataFrame: Translated dataframe

#### process_file
```python
async def process_file(self, input_path: str, output_path: str = None, set_columns: list[str] = None, exclude_columns: list[str] = None) -> None
```
Process a CSV file and save the translated version. If no output path is provided, appends the OUTPUT_SUFFIX to the input filename.

**Arguments:**
- `input_path` (str): Path to input CSV file
- `output_path` (str, optional): Path to output CSV file. Defaults to None.
- `set_columns` (list[str], optional): Columns to treat as sets. Defaults to None.
- `exclude_columns` (list[str], optional): Columns to exclude from translation. Defaults to None.

## Translation Service

Handles communication with the OpenAI API for text translation. Supports batch processing and retries.

### class TranslationService

Handles communication with the OpenAI API for text translation.

#### Constructor

```python
def __init__(self, dotenv_path: str = None)
```

**Arguments:**
- `dotenv_path` (str, optional): Path to the .env file. Defaults to None.

**Required Environment Variables:**
- `OPENAI_API_KEY`: OpenAI API key
- `OPENAI_MODEL`: OpenAI model to use
- `MAX_TOKENS`: Maximum tokens for API calls
- `TEMPERATURE`: Temperature for API calls
- `BATCH_SIZE`: Number of texts to translate in one batch

### Methods

#### translate_texts
```python
async def translate_texts(self, texts: list[str], source_lang: str, target_lang: str) -> list[str]
```
Translates a list of texts in batches. Handles rate limiting between batches.

**Arguments:**
- `texts` (list[str]): List of texts to translate
- `source_lang` (str): Source language code
- `target_lang` (str): Target language code

**Returns:**
- list[str]: List of translated texts

#### translate_batch
```python
async def translate_batch(self, texts: list[str], source_lang: str, target_lang: str, max_retries: int = 3) -> list[str]
```
Translate a batch of texts with retry mechanism.

**Arguments:**
- `texts` (list[str]): List of texts to translate
- `source_lang` (str): Source language code
- `target_lang` (str): Target language code
- `max_retries` (int, optional): Maximum number of retries. Defaults to 3.

**Returns:**
- list[str]: List of translated texts

## Text Preprocessor

Protects special content during translation to ensure it remains intact.

### class TextPreprocessor

Handles preprocessing of text before translation to protect special content.

#### Constructor

```python
def __init__(self)
```

Initializes preprocessor with HTML, JSON, Measurement, and Table protectors.

### Methods

#### preprocess
```python
def preprocess(self, text: str) -> str
```
Preprocess text by protecting all special content.

**Arguments:**
- `text` (str): Text to preprocess

**Returns:**
- str: Preprocessed text with protected content

#### postprocess
```python
def postprocess(self, text: str) -> str
```
Restore all protected content after translation.

**Arguments:**
- `text` (str): Text to postprocess

**Returns:**
- str: Original text with restored content

## Utility Functions

### get_base_columns
```python
def get_base_columns(columns: list[str], fls: str = '.') -> set[str]
```
Extract base column names without language suffix.

**Arguments:**
- `columns` (list[str]): List of column names
- `fls` (str, optional): Field language separator. Defaults to '.'.

**Returns:**
- set[str]: Set of base column names

### get_language_columns
```python
def get_language_columns(df: pd.DataFrame, base_name: str, fls: str = '.') -> dict[str, str]
```
Get all language variations of a column.

**Arguments:**
- `df` (pd.DataFrame): Input dataframe
- `base_name` (str): Base column name
- `fls` (str, optional): Field language separator. Defaults to '.'.

**Returns:**
- dict[str, str]: Dictionary mapping language codes to column names
