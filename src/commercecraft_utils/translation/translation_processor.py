import emoji
import json
import regex as re
from typing import Any, Union


class TranslationProcessor:
    """A text preprocessing utility that extracts and manages patterns in text.

    This class provides functionality to extract, replace, and restore various patterns
    in text including JSON structures, control characters, HTML tags, URLs, email addresses,
    and emojis. It uses a placeholder system to temporarily replace these patterns during
    processing while maintaining their original content.

    Features:
        - Pattern extraction with customizable regex patterns
        - JSON structure preservation and processing
        - Emoji handling
        - Bidirectional conversion (text ↔ placeholders)
        - Order-preserving pattern management

    Attributes:
        patterns (dict[str, Union[str, None]]): Configurable patterns for text extraction
        counter (int): Current counter for generating unique placeholders
        extracted (dict[str, Any]): Mapping of placeholders to their original content

    Example:
        >>> tp = TextPreprocessor()
        >>> text = "Hello 😊! Email me at user@example.com"
        >>> cleaned, extracted = tp.preprocess(text)
        >>> print(cleaned)
        'Hello <@__PH__EMOJI__1__@>! Email me at <@__PH__EMAIL__2__@>'
        >>> original = tp.postprocess(cleaned)
        >>> assert original == text
    """
    def __init__(self):
        """Initializes the TextPreprocessor with default patterns, counter, and extracted data."""
        self.__counter: int = 1
        self.__extracted: dict[str, Any] = {}
        self.__patterns: dict[str, Union[str, None]] = {
            'CTRL': r'[\x00-\x09\x0B\x0C\x0E-\x1F\x7F\u0080-\u009F\u200B\u00AD\u2028\u2029\uFEFF\u2060]',
            'JSON': r'\{(?:[^{}]|(?R))*\}',
            'HTML': r'<(\/?[a-zA-Z][a-zA-Z0-9-]*)(\s+[^>]+)?>',
            'URL': r'(https?:\/\/(?:[a-zA-Z0-9-]+\.)+[a-zA-Z]{2,}(?::\d+)?(?:\/[^\s?#]*)?(?:\?[^\s#]*)?(?:#[^\s]*)?)',
            'EMAIL': r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b',
            'EMOJI': None,
        }
        self.__keys_shadow: tuple[str] = tuple(self.__patterns.keys())
        self.__compile_patterns()

    @property
    def counter(self) -> int:
        """Get the current counter value used for generating unique placeholders.
        
        Returns:
            int: The current counter value.
        """
        return self.__counter

    @property
    def extracted(self) -> dict[str, Any]:
        """Get a copy of the extracted data dictionary containing all replaced patterns.
        
        Returns:
            dict[str, Any]: A copy of the dictionary mapping placeholders to their original content.
        """
        return self.__extracted.copy()

    @property
    def patterns(self) -> dict[str, Union[str, None]]:
        """Get a copy of the current patterns dictionary used for text processing.
        
        Returns:
            dict[str, Union[str, None]]: A copy of the patterns dictionary mapping pattern names to regex patterns.
        """
        return self.__patterns.copy()

    @patterns.setter
    def patterns(self, new_patterns: dict[str, Union[str, None]]) -> None:
        """Set new patterns dictionary while maintaining order and using existing patterns as fallback.
        
        Args:
            new_patterns (dict[str, Union[str, None]]): Dictionary of new patterns to update.
                                                       Missing patterns will keep their existing values.
        """
        ordered_patterns = {}
        
        for key in self.__keys_shadow:
            ordered_patterns[key] = new_patterns.get(key, self.__patterns[key])
        
        for key, value in new_patterns.items():
            if key not in self.__keys_shadow:
                ordered_patterns[key] = value
        
        self.__patterns = ordered_patterns
        self.__compile_patterns()
        
    def __compile_patterns(self) -> None:
        """
        Compiles a combined regex with named capture groups for all token patterns.
        """
        self.__token_regex = re.compile('|'.join(f'(?P<{name}>{pattern})' for name, pattern in self.__patterns.items()))

    def __extract_pattern(self, text: str, prefix: str, pattern: str, is_emoji: bool = False) -> str:
        """Extract patterns or emojis from text and replace them with unique placeholders.
        
        Args:
            text (str): The input text to process.
            pattern (str): The regex pattern to match (ignored for emojis).
            prefix (str): The prefix to use in the placeholder (e.g., 'JSON', 'URL').
            is_emoji (bool, optional): Whether to process emojis instead of regex. Defaults to False.
            
        Returns:
            str: The text with matched patterns replaced by placeholders.
        """
        def replacer(match):
            nonlocal self
            placeholder = f'<@__PH__{prefix}__{self.__counter}__@>'
            self.__extracted[placeholder] = match.group()
            self.__counter += 1
            
            return placeholder
        
        if is_emoji:
            matches = emoji.emoji_list(text)
            
            for match in matches:
                placeholder = f'<@__PH__{prefix}__{self.__counter}__@>'
                self.__extracted[placeholder] = match['emoji']
                text = text.replace(match['emoji'], placeholder, 1)
                self.__counter += 1
        else:
            text = re.sub(pattern, replacer, text)
            
        return text

    def placeholder_builder(self, text: str) -> str:
        """Process text by extracting all configured patterns and replacing them with placeholders.
    
        This method applies all patterns in the order they were defined, replacing matches
        with unique placeholders that can be used to restore the original content later.
    
        Args:
            text (str): The input text to process.
    
        Returns:
            str: The processed text with all patterns replaced by placeholders.
        """
        for key, pattern in self.__patterns.items():
            text = self.__extract_pattern(text, key, pattern, is_emoji=(key == 'EMOJI'))
            
        return text

    def __process_json_value(self, value: Any) -> Union[dict, list, str]:
        """Recursively process JSON values to extract and replace patterns with placeholders.
        
        Args:
            value (Any): The JSON value to process (can be dict, list, or primitive type).
            
        Returns:
            Union[dict, list, str]: The processed value with patterns replaced by placeholders.
        """
        if isinstance(value, dict):
            return self.__process_json_dict(value)
        elif isinstance(value, list):
            return self.__process_json_list(value)
        else:
            return self.placeholder_builder(str(value))

    def __process_json_dict(self, dct: dict[str, Any]) -> dict[str, Any]:
        """Process a dictionary by replacing patterns in both keys and values with placeholders.
        
        Args:
            dct (dict[str, Any]): The dictionary to process.
            
        Returns:
            dict[str, Any]: A new dictionary with patterns replaced by placeholders.
        """
        processed_dct: dict[str, Any] = {}
        
        for key, value in dct.items():
            key_text = self.placeholder_builder(str(key))
            value_text = self.__process_json_value(value)
            processed_dct[key_text] = value_text
            
        return processed_dct

    def __process_json_list(self, lst: list[Any]) -> list[Any]:
        """Process a list by replacing patterns in all items with placeholders.
        
        Args:
            lst (list[Any]): The list to process.
            
        Returns:
            list[Any]: A new list with patterns replaced by placeholders.
        """
        return [self.__process_json_value(item) for item in lst]

    def __process_json(self, text: str) -> str:
        """Process JSON content, recursively extracting patterns.
        
        Args:
            text (str): The JSON text to process.
            
        Returns:
            str: The processed JSON text with patterns replaced by placeholders.
        """
        try:
            json_parsed = json.loads(text)
        except json.JSONDecodeError:
            return text

        processed_json = self.__process_json_value(json_parsed)
        
        return json.dumps(processed_json, indent=4)

    def preprocess(self, text: str) -> tuple[str, dict[str, Any]]:
        """Preprocess the input text by extracting patterns and processing JSON structures.
        
        Args:
            text (str): The input text to preprocess.
            
        Returns:
            tuple[str, dict[str, Any]]: A tuple containing the preprocessed text and the extracted data dictionary.
        """
        text = self.placeholder_builder(text)

        for key, value in list(self.__extracted.items()):
            if '__PH__JSON__' in key:
                processed_json = self.__process_json(value)
                self.__extracted[key] = processed_json

        return text, self.__extracted

    def postprocess(self, text: str) -> str:
        """Restore the original text by reinserting extracted content back into placeholders.
        
        Args:
            text (str): The preprocessed text to restore.
            
        Returns:
            str: The original text with placeholders replaced by their original content.
        """
        sorted_placeholders = sorted(self.__extracted.items(), key = lambda value: ('__PH__JSON__' not in value[0], value[0]))
        
        for placeholder, original_content in sorted_placeholders:
            text = text.replace(placeholder, original_content)
        
        return text