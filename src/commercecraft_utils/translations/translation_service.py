import asyncio
from openai import AsyncOpenAI
from .utils import configure_logger

class TranslationService:
    """
    Service for handling translations using OpenAI's API.

    Args:
        api_key (str): OpenAI API key for translation service
        model (str, optional): OpenAI model for translation service. Defaults to 'gpt-4o-mini'
        max_tokens (int, optional): Maximum tokens for translation service. Defaults to 2000
        temperature (float, optional): Temperature for translation service. Defaults to 0.0
        request_batch_size (int, optional): Number of texts to send in a single API request. Defaults to 50
    """

    def __init__(
        self,
        api_key: str,
        model: str = 'gpt-4o-mini',
        max_tokens: int = 2000,
        temperature: float = 0.0,
        request_batch_size: int = 50,
    ):
        """Initialize the TranslationService."""
        self.__logger = configure_logger(__name__)
        
        try:
            # Validate and set parameters
            if not api_key:
                self.__logger.error("API key is required but was not provided")
                raise ValueError("API key is required")

            self.__api_key = api_key
            self.__model = model
            self.__max_tokens = int(max_tokens)
            self.__temperature = float(temperature)
            self.__request_batch_size = int(request_batch_size)
            
            # Initialize OpenAI client
            self.__client = AsyncOpenAI(api_key=self.__api_key)
            
            self.__logger.info(
                "TranslationService initialized with: model=%s, max_tokens=%d, "
                "temperature=%.2f, request_batch_size=%d",
                self.__model, self.__max_tokens, self.__temperature, 
                self.__request_batch_size
            )
            
        except Exception as e:
            self.__logger.error("Failed to initialize TranslationService: %s", str(e))
            raise

    def _create_system_prompt(self, source_lang: str, target_lang: str) -> str:
        """
        Create the system prompt for translation.

        Args:
            source_lang (str): Source language code.
            target_lang (str): Target language code.

        Returns:
            str: Formatted system prompt.
        """
        return f"""You are a professional translator from {source_lang} to {target_lang}.
            
        CRITICAL INSTRUCTIONS FOR LINE HANDLING:
        1. You will receive text split into numbered sections like this:
           [1] First line of text
           [2] Second line of text
        2. You MUST keep the exact same numbering in your response
        3. NEVER add or remove line numbers
        4. NEVER split or combine lines
        5. Translate ONLY the text after the [N] marker
        
        Additional instructions:
        - Maintain all formatting and special characters
        - Translate ONLY the text portions
        - Keep the same tone and formality level
        - Preserve any technical terms or proper nouns
        - Numbers should be kept in their original format
        - Do not add explanations or notes
        - Do not include the original text
        - Do not add quotation marks unless they exist in the original
        - Do not translate anything between {{{{}}}}
        """

    def _preprocess_text(self, text: str) -> list[str]:
        """Split text into numbered lines."""
        lines = text.split('\n')
        return [f"[{i+1}] {line}" for i, line in enumerate(lines) if line.strip()]

    def _process_response(self, content: str) -> list[str]:
        """Process the response, extracting only the translated text after line numbers."""
        translations = []
        for line in content.split('\n'):
            line = line.strip()
            if line and '[' in line and ']' in line:
                # Extract everything after the [N] marker
                translated_text = line.split(']', 1)[1].strip()
                translations.append(translated_text)
        return translations

    async def translate_batch(
        self, texts: list[str], source_lang: str, target_lang: str, max_retries: int = 3
    ) -> list[str]:
        if not texts:
            self.__logger.warning("Empty text list provided for translation")
            return []

        self.__logger.info(
            "Starting batch translation of %d texts from %s to %s",
            len(texts), source_lang, target_lang
        )

        all_translations = []
        for idx, text in enumerate(texts, 1):
            # Split and number each line
            numbered_lines = self._preprocess_text(text)
            
            try:
                self.__logger.info(
                    "HTTP Request %d/%d: Translating text...",
                    idx, len(texts)
                )
                
                response = await self.__client.chat.completions.create(
                    model=self.__model,
                    messages=[
                        {
                            'role': 'system',
                            'content': self._create_system_prompt(source_lang, target_lang),
                        },
                        {
                            'role': 'user',
                            'content': '\n'.join(numbered_lines),
                        },
                    ],
                    max_tokens=self.__max_tokens,
                    temperature=self.__temperature,
                )

                translated_lines = self._process_response(response.choices[0].message.content)
                
                if len(translated_lines) != len(numbered_lines):
                    self.__logger.error(
                        "Line count mismatch in text %d/%d. Original: %d, Translated: %d",
                        idx, len(texts), len(numbered_lines), len(translated_lines)
                    )
                    self.__logger.debug("Original numbered lines: %s", numbered_lines)
                    self.__logger.debug("Translated lines: %s", translated_lines)
                    raise ValueError(f'Expected {len(numbered_lines)} lines in translation, got {len(translated_lines)}')
                
                # Join the translated lines back together
                final_translation = '\n'.join(translated_lines)
                all_translations.append(final_translation)

            except Exception as e:
                self.__logger.error(
                    "Translation failed for text %d/%d: %s. Text preview: %s",
                    idx, len(texts), str(e),
                    text[:100] + "..." if len(text) > 100 else text
                )
                raise

        self.__logger.info(
            "Successfully translated batch of %d texts from %s to %s",
            len(texts), source_lang, target_lang
        )
        
        return all_translations

    async def translate_texts(
        self, texts: list[str], source_lang: str, target_lang: str
    ) -> list[str]:
        """
        Translate a list of texts from source language to target language.

        Args:
            texts (list[str]): List of texts to translate
            source_lang (str): Source language code
            target_lang (str): Target language code

        Returns:
            list[str]: List of translated texts.
        """
        if not texts:
            self.__logger.warning("Empty text list provided for translation")
            return []

        self.__logger.info(
            "Starting translation of %d texts from %s to %s in batches of %d",
            len(texts), source_lang, target_lang, self.__request_batch_size
        )

        all_translations = []
        batch_count = (len(texts) + self.__request_batch_size - 1) // self.__request_batch_size

        for i in range(0, len(texts), self.__request_batch_size):
            batch = texts[i : i + self.__request_batch_size]
            current_batch = (i // self.__request_batch_size) + 1
            
            self.__logger.info(
                "Processing batch %d/%d (%d texts)",
                current_batch, batch_count, len(batch)
            )

            try:
                translations = await self.translate_batch(
                    batch,
                    source_lang,
                    target_lang
                )
                all_translations.extend(translations)

                if i + self.__request_batch_size < len(texts):
                    self.__logger.debug("Applying rate limiting between batches")
                    await asyncio.sleep(1)

            except Exception as e:
                self.__logger.error(
                    "Failed to translate batch %d/%d: %s",
                    current_batch, batch_count, str(e)
                )
                # Return partial translations up to this point
                return all_translations

        self.__logger.info(
            "Completed translation of all %d texts from %s to %s",
            len(texts), source_lang, target_lang
        )

        return all_translations
