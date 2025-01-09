import os
import asyncio
from typing import Optional
from dotenv import load_dotenv
from openai import AsyncOpenAI


class TranslationService:
    """
    Service for handling translations using the OpenAI API.
    Supports batch processing and implements retry mechanisms.

    Args:
        dotenv_path (str, optional): Path to the .env file. Defaults to None.

    Raises:
        ValueError: If required environment variables are missing or invalid.
    """

    def __init__(self, dotenv_path: Optional[str] = None):
        if not load_dotenv(dotenv_path=dotenv_path if dotenv_path else '.env'):
            raise ValueError('No .env file found')
        
        # Get environment variables
        if (api_key := os.getenv('OPENAI_API_KEY')) is None:
            raise ValueError('OPENAI_API_KEY environment variable is required')
        self.client = AsyncOpenAI(api_key=api_key)
        
        if (model := os.getenv('OPENAI_MODEL')) is None:
            raise ValueError('OPENAI_MODEL environment variable is required')
        self.__model = model
        
        if (max_tokens := os.getenv('MAX_TOKENS')) is None:
            raise ValueError('MAX_TOKENS environment variable is required')
        
        try:
            self.__max_tokens = int(max_tokens)
        except ValueError:
            raise ValueError('MAX_TOKENS must be a valid integer')
        
        if (temperature := os.getenv('TEMPERATURE')) is None:
            raise ValueError('TEMPERATURE environment variable is required')
        
        try:
            self.__temperature = float(temperature)
        except ValueError:
            raise ValueError('TEMPERATURE must be a valid float')
        
        if (batch_size := os.getenv('BATCH_SIZE')) is None:
            raise ValueError('BATCH_SIZE environment variable is required')
        
        try:
            self.__batch_size = int(batch_size)
        except ValueError:
            raise ValueError('BATCH_SIZE must be a valid integer')

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
            return []

        all_translations = []
        for text in texts:
            # Split and number each line
            numbered_lines = self._preprocess_text(text)
            
            try:
                response = await self.client.chat.completions.create(
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
                    print(f"Line count mismatch. Original: {len(numbered_lines)}, Translated: {len(translated_lines)}")
                    print(f"Original numbered lines: {numbered_lines}")
                    print(f"Translated lines: {translated_lines}")
                    raise ValueError(f'Expected {len(numbered_lines)} lines in translation, got {len(translated_lines)}')
                
                # Join the translated lines back together
                final_translation = '\n'.join(translated_lines)
                all_translations.append(final_translation)

            except Exception as e:
                print(f"Translation failed: {str(e)}")
                print(f"Original text: {text}")
                raise

        return all_translations

    async def translate_texts(
        self, texts: list[str], source_lang: str, target_lang: str
    ) -> list[str]:
        """
        Translate multiple texts with batching and rate limiting.

        Args:
            texts (list[str]): List of texts to translate.
            source_lang (str): Source language code.
            target_lang (str): Target language code.

        Returns:
            list[str]: List of translated texts.
        """
        all_translations = []

        for i in range(0, len(texts), self.__batch_size):
            batch = texts[i : i + self.__batch_size]
            try:
                translations = await self.translate_batch(
                    batch,
                    source_lang,
                    target_lang
                )
                all_translations.extend(translations)

                if i + self.__batch_size < len(texts):
                    # Rate limiting between batches
                    await asyncio.sleep(1)
            except Exception as e:
                print(f'Failed to translate batch {i//self.__batch_size + 1}: {str(e)} \n {all_translations}')
                # Return partial translations up to this point
                return all_translations

        return all_translations
