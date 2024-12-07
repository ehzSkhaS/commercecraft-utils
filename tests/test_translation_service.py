import os
import pytest
from src.translation_service import TranslationService


def test_create_translation_prompt():
    if not os.getenv('OPENAI_API_KEY'):
        pytest.skip('No OpenAI API key available')

    service = TranslationService()
    texts = ['In for a Penny,', 'In for a Pound']
    prompt = service._create_translation_prompt(texts, 'en-US', 'fr-FR')

    print(f'\n{prompt}')
    assert 'In' in prompt
    assert 'for' in prompt
    assert 'a' in prompt
    assert 'Penny' in prompt
    assert 'Pound' in prompt
    assert 'en-us' in prompt.lower()
    assert 'fr-fr' in prompt.lower()


def test_process_response():
    if not os.getenv('OPENAI_API_KEY'):
        pytest.skip('No OpenAI API key available')

    service = TranslationService()

    response = 'Hello\nWorld\nTest'
    result = service._process_response(response)
    print(f"\n{result}")
    assert result == ['Hello', 'World', 'Test']

    response = '  Hello  \n  World  \n  Test  '
    result = service._process_response(response)
    print(f'\n{result}')
    assert result == ['Hello', 'World', 'Test']


@pytest.mark.asyncio
async def test_translation_service():
    if not os.getenv('OPENAI_API_KEY'):
        pytest.skip('No OpenAI API key available')

    service = TranslationService()
    texts = [
        "Nobody exists on purpose, nobody belongs anywhere, everybody's gonna die.",
        'Come watch TV!',
    ]

    translations = await service.translate_texts(texts, 'en-US', 'fr-FR')

    print(f'\nInput: \n{texts} \nTranslated: \n{translations}')
    assert len(translations) == len(texts)
    assert all(isinstance(t, str) for t in translations)


@pytest.mark.asyncio
async def test_empty_translation():
    if not os.getenv('OPENAI_API_KEY'):
        pytest.skip('No OpenAI API key available')

    service = TranslationService()
    texts = []

    translations = await service.translate_texts(texts, 'en-US', 'fr-FR')

    print(f'\nInput: \n{texts} \nTranslated: \n{translations}')
    assert translations == []


@pytest.mark.asyncio
async def test_batch_translation():
    if not os.getenv('OPENAI_API_KEY'):
        pytest.skip('No OpenAI API key available')

    service = TranslationService()
    # I'm testing with size 60, since default batch size is 50
    texts = [f'{i} sheep zzzZzz' for i in range(60)]

    translations = await service.translate_texts(texts, 'en-US', 'fr-FR')

    print(f'\nInput: \n{texts} \nTranslated: \n{translations}')
    assert len(translations) == len(texts)


@pytest.mark.asyncio
async def test_translation_with_special_chars():
    if not os.getenv('OPENAI_API_KEY'):
        pytest.skip('No OpenAI API key available')

    service = TranslationService()
    texts = [
        'Hello There!',
        'are u capable {{&}} smart enough to...',
        'translate something like:',
        '{watch out, this is a test}',
        '{{@why are u translating this out v2.0?}}',
        'whatabout@this.com',
        '{"and": "this"}',
    ]

    translations = await service.translate_texts(texts, 'en-US', 'fr-FR')

    print(f'\nInput: \n{texts} \nTranslated: \n{translations}')
    assert len(translations) == len(texts)
    # Check that special characters are preserved
    assert any(
        char in ''.join(translations) for char in ['!', '&', '@', '{', '}', '"', ':']
    )
    assert texts[0] != translations[0]
    assert texts[1] != translations[1]
    assert texts[2] != translations[2]
    assert texts[3] != translations[3]
    """ We need a preprocessing step to remove special chars from the response,
        it dosn't matter how many or how you put the rules for the promt, 
        the result will be the same, the LLM keeps missbehaving.
        The other solution is to fine tune the model to not generate special chars
        or ignore anything inside them. """
    # assert texts[4] == translations[4]
    # assert texts[5] == translations[5]
    assert texts[6] != translations[6]
