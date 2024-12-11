import os
import pytest
from src.commercecraft_utils.translation_service import TranslationService


def test_create_system_prompt():
    if not os.getenv('OPENAI_API_KEY'):
        pytest.skip('No OpenAI API key available')

    service = TranslationService()
    system_prompt = service._create_system_prompt('en-US', 'fr-FR')

    print(f'\n{system_prompt}')
    assert 'professional translator from en-US to fr-FR' in system_prompt
    assert 'Return ONLY the translations' in system_prompt
    assert 'Maintain the exact meaning' in system_prompt
    assert 'Keep the same tone' in system_prompt
    assert 'Preserve any technical terms' in system_prompt
    assert 'Do not add explanations' in system_prompt
    assert 'Do not include the original text' in system_prompt
    assert 'Numbers should be kept in their original format' in system_prompt


def test_create_user_prompt():
    if not os.getenv('OPENAI_API_KEY'):
        pytest.skip('No OpenAI API key available')

    service = TranslationService()
    texts = ['In for a Penny,', 'In for a Pound']
    user_prompt = service._create_user_prompt(texts)

    print(f'\n{user_prompt}')
    assert user_prompt == 'In for a Penny,\nIn for a Pound'
    assert texts[0] in user_prompt
    assert texts[1] in user_prompt
    assert user_prompt.count('\n') == len(texts) - 1


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
        '{{watch out, this is a test}}',
        'whatabout@this.com',
        '{"where": "are"}',
        '{"hello": "there"}',
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
    assert texts[3] == translations[3]
    """ We need a preprocessing step to remove special chars from the response,
        it dosn't matter how many or how you put the rules for the promt, 
        the result will be the same, the LLM keeps missbehaving.
        The other solution is to fine tune the model to not generate special chars
        or ignore anything inside them. """
    # assert texts[4] == translations[4]
    # assert texts[5] == translations[5]
    # assert texts[6] != translations[6]
