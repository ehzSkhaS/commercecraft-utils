import os
import pytest
import pandas as pd
from unittest.mock import patch
from src.commercecraft_utils.translation_engine import TranslationEngine

@pytest.fixture
def translation_engine():
    with patch.dict(os.environ, {
        'SET_SEPARATOR': ';',
        'OUTPUT_SUFFIX': '_translated',
        'FIELD_LANGUAGE_SEPARATOR': '.',
        'LANGUAGE_SEPARATOR': '-'
    }):
        return TranslationEngine()

@pytest.mark.asyncio
async def test_translate_values(translation_engine):
    """Test translation of individual values."""
    values = ['Whitestone', 'Greyskull Keep', "Scanlan's Hand"]
    translations = await translation_engine.translate_values(
        values, 'en-US', 'fr-FR'
    )
    
    print(f'\nInput: \n{values} \nTranslated: \n{translations}')
    assert len(translations) == len(values)

@pytest.mark.asyncio
async def test_translate_empty_values(translation_engine):
    """Test handling of empty values."""
    values = ['', None, 'Vestiges of Divergence']
    translations = await translation_engine.translate_values(
        values, 'en-US', 'fr-FR'
    )
    
    print(f'\nInput: \n{values} \nTranslated: \n{translations}')
    assert translations[0] == ''
    assert translations[1] is None
    assert isinstance(translations[2], str)

@pytest.mark.asyncio
async def test_translate_dataframe(translation_engine):
    """Test translation of a complete dataframe."""
    data = {
        'name.en-US': ['elements', 'mysteries', 'adventures'],
        'name.fr-FR': ['', '', ''],
        'prices': ['US-USD 8800', 'FR-EUR 4500', ''],
        'description.en-US': ['guardians', 'heroes', 'villains'],
        'description.fr-FR': ['', '', ''],
        'location.en-US': ['role', 'power', 'path'],
        'location.fr-FR': ['', '', '']
    }
    df = pd.DataFrame(data)
    translated_df = await translation_engine.translate_dataframe(df)
    
    print(f'\nInput: \n{df} \nTranslated: \n{translated_df}')
    translated_df.to_csv('test.csv', index=False)
    assert not translated_df['name.fr-FR'].isna().all()
    assert not translated_df['description.fr-FR'].isna().all()
    assert not translated_df['location.fr-FR'].isna().all()

@pytest.mark.asyncio
async def test_set_translation(translation_engine):
    """Test translation of set fields."""
    data = {
        'items.en-US': ['Whisper;Mythcarver;Fenthras', 'Cabal\'s Ruin;Deathwalker\'s Ward', 'Plate of the Dawnmartyr'],
        'items.fr-FR': ['', '', ''],
        'spells.en-US': ['Divine Gate;Banishment;True Resurrection', 'Hex;Eldritch Blast', 'Healing Word'],
        'spells.fr-FR': ['', '', '']
    }
    df = pd.DataFrame(data)
    set_columns = ['items', 'spells']
    translated_df = await translation_engine.translate_dataframe(df, set_columns)
    
    print(f'\nInput: \n{df} \nTranslated: \n{translated_df}')
    translated_df.to_csv('test_sets.csv', index=False)
    assert not translated_df['items.fr-FR'].isna().all()
    assert not translated_df['spells.fr-FR'].isna().all()

@pytest.mark.asyncio
async def test_file_processing(translation_engine, tmp_path):
    """Test processing of CSV files."""
    input_file = tmp_path / "vox_machina_inventory.csv"
    data = {
        'item.en-US': ['Bag of Holding', 'Deck of Many Things'],
        'item.fr-FR': ['', ''],
        'owner.en-US': ['Vex\'ahlia', 'Percy de Rolo'],
        'owner.fr-FR': ['', '']
    }
    df = pd.DataFrame(data)
    df.to_csv(input_file, index=False)
    
    await translation_engine.process_file(str(input_file))
    output_file = tmp_path / "vox_machina_inventory_translated.csv"
    assert output_file.exists()

@pytest.mark.asyncio
async def test_mixed_content_translation(translation_engine):
    """Test translation of mixed content types."""
    data = {
        'character.en-US': ['Pike Trickfoot', 'Scanlan Shorthalt', 'Grog Strongjaw'],
        'character.fr-FR': ['', '', ''],
        'abilities.en-US': ['Divine Magic;Healing', 'Bardic Inspiration;Cutting Words', 'Rage;Great Weapon Master'],
        'abilities.fr-FR': ['', '', ''],
        'status.en-US': ['Gnome Cleric', 'Gnome Bard', 'Goliath Barbarian'],
        'status.fr-FR': ['', '', '']
    }
    df = pd.DataFrame(data)
    set_columns = ['abilities']
    translated_df = await translation_engine.translate_dataframe(df, set_columns)

    print(f'\nInput: \n{df} \nTranslated: \n{translated_df}')    
    assert not translated_df['character.fr-FR'].isna().all()
    assert not translated_df['abilities.fr-FR'].isna().all()
    assert not translated_df['status.fr-FR'].isna().all()