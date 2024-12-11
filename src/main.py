import asyncio
from commercecraft_utils import TranslationEngine

PROD_PATH = 'data/products.csv'
CAT_PATH = 'data/categories.csv'


async def main():
    translator = TranslationEngine()

    # Process categories
    # print("Processing categories...")
    # await translator.process_file(CAT_PATH, exclude_columns=['slug'])

    # Process products
    print("Processing products...")
    await translator.process_file(PROD_PATH, set_columns=['benefits', ],exclude_columns=['slug', 'recomendations'])


if __name__ == "__main__":
    asyncio.run(main())
