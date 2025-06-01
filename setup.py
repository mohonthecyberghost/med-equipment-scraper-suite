from setuptools import setup, find_packages

setup(
    name="med-equipment-scraper-suite",
    version="1.0.0",
    packages=find_packages(),
    install_requires=[
        'playwright==1.40.0',
        'python-dotenv==1.0.0',
        'mysql-connector-python==8.2.0',
        'pandas==2.1.3',
        'beautifulsoup4==4.12.2',
        'requests==2.31.0',
        'aiohttp==3.9.1',
        'python-slugify==8.0.1',
        'tqdm==4.66.1',
        'pytest==7.4.3',
        'pytest-asyncio==0.21.1',
        'pytest-playwright==0.4.3'
    ],
    python_requires='>=3.8',
) 