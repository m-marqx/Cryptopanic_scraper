# CryptoPanicScraper

## Overview

CryptoPanicScraper is an asynchronous web scraper that gathers cryptocurrency-related news from the CryptoPanic website, analyzes sentiment using the NLTK VADER sentiment analysis tool, and caches the data for reuse. The scraper allows users to specify a topic, set a filter, limit the number of articles, and customize other parameters for news collection.

## Features

- **Topic Search**: Search for news related to specific cryptocurrencies or topics
- **Sentiment Analysis**: Perform sentiment analysis using the VADER model, enhanced with custom financial terms
- **Caching**: Caches previously scraped articles to avoid duplicating data in subsequent runs
- **Pagination Handling**: Automatically scrolls or clicks the "Load More" button to fetch additional articles
- **Data Storage**: Saves scraped news data locally in a pickle file for persistence

## Requirements

- `python` (>=3.8)
- `asyncio`
- `nltk`
- `playwright`
- `pathlib`

## Setup Instructions

Follow these steps in order to set up and run the scraper:

### 1. Install Python Dependencies

```bash
pip install -r requirements.txt
```

### 2. Install Playwright Browsers

After installing the Python packages, you need to install the Playwright browser binaries:

```bash
python -m playwright install
```

### 3. Download VADER Lexicon

The sentiment analysis requires the VADER lexicon from NLTK. Open a Python interactive shell and run:

```python
>>> import nltk
>>> nltk.download('vader_lexicon')
```

Alternatively, you can run this one-liner from the command line:

```bash
python -c "import nltk; nltk.download('vader_lexicon')"
```

### 4. Run the Scraper

Once all dependencies are installed, you can run the scraper:

```bash
python main.py
```

## Usage

### Parameters

- **filter**: Type of news to filter (e.g., "all", "bullish", "bearish")
- **limit**: Maximum number of articles to scrape (default: 10)
- **topic**: Specific cryptocurrency or topic to search for (e.g., "BTC", "ETH")
- **save_path**: Directory to save cached data (default: 'news_data')

### Basic Usage

Edit `main.py` to configure the scraper parameters:

```python
import asyncio
from api_news_scraper import CryptoPanicScraper

async def main():
    scraper = CryptoPanicScraper()
    scraper.filter = "all"  # Options: all, bullish, bearish, important, etc.
    scraper.limit = 50      # Number of articles to scrape
    scraper.topic = "BTC"   # Cryptocurrency topic to search for
    await scraper.run()

if __name__ == "__main__":
    asyncio.run(main())
```
## Methods
- **_update_vader_lexicon(self): Updates the VADER sentiment lexicon with custom financial terms.
- **load_cached_data(self): Loads cached articles from a pickle file if available.
- **async run(self): Initiates the scraper, navigates to the CryptoPanic page, and collects data.
- **async search_topic(self, page, topic): Searches for a specific topic using CryptoPanic's search bar.
- **async collect_data(self, page): Collects news articles from the page, including sentiment analysis.
- **async get_news_sentiment(self, symbol: str): Fetches sentiment data for a specific cryptocurrency symbol.
- **async load_more(self, page): Scrolls or clicks the "Load More" button to load more content on the page.
- **async retry_fetch_text(self, element, selector, default_value, retries=2): Retries fetching text content with error handling.
- **async retry_fetch_attribute(self, element, selector, attribute, default_value, retries=2): Retries fetching an attribute (e.g., href) with error handling.
- **async get_currencies(self, element): Extracts the mentioned cryptocurrencies from the article.
- **async get_votes(self, element): Extracts vote data such as bullish, bearish, and other vote counts.
- **analyze_sentiment_with_vader(self, title): Performs sentiment analysis on an article title using VADER.
- **save_data(self): Saves the scraped data to a file, merging new data with cached data.

### Example Usage

You can also use the scraper programmatically to fetch sentiment data:

```python
from api_news_scraper import CryptoPanicScraper

async def get_btc_sentiment():
    scraper = CryptoPanicScraper()
    data = await scraper.get_news_sentiment("BTC")
    return data
```

## Output

The scraper saves data in two formats:

1. **Pickle Cache**: `news_data/cryptopanic_{filter}_cache.pickle` - Contains all scraped articles (new + cached)
2. **Console Logs**: Real-time logging of scraping progress and sentiment analysis

Each article includes:
- Date and time
- Title
- Associated cryptocurrencies
- Vote counts (bullish, bearish, etc.)
- Source name and URL
- Sentiment classification (very bullish, bullish, neutral, bearish, very bearish)
- Confidence score (0-100%)

## Sentiment Analysis

The scraper uses VADER (Valence Aware Dictionary and sEntiment Reasoner) enhanced with custom financial terms for more accurate cryptocurrency news sentiment analysis. Sentiment is classified into five categories:

- **Very Bullish**: Compound score ≥ 0.6
- **Bullish**: 0.1 ≤ Compound score < 0.6
- **Neutral**: -0.1 < Compound score < 0.1
- **Bearish**: -0.6 < Compound score ≤ -0.1
- **Very Bearish**: Compound score ≤ -0.6

## Troubleshooting

### Permission Errors

On Windows, you may need to run the terminal as Administrator for some installation steps.