"""Entry point for the CryptoPanic news scraper.

Usage
-----
Default (incremental, skip cached articles)::

    python main.py

Force re-fetch every article on the page::

    python main.py --force-update
"""

import argparse
import asyncio

from news_scraper import NewsArticleScraper


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments.

    Returns
    -------
    argparse.Namespace
        Parsed arguments with ``force_update`` boolean flag.
    """
    parser = argparse.ArgumentParser(
        description="Scrape market news from CryptoPanic.",
    )
    parser.add_argument(
        "--force-update",
        action="store_true",
        default=False,
        help="Re-fetch all articles regardless of cache state.",
    )
    parser.add_argument(
        "--max-concurrency",
        type=int,
        default=50,
        help="Max concurrent CDP tasks for fallback parsing (default: 50).",
    )
    return parser.parse_args()


async def main() -> None:
    """Instantiate the scraper and run the pipeline."""
    args = parse_args()

    scraper = NewsArticleScraper(
        cache_path="news_data/cached_news_data.json",
        unique_urls_path="news_data/cached_unique_urls.json",
        headless=False,
        max_retries=5,
        scroll_pause=2.0,
        force_update=args.force_update,
        max_concurrency=args.max_concurrency,
    )

    await scraper.run()

async def open_all_redirect_urls():
    """Open all redirect URLs stored in cached_all_redirect_urls.json."""
    scraper = NewsArticleScraper(
        cache_path="news_data/cached_news_data.json",
        unique_urls_path="news_data/cached_unique_urls.json",
        headless=False,
        max_retries=5,
        scroll_pause=2.0,
        force_update=False,
        max_concurrency=50,
    )

    await scraper.open_all_redirect_urls()

if __name__ == "__main__":
    asyncio.run(main())
    asyncio.run(open_all_redirect_urls())