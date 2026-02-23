"""CryptoPanic news scraper module.

Uses zendriver to bypass Cloudflare protection and scrape market news
from CryptoPanic's infinite-scroll page. Caches results incrementally
to JSON files and builds a unique-source URL index.
"""

import asyncio
import json
import os
import re
import time
from typing import TypedDict
import numpy as np
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import JSONB

import requests
import zendriver as zd
from tqdm import tqdm
from zendriver.core.tab import Tab

from logging_config import logger


class ArticleData(TypedDict):
    """Schema for a scraped news article.

    Attributes
    ----------
    title : str
        The article headline.
    url : str
        CryptoPanic relative URL path
        (e.g. ``/news/29538595/Dogecoin-Whale-Alert-...``).
    redirect_url : str
        CryptoPanic click-through redirect URL
        (e.g. ``/news/click/29538595/``).
    source : str
        Source domain name (e.g. ``decrypt.co``).
    source_type : str
        One of ``"link"``, ``"twitter"``, or ``"youtube"``.
    date : str
        ISO datetime string from the article's ``<time>`` element.
    currencies : list[str]
        Ticker symbols associated with the article.
    votes : dict[str, int]
        Vote counts keyed by vote type (e.g. ``{"Bullish": 5}``).
    """

    title: str
    url: str
    redirect_url: str
    source: str
    source_type: str
    date: str
    currencies: list[str]
    votes: dict[str, int]


class NewsArticleScraper:
    """Scrape market news articles from CryptoPanic.

    Uses zendriver (undetected Chrome) to navigate CryptoPanic's
    Cloudflare-protected, infinite-scroll news page.  Extracts article
    metadata and caches results to JSON for incremental scraping.

    Parameters
    ----------
    cache_path : str, optional
        Path to the JSON cache file, by default ``"cached_news_data.json"``.
    unique_urls_path : str, optional
        Path to the unique-source URL index file, by default
        ``"cached_unique_urls.json"``.
    headless : bool, optional
        Whether to run the browser in headless mode, by default ``False``.
    max_retries : int, optional
        Maximum consecutive stale-scroll attempts before stopping,
        by default ``5``.
    scroll_pause : float, optional
        Seconds to wait between scroll attempts, by default ``2.0``.
    force_update : bool, optional
        If ``True``, re-fetch articles already present in the cache,
        by default ``False``.
    verify_template_path : str or None, optional
        Path to a custom Cloudflare verification template,
        by default ``None`` (use zendriver's built-in template).
    max_concurrency : int or None, optional
        Maximum number of concurrent browser tabs, by default ``None``

    Methods
    -------
    run() -> None
        Execute the full scraping pipeline: launch browser, bypass
        Cloudflare, scroll to load all articles, extract metadata, and
        save results to disk.
    open_all_redirect_urls(redirect_urls: list[str]) -> None
        Open each redirect URL to resolve its final destination, with
        incremental saving and progress tracking.

    Examples
    --------
    >>> import asyncio
    >>> scraper = CryptoPanicScraper(force_update=False)
    >>> asyncio.run(scraper.run())
    """

    _BASE_URL = "https://cryptopanic.com/"
    _CF_MAX_ATTEMPTS = 5
    _INCREMENTAL_SAVE_INTERVAL = 50
    _DEFAULT_MAX_CONCURRENCY = 50
    _CACHED_REDIRECT_PATH = "news_data/filled_sources/redirected_sources.json"
    _CACHED_SOURCES_PATH = "news_data/filled_sources/cached_sources.json"
    _FORMATTED_SOURCES_PATH = "news_data/filled_sources/formatted_sources.json"
    _CONTENT_CACHE_PATH = "news_data/cached_news_content.json"
    _JINA_BASE_URL = "https://r.jina.ai/"
    _JINA_RATE_LIMIT_AUTH = 0.12   # seconds between requests (with API key)
    _JINA_RATE_LIMIT_FREE = 3.0    # seconds between requests (without API key)
    _REDIRECT_SAVE_INTERVAL = 100

    # ------------------------------------------------------------------
    #  JavaScript: extract ALL articles from the DOM in a single call.
    #  This replaces thousands of sequential CDP round-trips with ONE.
    # ------------------------------------------------------------------
    _EXTRACT_ALL_JS = r"""
    (() => {
        const rows = document.querySelectorAll('div.news-row.news-row-link');
        return Array.from(rows).map(el => {
            const titleEl  = el.querySelector('span.title-text span');
            const linkEl   = el.querySelector('a.news-cell.nc-title');
            const timeEl   = el.querySelector('time');
            const sourceEl = el.querySelector('span.si-source-domain');

            const title = titleEl ? titleEl.textContent.trim() : null;
            const url   = linkEl  ? linkEl.getAttribute('href') : null;
            const date  = timeEl
                ? (timeEl.getAttribute('datetime') || timeEl.textContent.trim())
                : '';
            const source = sourceEl ? sourceEl.textContent.trim() : 'unknown';

            let sourceType = 'link';
            if (el.querySelector('span.open-link-icon.icon.icon-twitter'))
                sourceType = 'twitter';
            else if (el.querySelector('span.open-link-icon.icon.icon-youtube-play'))
                sourceType = 'youtube';

            const currEls = el.querySelectorAll('a.colored-link');
            const currencies = currEls.length
                ? Array.from(currEls).map(n => n.textContent.trim()).filter(Boolean)
                : ['N/A'];

            const votes = {};
            el.querySelectorAll('span.nc-vote-cont').forEach(node => {
                const t = (node.getAttribute('title') || '').trim();
                const m = t.match(/^\s*(\d+)\s+(.+?)\s+votes?\s*$/);
                if (m) votes[m[2]] = parseInt(m[1], 10);
            });

            let redirectUrl = url;
            if (url) {
                const rm = url.match(/\/news\/(\d+)\//);
                if (rm) redirectUrl = '/news/click/' + rm[1] + '/';
            }

            return {
                title, url, redirect_url: redirectUrl,
                date, source, source_type: sourceType,
                currencies, votes
            };
        });
    })()
    """

    def __init__(
        self,
        cache_path: str = "cached_news_data.json",
        unique_urls_path: str = "cached_unique_urls.json",
        headless: bool = False,
        max_retries: int = 5,
        scroll_pause: float = 2.0,
        force_update: bool = False,
        verify_template_path=None,
        max_concurrency: int | None = None,
    ) -> None:
        self.cache_path = cache_path
        self.unique_urls_path = unique_urls_path
        self.headless = headless
        self.max_retries = max_retries
        self.scroll_pause = scroll_pause
        self.force_update = force_update
        self.template_path = verify_template_path
        self.max_concurrency = max_concurrency or self._DEFAULT_MAX_CONCURRENCY

        self.cache: dict[str, ArticleData] = self._load_cache()
        self._new_since_last_save: int = 0

    def _load_cache(self) -> dict[str, ArticleData]:
        """Load cached article data from disk.

        Returns
        -------
        dict[str, ArticleData]
            Cached articles keyed by CryptoPanic URL path.
            Returns an empty dict if the cache file does not exist
            or cannot be parsed.
        """
        if not os.path.exists(self.cache_path):
            logger.info(
                "No cache file found at '%s'. Starting fresh.", self.cache_path
            )
            return {}

        try:
            with open(self.cache_path, "r", encoding="utf-8") as fh:
                data: dict[str, ArticleData] = json.load(fh)
            logger.info(
                "Loaded %d cached articles from '%s'.",
                len(data),
                self.cache_path,
            )
            return data
        except (json.JSONDecodeError, OSError) as exc:
            logger.error(
                "Failed to load cache from '%s': %s", self.cache_path, exc
            )
            return {}

    def _save_cache(self) -> None:
        """Persist the article cache to disk as JSON."""
        try:
            with open(self.cache_path, "w", encoding="utf-8") as fh:
                json.dump(self.cache, fh, ensure_ascii=False, indent=2)
            logger.info(
                "Saved %d articles to '%s'.", len(self.cache), self.cache_path
            )
        except OSError as exc:
            logger.error(
                "Failed to save cache to '%s': %s", self.cache_path, exc
            )

    def _save_unique_urls(self) -> None:
        """Build and save a mapping of unique source domains to sample URLs.

        Iterates the cache and keeps the first URL encountered for each
        unique source domain.  Writes to ``self.unique_urls_path``.
        """
        urls = pd.read_json(self.cache_path).T.query(
            "source_type == 'link'"
        ).drop_duplicates("source").set_index("source")["url"].to_dict()

        try:
            with open(self.unique_urls_path, 'w', encoding="utf-8") as f:
                json.dump(urls, f, indent=2, ensure_ascii=False)
            logger.info(
                "Saved %d unique source URLs to '%s'.",
                len(urls),
                self.unique_urls_path,
            )
        except OSError as exc:
            logger.error(
                "Failed to save unique URLs to '%s': %s",
                self.unique_urls_path,
                exc,
            )

    def _conditional_incremental_save(self) -> None:
        """Save the cache if enough new articles have accumulated.

        Triggers a write every ``_INCREMENTAL_SAVE_INTERVAL`` new articles
        to avoid data loss during long scroll sessions.
        """
        if self._new_since_last_save >= self._INCREMENTAL_SAVE_INTERVAL:
            self._save_cache()
            self._new_since_last_save = 0

    def _load_sources_config(self) -> dict:
        """Load formatted source selectors from disk.

        Returns
        -------
        dict
            Source config keyed by domain.
        """
        if not os.path.exists(self._FORMATTED_SOURCES_PATH):
            logger.warning(
                "Formatted sources not found at '%s'.",
                self._FORMATTED_SOURCES_PATH,
            )
            return {}

        try:
            with open(self._FORMATTED_SOURCES_PATH, "r", encoding="utf-8") as fh:
                data = json.load(fh)
            logger.info(
                "Loaded %d source configs from '%s'.",
                len(data),
                self._FORMATTED_SOURCES_PATH,
            )
            return data
        except (json.JSONDecodeError, OSError) as exc:
            logger.error(
                "Failed to load sources config: %s", exc
            )
            return {}
