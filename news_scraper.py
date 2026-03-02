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
    _CACHED_REDIRECT_PATH = (
        "news_data/filled_sources/redirected_sources.parquet"
    )
    _CACHED_SOURCES_PATH = "news_data/filled_sources/cached_sources.json"
    _FORMATTED_SOURCES_PATH = "news_data/filled_sources/formatted_sources.json"
    _CONTENT_CACHE_PATH = "news_data/cached_news_content.parquet"
    _JINA_BASE_URL = "https://r.jina.ai/"
    _DATABASE_URL = os.getenv("DATABASE_URL")
    _JINA_RATE_LIMIT_AUTH = 0.12  # seconds between requests (with API key)
    _JINA_RATE_LIMIT_FREE = 3.0  # seconds between requests (without API key)
    _REDIRECT_SAVE_INTERVAL = 10

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
        max_concurrency: int | None = None,
    ) -> None:
        self.cache_path = cache_path
        self.unique_urls_path = unique_urls_path
        self.headless = headless
        self.max_retries = max_retries
        self.scroll_pause = scroll_pause
        self.force_update = force_update
        self.max_concurrency = max_concurrency or self._DEFAULT_MAX_CONCURRENCY

        self.cache: dict[str, ArticleData] = self._load_cache()
        self._new_since_last_save: int = 0
        self._sources_config: dict = self._load_sources_config()
        self._engine = create_engine(self._DATABASE_URL)

        self._jina_api_key: str | None = os.getenv("JINA_API_KEY")
        self._jina_rate_limit: float = (
            self._JINA_RATE_LIMIT_AUTH
            if self._jina_api_key
            else self._JINA_RATE_LIMIT_FREE
        )

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
        urls = (
            pd.read_json(self.cache_path)
            .T.query("source_type == 'link'")
            .drop_duplicates("source")
            .set_index("source")["url"]
            .to_dict()
        )

        try:
            with open(self.unique_urls_path, "w", encoding="utf-8") as f:
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

    async def run(self) -> None:
        """Execute the full scraping pipeline.

        Launches the browser, navigates to CryptoPanic, bypasses
        Cloudflare, scrolls to collect all available articles, and
        saves the results to disk.

        Raises
        ------
        RuntimeError
            If Cloudflare bypass fails after maximum attempts.
        """
        browser = await zd.start(headless=self.headless, sandbox=False)
        try:
            page = await browser.get(self._BASE_URL)
            logger.info("Navigating to %s", self._BASE_URL)

            await self._bypass_cloudflare(page)
            await self._scroll_and_collect(page)
        finally:
            await browser.stop()
            self._save_cache()
            self._save_unique_urls()

    async def _bypass_cloudflare(self, page: Tab) -> None:
        """Wait for and solve the Cloudflare challenge.

        Parameters
        ----------
        page : Tab
            The zendriver page/tab to verify.

        Raises
        ------
        RuntimeError
            If the challenge cannot be bypassed after
            ``_CF_MAX_ATTEMPTS`` attempts.
        """
        for attempt in range(1, self._CF_MAX_ATTEMPTS + 1):
            try:
                await page.wait_for(selector=".ray-id", timeout=1)
                cf_marker = await page.query_selector(".ray-id")
                logger.info(
                    "Cloudflare challenge detected (attempt %d/%d).",
                    attempt,
                    self._CF_MAX_ATTEMPTS
                )
            except Exception:
                cf_marker = None

            if not cf_marker:
                # logger.info("No Cloudflare challenge detected — proceeding.")
                return

            logger.warning(
                "Cloudflare challenge detected. (attempt %d/%d).",
                attempt,
                self._CF_MAX_ATTEMPTS
            )
            await page.sleep(5)
            await page.verify_cf()

        if cf_marker:
            await page.save_screenshot("cf_challenge_failure.png")
            raise RuntimeError(
                "Failed to bypass Cloudflare after "
                f"{self._CF_MAX_ATTEMPTS} attempts.  "
                "Screenshot saved to cf_challenge_failure.png."
            )

        logger.info("Cloudflare challenge cleared.")

    async def _scroll_and_collect(self, page: Tab) -> None:
        """Scroll to the absolute bottom and collect every article.

        Continuously scrolls, clicks "Load more" when available, and
        stops only when no new content appears after ``max_retries``
        consecutive attempts or the ``all-loaded`` marker is found.

        Parameters
        ----------
        page : Tab
            The zendriver page/tab to scroll and scrape.
        """
        stale_count = 0
        previous_count = 0

        while True:
            # Scroll to bottom
            await page.evaluate(
                "window.scrollTo(0, document.body.scrollHeight)"
            )
            await page.sleep(self.scroll_pause)

            # Click "Load more" button if present
            await self._click_load_more(page)

            # Count articles currently in the DOM
            articles = await page.query_selector_all(
                "div.news-row.news-row-link"
            )
            current_count = len(articles)
            logger.info("Articles in DOM: %d", current_count)

            if current_count == previous_count:
                stale_count += 1
                logger.info(
                    "No new articles loaded (%d/%d stale attempts).",
                    stale_count,
                    self.max_retries,
                )
                if stale_count >= self.max_retries:
                    logger.info("Reached max stale retries — stopping scroll.")
                    break
            else:
                stale_count = 0

            previous_count = current_count

            # Check for CryptoPanic's "all-loaded" DOM marker
            if await self._is_all_loaded(page):
                logger.info("All articles loaded (all-loaded marker found).")
                break

        # ----------------------------------------------------------
        #  Extract article data — prefer bulk JS (1 CDP call) over
        #  per-element parsing (thousands of CDP calls).
        # ----------------------------------------------------------
        new_count = await self._bulk_extract_articles(page)

        if new_count == -1:
            # JS extraction failed — fall back to concurrent parsing
            logger.warning(
                "Bulk JS extraction failed. "
                "Falling back to concurrent per-element parsing."
            )
            new_count = await self._parse_articles_concurrent(page)

        logger.info(
            "Finished — %d new articles collected (%d total in cache).",
            new_count,
            len(self.cache),
        )

    async def _bulk_extract_articles(self, page: Tab) -> int:
        """Extract every article from the DOM via a single JS call.

        Executes ``_EXTRACT_ALL_JS`` inside the browser, which returns
        a list of plain objects with all the article fields already
        extracted.  This replaces thousands of sequential CDP
        round-trips with exactly **one**.

        Parameters
        ----------
        page : Tab
            The zendriver page/tab.

        Returns
        -------
        int
            Number of *new* articles stored, or ``-1`` if the JS
            extraction failed (caller should fall back to per-element
            parsing).
        """
        try:
            raw: list[dict] = await page.evaluate(self._EXTRACT_ALL_JS)
        except Exception as exc:
            logger.error("JS bulk extraction raised: %s", exc)
            return -1

        if not isinstance(raw, list):
            logger.error(
                "JS bulk extraction returned %s instead of list.",
                type(raw).__name__,
            )
            return -1

        total = len(raw)
        logger.info(
            "Bulk JS extraction returned %d articles — processing.", total
        )

        new_count = 0
        for idx, item in enumerate(raw, start=1):
            title = item.get("title")
            url = item.get("url")
            if not title or not url:
                logger.debug(
                    "[%d/%d] Skipped — missing title or URL.", idx, total
                )
                continue

            if url in self.cache and not self.force_update:
                logger.debug(
                    "[%d/%d] Already cached: %s",
                    idx,
                    total,
                    title[:60],
                )
                continue

            article = ArticleData(
                title=title,
                url=url,
                redirect_url=item.get("redirect_url", url),
                date=item.get("date", ""),
                source=item.get("source", "unknown"),
                source_type=item.get("source_type", "link"),
                currencies=item.get("currencies", ["N/A"]),
                votes=item.get("votes", {}),
            )

            self.cache[url] = article
            self._new_since_last_save += 1
            new_count += 1

            logger.info(
                "Scraped [%d/%d]: %s | %s | %s | %s",
                idx,
                total,
                title[:80],
                article["source"],
                article["source_type"],
                article["date"] or "no date",
            )

            self._conditional_incremental_save()

        return new_count

    async def _parse_articles_concurrent(self, page: Tab) -> int:
        """Parse all article elements concurrently with a TaskGroup.

        Uses ``asyncio.TaskGroup`` (Python 3.11+) with a
        ``asyncio.Semaphore`` to cap the number of in-flight CDP
        requests at ``self.max_concurrency``.

        Parameters
        ----------
        page : Tab
            The zendriver page/tab.

        Returns
        -------
        int
            Number of new articles stored.
        """
        articles = await page.query_selector_all("div.news-row.news-row-link")
        total = len(articles)
        logger.info(
            "Concurrent parsing: %d articles, concurrency=%d.",
            total,
            self.max_concurrency,
        )

        sem = asyncio.Semaphore(self.max_concurrency)
        results: list[bool] = []

        async def _limited_parse(el: Tab, idx: int) -> bool:
            async with sem:
                return await self._parse_and_store(el, idx, total)

        async with asyncio.TaskGroup() as tg:
            tasks = [
                tg.create_task(_limited_parse(el, idx))
                for idx, el in enumerate(articles, start=1)
            ]

        results = [t.result() for t in tasks]
        return sum(results)

    async def _click_load_more(self, page: Tab) -> None:
        """Click the "Load more" button if it exists on the page.

        Parameters
        ----------
        page : Tab
            The current page/tab.
        """
        try:
            button = await page.find("Load more", timeout=1)
            if button:
                await button.click()
                await page.sleep(self.scroll_pause)
        except Exception:
            pass

    async def _is_all_loaded(self, page: Tab) -> bool:
        """Check whether the ``all-loaded`` DOM marker is present.

        Parameters
        ----------
        page : Tab
            The current page/tab.

        Returns
        -------
        bool
            ``True`` if the marker element exists, ``False`` otherwise.
        """
        try:
            marker = await page.select("all-loaded", timeout=1)
            return marker is not None
        except Exception:
            return False

    async def _parse_and_store(
        self, element: Tab, index: int, total: int
    ) -> bool:
        """Parse a single article and store it in the cache immediately.

        Designed to run as a concurrent task inside an
        ``asyncio.TaskGroup``.  Each task independently parses its
        element and writes the result to the shared cache, triggering
        an incremental save when the threshold is reached.

        Parameters
        ----------
        element : Tab
            A zendriver element handle for a
            ``div.news-row.news-row-link`` node.
        index : int
            1-based position of this article in the DOM (for logging).
        total : int
            Total number of articles being processed (for logging).

        Returns
        -------
        bool
            ``True`` if a new article was stored, ``False`` otherwise.
        """
        try:
            result = await self._parse_article(element)
            if result is None:
                logger.debug(
                    "[%d/%d] Skipped — could not parse title or URL.",
                    index,
                    total,
                )
                return False

            url = result["url"]
            if url in self.cache and not self.force_update:
                logger.debug(
                    "[%d/%d] Already cached: %s",
                    index,
                    total,
                    result["title"][:60],
                )
                return False

            self.cache[url] = result
            self._new_since_last_save += 1

            logger.info(
                "Scraped [%d/%d]: %s | %s | %s | %s",
                index,
                total,
                result["title"][:80],
                result["source"],
                result["source_type"],
                result["date"] or "no date",
            )

            self._conditional_incremental_save()
            return True
        except Exception as exc:
            logger.error("Error parsing article %d: %s", index, exc)
            return False

    async def _parse_article(self, element: Tab) -> ArticleData | None:
        """Extract article metadata from a news-row DOM element.

        Parameters
        ----------
        element : Tab
            A zendriver element handle for a
            ``div.news-row.news-row-link`` node.

        Returns
        -------
        ArticleData or None
            Parsed article data, or ``None`` if the title or URL
            could not be extracted.
        """
        title = await self._get_text(element, "span.title-text span")
        if not title:
            return None

        url = await self._get_attribute(
            element, "a.news-cell.nc-title", "href"
        )
        if not url:
            return None

        date = (
            await self._get_attribute(element, "time", "datetime")
            or await self._get_text(element, "time")
            or ""
        )

        return ArticleData(
            title=title,
            url=url,
            redirect_url=self._build_redirect_url(url),
            date=date,
            source=await self._get_text(element, "span.si-source-domain")
            or "unknown",
            source_type=await self._detect_source_type(element),
            currencies=await self._get_currencies(element),
            votes=await self._get_votes(element),
        )

    @staticmethod
    def _build_redirect_url(url: str) -> str:
        """Convert an article URL to its click-through redirect URL.

        Parameters
        ----------
        url : str
            Article URL path (e.g. ``/news/29538595/Dogecoin-...``).

        Returns
        -------
        str
            Redirect URL (e.g. ``/news/click/29538595/``), or the
            original URL if the numeric ID cannot be extracted.
        """
        match = re.search(r"/news/(\d+)/", url)
        if match:
            return f"/news/click/{match.group(1)}/"
        return url

    @staticmethod
    async def _get_text(element: Tab, selector: str) -> str | None:
        """Safely extract text content from a child element.

        Parameters
        ----------
        element : Tab
            Parent element to query within.
        selector : str
            CSS selector for the target child element.

        Returns
        -------
        str or None
            Stripped text content, or ``None`` if not found.
        """
        try:
            node = await element.query_selector(selector)
            if node and node.text:
                return node.text.strip()
        except Exception:
            pass
        return None

    @staticmethod
    async def _get_attribute(
        element: Tab, selector: str, attribute: str
    ) -> str | None:
        """Safely extract an attribute value from a child element.

        Parameters
        ----------
        element : Tab
            Parent element to query within.
        selector : str
            CSS selector for the target child element.
        attribute : str
            Name of the attribute to read.

        Returns
        -------
        str or None
            Stripped attribute value, or ``None`` if not found.
        """
        try:
            node = await element.query_selector(selector)
            if node:
                value = node.attrs.get(attribute)
                if value:
                    return value.strip()
        except Exception:
            pass
        return None

    @staticmethod
    async def _detect_source_type(element: Tab) -> str:
        """Determine the article's source type from icon CSS classes.

        Parameters
        ----------
        element : Tab
            Article row element to inspect.

        Returns
        -------
        str
            One of ``"twitter"``, ``"youtube"``, or ``"link"``.
        """
        try:
            if await element.query_selector(
                "span.open-link-icon.icon.icon-twitter"
            ):
                return "twitter"
            if await element.query_selector(
                "span.open-link-icon.icon.icon-youtube-play"
            ):
                return "youtube"
        except Exception:
            pass
        return "link"

    @staticmethod
    async def _get_currencies(element: Tab) -> list[str]:
        """Extract currency ticker symbols from an article element.

        Parameters
        ----------
        element : Tab
            Article row element to inspect.

        Returns
        -------
        list[str]
            Ticker symbols (e.g. ``["BTC", "ETH"]``), or ``["N/A"]``
            if none are found.
        """
        try:
            nodes = await element.query_selector_all("a.colored-link")
            tickers = [n.text.strip() for n in nodes if n.text]
            if tickers:
                return tickers
        except Exception:
            pass
        return ["N/A"]

    @staticmethod
    async def _get_votes(element: Tab) -> dict[str, int]:
        """Extract vote counts from an article element.

        Parses the ``title`` attribute of each ``span.nc-vote-cont``
        child, which follows the pattern ``"<count> <type> votes"``
        (e.g. ``"5 Bullish votes"``).

        Parameters
        ----------
        element : Tab
            Article row element to inspect.

        Returns
        -------
        dict[str, int]
            Vote counts keyed by type (e.g. ``{"Bullish": 5}``).
            Returns an empty dict if no votes are found.
        """
        votes: dict[str, int] = {}
        try:
            nodes = await element.query_selector_all("span.nc-vote-cont")
            for node in nodes:
                title_attr = node.attrs.get("title")
                if not title_attr:
                    continue
                match = re.match(
                    r"\s*(\d+)\s+(.+?)\s+votes?\s*$",
                    title_attr.strip(),
                )
                if match:
                    votes[match.group(2)] = int(match.group(1))
        except Exception:
            pass
        return votes

    @staticmethod
    def _remove_site_prefixes(url_series: pd.Series) -> pd.Series:
        prefixes: list[str] = [
            "www.",
            "en.",
            "news.",
            "feeds2.",
            "daily.",
            "square.",
        ]
        pattern: str = r"^(" + "|".join(map(re.escape, prefixes)) + r")"
        return url_series.str.replace(pattern, "", regex=True)

    @staticmethod
    def _strip_single_prefix(domain: str) -> str:
        """Strip common subdomain prefixes from a single domain string."""
        prefixes = ["www.", "en.", "news.", "feeds2.", "daily.", "square."]
        for prefix in prefixes:
            if domain.startswith(prefix):
                domain = domain[len(prefix) :]
        return domain

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
            with open(
                self._FORMATTED_SOURCES_PATH, "r", encoding="utf-8"
            ) as fh:
                data = json.load(fh)
            logger.info(
                "Loaded %d source configs from '%s'.",
                len(data),
                self._FORMATTED_SOURCES_PATH,
            )
            return data
        except (json.JSONDecodeError, OSError) as exc:
            logger.error("Failed to load sources config: %s", exc)
            return {}

    def _find_source_config(self, source: str) -> dict | None:
        """Find the source config for a given domain.

        Matches by stripping common prefixes from both the source
        and the config keys.

        Parameters
        ----------
        source : str
            The source domain (e.g. ``"ethnews.com"``).

        Returns
        -------
        dict or None
            The source config dict, or ``None`` if not found.
        """
        stripped = self._strip_single_prefix(source)
        for key, config in self._sources_config.items():
            if self._strip_single_prefix(key) == stripped:
                return config
        return None

    def _save_content_cache(self, df: pd.DataFrame) -> None:
        """Save the content DataFrame to disk as JSON."""
        try:
            df.to_parquet(self._CONTENT_CACHE_PATH)
            logger.info(
                "Saved %d articles with content to '%s'.",
                len(df),
                self._CONTENT_CACHE_PATH,
            )
        except OSError as exc:
            logger.error("Failed to save content cache: %s", exc)

    def _load_redirected_urls(self) -> dict[str, str]:
        """Load previously resolved redirect URLs from disk.

        Returns
        -------
        dict[str, str]
            Mapping of redirect URL → final URL.  Empty dict if the
            cache file does not exist or cannot be parsed.
        """
        if not os.path.exists(self._CACHED_SOURCES_PATH) or not os.path.exists(
            self.cache_path
        ):
            return {}
        try:
            redirect_data = pd.read_json(self.cache_path).T
            link_data = redirect_data.query("source_type == 'link'")[
                ["redirect_url", "source"]
            ]
            cached_sources = (
                pd.read_json(self._CACHED_SOURCES_PATH)
                .T.reset_index()
                .rename(columns={"index": "url"})
            )

            cached_sources["source"] = self._remove_site_prefixes(
                cached_sources["source"]
            )
            link_data["source"] = self._remove_site_prefixes(
                link_data["source"]
            )

            cached_sources_list = cached_sources["source"].tolist()
            source_not_found = (
                link_data.query("source != @cached_sources_list")["source"]
                .unique()
                .tolist()
            )

            logger.error(
                "Sources not found: %s",
                source_not_found,
            )

            return link_data.query("source == @cached_sources_list")
        except (json.JSONDecodeError, OSError) as exc:
            logger.error(
                "Failed to load redirect cache from '%s': %s",
                self._CACHED_SOURCES_PATH,
                exc,
            )
            return {}

    def _save_redirected_urls(self, redirected_urls: dict[str, str]) -> None:
        """Persist the redirect-URL mapping to disk."""
        try:
            pd.DataFrame([redirected_urls]).to_parquet(
                self._CACHED_REDIRECT_PATH
            )
        except (OSError, ValueError) as exc:
            logger.error(
                "Failed to save redirect cache to '%s': %s",
                self._CACHED_REDIRECT_PATH,
                exc,
            )

    async def open_all_redirect_urls(self) -> None:
        """Open all redirect URLs, resolve final destinations, and
        fetch article content via the Jina reader API.

        Uses zendriver to open each redirect URL, records the final
        URL after redirection, then calls ``read_with_jina_api`` to
        extract the article content.  Shows a ``tqdm`` progress bar
        and incrementally saves partial results every
        ``_REDIRECT_SAVE_INTERVAL`` URLs so that progress is not
        lost on failure.
        """
        loaded_data = self._load_redirected_urls()
        news_content = pd.read_sql("SELECT * FROM news_content", self._engine)

        # Store loaded_data into news_content table
        unloaded_data = loaded_data[
            ~loaded_data["redirect_url"].isin(news_content["redirect_url"])
        ]
        unloaded_data.to_sql(
            "news_content", self._engine, if_exists="append", index=False
        )

        # remove data already in news_content table
        not_cached_data = news_content.query("content.isnull()")

        redirected_urls = not_cached_data["redirect_url"].tolist()
        sources = not_cached_data["source"].tolist()
        redirected_urls_dict: dict[str, str] = {}
        content_map: dict[str, str | None] = {}

        if not redirected_urls:
            return

        # Load content DataFrame upfront for incremental updates
        cached_df = pd.read_json(self.cache_path).T
        if "content" not in cached_df.columns:
            cached_df["content"] = None
        if "final_url" not in cached_df.columns:
            cached_df["final_url"] = None

        browser = await zd.start(headless=self.headless)
        unsaved_count = 0
        recently_fetched_urls: list[str] = []

        async def _open_redirect_page(
            redirect_url: str,
            source: str,
            bypass_cf: bool = False,
        ) -> None:
            try:
                page = await browser.get(self._BASE_URL[:-1] + redirect_url)

                if bypass_cf:
                    await self._bypass_cloudflare(page)
                is_cloudflare_page = await page.query_selector(
                    "cf-footer-item"
                )

                if is_cloudflare_page:
                    redirected_urls_dict[redirect_url] = (
                        "Cloudflare Page Error"
                    )
                    content_map[redirect_url] = None
                    return

                final_url = page.url
                redirected_urls_dict[redirect_url] = final_url
            except Exception as exc:
                logger.error(
                    "Failed to resolve redirect '%s': %s", redirect_url, exc
                )
                redirected_urls_dict[redirect_url] = f"Error: {exc}"
                content_map[redirect_url] = None
                return

            # Fetch article content via Jina API (only if redirected away from CryptoPanic)
            if final_url.startswith(self._BASE_URL):
                logger.debug(
                    "Still on CryptoPanic ('%s') — skipping Jina fetch.",
                    final_url[:80],
                )
                content_map[redirect_url] = None
            else:
                content = self.read_with_jina_api(final_url, source)
                content_map[redirect_url] = content

        def _incremental_save() -> None:
            """Update the DataFrame with current content and save both caches."""
            self._save_redirected_urls(redirected_urls_dict)
            cached_df["content"] = (
                cached_df["redirect_url"]
                .map(content_map)
                .combine_first(cached_df["content"])
            )
            cached_df["final_url"] = (
                cached_df["redirect_url"]
                .map(redirected_urls_dict)
                .combine_first(cached_df["final_url"])
            )
            self._save_content_cache(cached_df)

            if recently_fetched_urls:
                fetched_df = cached_df[
                    cached_df["redirect_url"].isin(recently_fetched_urls)
                ]
                self._save_data_in_db(fetched_df)
                recently_fetched_urls.clear()

        try:
            for redirect_url, source in tqdm(
                zip(redirected_urls, sources),
                desc="Resolving redirects",
                unit="url",
                total=len(redirected_urls),
            ):
                await _open_redirect_page(
                    redirect_url, source, bypass_cf=True
                )
                recently_fetched_urls.append(redirect_url)
                unsaved_count += 1

                if unsaved_count >= self._REDIRECT_SAVE_INTERVAL:
                    _incremental_save()
                    unsaved_count = 0
                    logger.info(
                        "Incremental save — %d redirected URLs so far.",
                        len(redirected_urls_dict),
                    )
        except Exception as e:
            logger.error("Error resolving redirects: %s", e)
            raise Exception(f"Error resolving redirects: {e}")
        finally:
            _incremental_save()
            logger.info(
                "Saved %d redirected URLs to '%s'.",
                len(redirected_urls_dict),
                self._CACHED_REDIRECT_PATH,
            )
            await browser.stop()

    def read_with_jina_api(self, url: str, source: str) -> str | None:
        """Fetch article content from a URL using the Jina reader API.

        Builds ``X-Target-Selector`` from the source's
        ``title_css_path`` and ``text_css_path``, and passes
        ``ignore_css_path`` as ``X-Remove-Selector``.

        Parameters
        ----------
        url : str
            The final article URL to read.
        source : str
            The source domain (e.g. ``"ethnews.com"``).

        Returns
        -------
        str or None
            The extracted article text, or ``None`` if the source
            should be skipped or configuration is missing.
        """
        config = self._find_source_config(source)
        if config is None:
            logger.debug("No source config found for '%s' — skipping.", source)
            return None

        if config.get("skip"):
            logger.debug("Source '%s' is marked as skip — skipping.", source)
            return None

        # Build X-Target-Selector from title + text CSS paths
        title_paths = config.get("title_css_path", "")
        text_paths = config.get("text_css_path", "")

        # Normalise to list (some sources use list, others string)
        if isinstance(title_paths, str):
            title_paths = [title_paths] if title_paths else []
        if isinstance(text_paths, str):
            text_paths = [text_paths] if text_paths else []

        target_selectors = [s for s in title_paths + text_paths if s]
        target_selector = ", ".join(target_selectors)

        ignore_css = config.get("ignore_css_path", "")

        headers: dict[str, str] = {}
        if self._jina_api_key:
            headers["Authorization"] = f"Bearer {self._jina_api_key}"
        if target_selector:
            headers["X-Target-Selector"] = target_selector
        if ignore_css:
            headers["X-Remove-Selector"] = ignore_css

        jina_url = f"{self._JINA_BASE_URL}{url}"

        try:
            response = requests.get(jina_url, headers=headers, timeout=10)
            response.raise_for_status()
            logger.info(
                "Jina API fetched %d chars for '%s'.",
                len(response.text),
                url[:80],
            )
            return response.text
        except requests.RequestException as exc:
            logger.error("Jina API request failed for '%s': %s", url, exc)
            return None
        finally:
            time.sleep(self._jina_rate_limit)

    def _save_data_in_db(
        self,
        content: pd.DataFrame,
        table_name: str = "news_content",
        schema: str = "public",
    ):
        content = content.copy()

        def make_serializable(obj):
            if isinstance(obj, np.ndarray):
                return obj.tolist()
            if isinstance(obj, (np.integer, np.floating)):
                return obj.item()
            return obj

        # Pre-process JSONB columns to handle non-serializable numpy types
        for col in ["currencies", "votes"]:
            if col in content.columns:
                content[col] = content[col].apply(make_serializable)

        database_url = self._DATABASE_URL
        engine = create_engine(database_url)

        from sqlalchemy.dialects.postgresql import insert

        def upsert_method(table, conn, keys, data_iter):
            data = [dict(zip(keys, row)) for row in data_iter]
            insert_stmt = insert(table.table).values(data)
            upsert_stmt = insert_stmt.on_conflict_do_update(
                index_elements=["redirect_url"],
                set_={
                    c.name: c
                    for c in insert_stmt.excluded
                    if c.name != "redirect_url"
                },
            )
            conn.execute(upsert_stmt)

        content.to_sql(
            name=table_name,
            con=engine,
            if_exists="append",
            index=False,
            dtype={"currencies": JSONB, "votes": JSONB},
            schema=schema,
            method=upsert_method,
        )
