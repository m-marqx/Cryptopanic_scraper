"""CryptoPanic news scraper module.

Uses zendriver to bypass Cloudflare protection and scrape market news
from CryptoPanic's infinite-scroll page. Caches results incrementally
to JSON files and builds a unique-source URL index.
"""

import asyncio
import json
import os
import re
from typing import TypedDict

import zendriver as zd
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


