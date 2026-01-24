import asyncio
import os
import pickle
import pathlib
import time
import requests
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from logging_config import logger
from tqdm import tqdm
import json
from typing import Literal
import sqlalchemy
import nodriver as uc
from nodriver import Tab

class CryptoPanicScraper:
    SCROLL_PAUSE_TIME = 2000

    def __init__(
        self,
        filter=None,
        limit=10,
        topic=None,
        verify_template_path=None,
        save_path="news_data",
        jina_api_key=None,
        max_retries=2,
        database: str | sqlalchemy.engine.Engine | None = None,
    ):
        self.filter = filter
        self.limit = limit
        self.topic = topic
        self.save_path = save_path
        self.data = []
        self.template_path = verify_template_path

        if isinstance(database, str):
            self.engine = sqlalchemy.create_engine(database)
        else:
            self.engine = database

        pathlib.Path(self.save_path).mkdir(parents=True, exist_ok=True)
        self.file_name = "cryptopanic"
        self.file_name += f"_{self.filter}" if self.filter else ""
        self.file_name += f"_{self.topic}" if self.topic else ""
        self.file_name += "_cache.json"
        self.file_path = os.path.join(self.save_path, self.file_name)
        self.max_retries = max_retries

        self.cached_data = self.load_cached_data()
        self.vader_analyzer = SentimentIntensityAnalyzer()
        self._update_vader_lexicon()

        self.api_key = jina_api_key

    def _update_vader_lexicon(self):
        """Updates VADER lexicon with custom financial terms and their sentiment scores."""
        financial_terms = {
            "profit": 2.0,
            "loss": -2.0,
            "gain": 1.5,
            "surge": 2.0,
            "collapse": -2.5,
            "plummet": -2.5,
            "bullish": 2.5,
            "bearish": -2.5,
            "recession": -3.0,
            "growth": 2.0,
            "decline": -1.5,
            "invest": 1.5,
            "inflation": -2.0,
            "revenue": 1.5,
            "downgrade": -2.0,
            "upgrade": 1.8,
            "outperform": 2.0,
            "underperform": -2.0,
            "volatile": -1.5,
            "fiscal": 1.2,
        }
        self.vader_analyzer.lexicon.update(financial_terms)
        logger.info("Updated VADER lexicon with financial terms.")

    def load_cached_data(
        self,
        format: Literal["pickle", "json", "db"] = "json"
    ):
        """Load previously scraped data (cached data) from the file if it exists."""
        if format == "db":
            if not self.engine:
                logger.error("No database engine provided.")
                raise TypeError("Database engine is not set.")

            try:
                cached_data = {}
                conn = self.engine.connect()
                result = conn.execute(sqlalchemy.text("SELECT * FROM news"))
                for row in result:
                    cached_data[row["url"]] = {
                        "Date": row["date"],
                        "Title": row["title"],
                        "Currencies": json.loads(row["currencies"]),
                        "Votes": json.loads(row["votes"]),
                        "Source": row["source"],
                        "Source_Type": row["source_type"],
                        "URL": row["url"],
                        "Sentiment": row["sentiment"],
                        "Confidence": row["confidence"],
                    }
                logger.info(
                    f"Loaded cached data from database with {len(cached_data)} entries."
                )
                return cached_data
            except Exception as e:
                logger.error(f"Failed to load cached data from database: {e}")
                return {}

        if os.path.exists(self.file_path):
            try:
                if format == "pickle":
                    with open(self.file_path, "rb") as f:
                        cached_data = pickle.load(f)
                elif format == "json":
                    with open(self.file_path, "r") as f:
                        cached_data = json.load(f)
                logger.info(
                    f"Loaded cached data from {self.file_path} with {len(cached_data)} entries."
                )
                return cached_data

            except Exception as e:
                logger.error(f"Failed to load cached data: {e}")
                return {}

        logger.info("No cached data file found. Starting fresh.")
        return {}

    async def cloudflare_bypass(self, page: Tab):
            counter = 0
            while await page.find("Ray ID"):
                counter += 1
                logger.warning(
                    f"Cloudflare challenge detected, waiting to verify... (Attempt {counter})"
                )
                await page.sleep(5)
                await page.verify_cf(self.template_path)

                if counter >= 5:
                    logger.error(
                        "Failed to bypass Cloudflare challenge after multiple attempts."
                    )
                    await page.save_screenshot(r"cf_challenge.png")
                    raise Exception("Cloudflare challenge could not be bypassed.")

    async def run(self):
        """Run the scraper."""
        browser = await uc.start(headless=False, lang="en")
        try:
            page = await browser.get(f"https://www.cryptopanic.com/news?filter={self.filter}")
            logger.info(f"Navigating to https://www.cryptopanic.com/news?filter={self.filter}")

            await self.cloudflare_bypass(page)

            if self.topic:
                await self.search_topic(page, self.topic)

            await self.collect_data(page)

        finally:
            browser.stop()

        # Save the collected data to a file if there are new articles
        if self.data:
            self.merge_data()
            rate_time = 0.12 if self.api_key else 3.0
            if self.api_key:
                logger.info(
                    "Using authenticated requests for description fetching."
                )
                self.update_descriptions(
                    rate_time, update_cached_data=False
                )
            else:
                logger.info(
                    "Using unauthenticated requests for description fetching."
                )
                await self.async_update_descriptions(
                    rate_time, update_cached_data=False
                )
            self.save_data(format="json")

    async def get_news_sentiment(self, symbol: str):
        """Fetch sentiment data for a given symbol."""
        self.topic = symbol  # Set topic to the requested symbol
        await self.run()  # Run the scraper to fetch news
        return self.data  # Return the collected data

    async def search_topic(self, page, topic):
        """Search for a specific topic using the search bar."""
        try:
            # Locate the search input element and enter the topic
            try:
                search_input = await page.query_selector("#acSearchInput")
            except Exception:
                search_input = None

            if search_input:
                logger.info(f"Searching for topic: {topic}")
                await search_input.send_keys(topic)
                await search_input.send_keys("\n")  # Press Enter
                await page.sleep(2)  # Give some time for results to load

                # Optionally click the first relevant result
                try:
                    first_result = await page.query_selector(
                        ".ac__entry.ac__selected"
                    )
                except Exception:
                    first_result = None

                if first_result:
                    await first_result.click()
                    logger.info(
                        f"Clicked on the first search result for topic: {topic}"
                    )
                    await page.sleep(2)
            else:
                logger.warning("Search input not found.")
        except Exception as e:
            logger.error(f"Failed to search for topic: {e}")

    async def collect_data(self, page):
        """Collect data from the page."""
        retries = 0
        loaded_elements = 0

        while loaded_elements < self.limit and retries < self.max_retries:
            # Load more content if possible
            await self.load_more(page)

            articles = await page.query_selector_all(
                "div.news-row.news-row-link"
            )
            loaded_elements = len(articles)
            logger.info(f"Loaded {loaded_elements} articles so far.")

            # If enough articles are loaded, stop
            if loaded_elements >= self.limit:
                break

            retries += 1
            logger.warning(
                f"Retry {retries}/{self.max_retries} to load more articles."
            )

        if loaded_elements == 0:
            logger.error("No articles loaded.")
            return

        logger.info(f"Processing {loaded_elements} articles.")

        # Process each article
        for i, article in enumerate(articles[: self.limit]):
            try:
                # Extract title
                title = await self.retry_fetch_text(
                    article, "span.title-text span", "Untitled"
                )

                # Skip untitled articles
                if title == "Untitled":
                    logger.info(f"Skipping article {i + 1}: No title found.")
                    continue

                # Extract URL
                source_url = await self.retry_fetch_attribute(
                    article, "a.news-cell.nc-title", "href", "N/A"
                )

                # Skip already cached articles
                if source_url in self.cached_data:
                    logger.info(f"Skipping article {i + 1}: Already cached.")
                    continue

                # Extract date
                date_time = await self.retry_fetch_attribute(
                    article, "time", "datetime", "N/A"
                )

                # Extract source
                source_name = await self.retry_fetch_text(
                    article, "span.si-source-domain", "Unknown"
                )

                # Detect source type (Twitter, YouTube, or standard link)
                source_type = await self.get_source_type(article)

                # Extract currencies
                currencies = await self.get_currencies(article)

                # Extract votes
                votes = await self.get_votes(article)

                # Perform sentiment analysis on the title
                sentiment, confidence = self.analyze_sentiment_with_vader(
                    title
                )

                # Prepare data entry
                article_data = {
                    "Date": date_time,
                    "Title": title,
                    "Currencies": currencies,
                    "Votes": votes,
                    "Source": source_name,
                    "Source_Type": source_type,
                    "URL": source_url,
                    "Sentiment": sentiment,
                    "Confidence": confidence,
                }

                self.data.append(article_data)
                logger.info(
                    f"Scraped article {i + 1}: {title} - {sentiment} - {confidence}%"
                )

            except Exception as e:
                logger.error(f"Error scraping article {i + 1}: {e}")

        logger.info(f"Finished gathering {len(self.data)} rows of data.")

    async def load_more(self, page):
        """Load more content by scrolling down or clicking 'Load More'."""
        try:
            # Check if there's a 'Load More' button and click it if it exists
            load_more_button = await page.query_selector(
                ".btn-outline-primary"
            )
            if load_more_button:
                await load_more_button.click()
                logger.info("Clicked 'Load More' button.")
            else:
                # If no 'Load More' button, simulate scrolling
                await page.evaluate(
                    "window.scrollTo(0, document.body.scrollHeight)"
                )
                logger.info("Scrolled to the bottom of the page.")

            # Wait for the new content to load
            await page.wait_for_timeout(self.SCROLL_PAUSE_TIME)

        except Exception as e:
            logger.error(f"Failed to load more content: {e}")

    async def retry_fetch_text(self, element, selector, default_value):
        """Retry fetching text content from an element."""
        attempt = 0
        while attempt < self.max_retries:
            try:
                elem = await element.query_selector(selector)
                if elem:
                    text = await elem.inner_text()
                    if text:
                        return text.strip()
            except Exception as e:
                logger.warning(
                    f"Retry {attempt + 1}/{self.max_retries} fetching text for {selector}: {e}"
                )
            attempt += 1
            await asyncio.sleep(1)

        logger.warning(
            f"Failed to fetch text for {selector} after {self.max_retries} retries. Using default: '{default_value}'"
        )
        return default_value

    async def retry_fetch_attribute(self, element, selector, attribute, default_value):
        """Retry fetching an attribute from an element."""
        attempt = 0
        while attempt < self.max_retries:
            try:
                elem = await element.query_selector(selector)
                if elem:
                    attr = await elem.get_attribute(attribute)
                    if attr:
                        return attr.strip()
            except Exception as e:
                logger.warning(f"Retry {attempt+1}/{self.max_retries} fetching attribute {attribute} for {selector}: {e}")
            attempt += 1
            await asyncio.sleep(1)

        logger.warning(f"Failed to fetch attribute {attribute} for {selector} after {self.max_retries} retries. Using default: '{default_value}'")
        return default_value

    async def get_source_type(self, element):
        """Detect the source type based on the icon class (Twitter, YouTube, or Link)."""
        try:
            # Check for Twitter icon
            twitter_icon = await element.query_selector("span.open-link-icon.icon.icon-twitter")
            if twitter_icon:
                return "twitter"

            # Check for YouTube icon
            youtube_icon = await element.query_selector("span.open-link-icon.icon.icon-youtube-play")
            if youtube_icon:
                return "youtube"

            # Default to standard link
            return "link"
        except Exception as e:
            logger.warning(f"Failed to detect source type: {e}")
            return "unknown"

    async def get_currencies(self, element):
        """Fetch currencies linked to the article."""
        try:
            currency_elements = await element.query_selector_all("a.colored-link")
            currencies = [await currency.inner_text() for currency in currency_elements]
            return currencies if currencies else ['No currency']
        except Exception as e:
            logger.warning(f"Failed to fetch currencies: {e}")
            return ['No currency']

    async def get_votes(self, element):
        """Extract vote data."""
        votes = {}
        try:
            vote_elements = await element.query_selector_all("span.nc-vote-cont")
            for vote_element in vote_elements:
                vote_title = await vote_element.get_attribute('title')
                if vote_title:
                    value = vote_title[:2]
                    action = vote_title.replace(value, '').replace('votes', '').strip()
                    votes[action] = int(value)
        except Exception as e:
            logger.warning(f"Failed to fetch votes: {e}")
        return votes

    def analyze_sentiment_with_vader(self, title):
        """Uses VADER to analyze sentiment for a given article title."""
        vader_scores = self.vader_analyzer.polarity_scores(title)
        compound_score = vader_scores['compound']

        # Enhanced thresholds for sentiment classification
        if compound_score >= 0.6:
            sentiment = 'very bullish'
        elif 0.1 <= compound_score < 0.6:
            sentiment = 'bullish'
        elif -0.1 < compound_score < 0.1:
            sentiment = 'neutral'
        elif -0.6 < compound_score <= -0.1:
            sentiment = 'bearish'
        else:
            sentiment = 'very bearish'

        confidence = int((abs(compound_score) ** 0.5) * 100)  # Confidence boost for more extreme scores
        title_length_factor = max(0.8, min(len(title) / 50, 1.2))  # Adjust based on length, clamped between 0.8 and 1.2
        confidence = int(confidence * title_length_factor)
        return sentiment, max(0, min(confidence, 100))  # Ensure confidence is within 0-100

    def merge_data(self):
        """Merge newly scraped data with cached data."""
        for article in self.data:
            self.cached_data[article['URL']] = article

    def save_data(self, format: Literal['pickle', 'json', 'db'] = 'pickle'):
        if format == 'pickle':
            with open(self.file_path, 'wb') as f:
                pickle.dump(self.cached_data, f)
        elif format == 'json':
            with open(self.file_path, 'w') as f:
                json.dump(self.cached_data, f)
        elif format == 'db':
            self.save_data_to_db()

        logger.info(f"Data saved to {self.file_path}")

    def save_data_to_db(self):
        """Save data to a database (placeholder function)."""
        if not self.engine:
            logger.error("No database engine provided.")
            raise TypeError("Database engine is not set.")

        self.engine.connect()
        metadata = sqlalchemy.MetaData()
        news_table = sqlalchemy.Table(
            'news',
            metadata,
            sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True, autoincrement=True),
            sqlalchemy.Column('date', sqlalchemy.String),
            sqlalchemy.Column('title', sqlalchemy.String),
            sqlalchemy.Column('currencies', sqlalchemy.String),
            sqlalchemy.Column('votes', sqlalchemy.String),
            sqlalchemy.Column('source', sqlalchemy.String),
            sqlalchemy.Column('source_type', sqlalchemy.String),
            sqlalchemy.Column('url', sqlalchemy.String, primary_key=True),
            sqlalchemy.Column('sentiment', sqlalchemy.String),
            sqlalchemy.Column('confidence', sqlalchemy.Integer),
        )
        metadata.create_all(self.engine)
        conn = self.engine.connect()

        for article in self.cached_data.values():
            ins = news_table.insert().values(
                date=article['Date'],
                title=article['Title'],
                currencies=json.dumps(article['Currencies']),
                votes=json.dumps(article['Votes']),
                source=article['Source'],
                source_type=article['Source_Type'],
                url=article['URL'],
                sentiment=article['Sentiment'],
                confidence=article['Confidence'],
            )
            try:
                conn.execute(ins)
            except sqlalchemy.exc.IntegrityError:
                logger.info(f"Article already exists in DB: {article['URL']}")

    def fetch_description_body_jina(
        self,
        url: str,
        minimum_time: float,
    ) -> str:
        start = time.perf_counter()
        url = f"https://r.jina.ai/https://cryptopanic.com{url}"

        headers = {
            "Accept": "application/json",
            "X-Engine": "direct",
            "X-Return-Format": "text",
        }

        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"

        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()  # Raise an error for HTTP errors
        except requests.RequestException as e:
            logger.error(f"Error fetching {url}: {e}")
            return f"Error fetching content: {e}"
        except Exception as e:
            logger.error(f"Unexpected error fetching {url}: {e}")
            return f"Unexpected error fetching content: {e}"

        end = time.perf_counter()

        if end - start < minimum_time:
            time.sleep(minimum_time - (end - start))
        if response.status_code == 200:
            description_body = response.json()["data"]["description"]
            return description_body

    async def async_fetch_description_body(
        self,
        page,
        url: str,
        minimum_time: float,
        max_retries: int = 5,
    ) -> str:
        """Fetch description body using an existing page instance with retry logic for rate limiting."""
        start = time.perf_counter()
        full_url = f"https://cryptopanic.com{url}"

        for attempt in range(max_retries):
            try:
                response = await page.goto(full_url)

                # Check for nginx 429 error
                if response and response.status == 429:
                    logger.warning(f"Rate limit (429) encountered for {full_url}, attempt {attempt + 1}/{max_retries}")
                    await asyncio.sleep(minimum_time * (attempt + 1))  # Exponential backoff
                    continue
                if response and response.status == 502:
                    logger.warning(f"Server error (502) encountered for {full_url}, attempt {attempt + 1}/{max_retries}")
                    await asyncio.sleep(minimum_time * (attempt + 1))  # Exponential backoff
                    continue

                error_table_locator = page.get_by_text(
                    "(╯°□°）╯︵ ┻━┻",
                    exact=False
                )

                error_text_locator = page.get_by_text(
                    "Can't retrieve data. Possible maintenance. Please try again in a moment.",
                    exact=False
                )

                if await error_table_locator.is_visible() or await error_text_locator.is_visible():
                    logger.warning(f"Maintenance message found for {full_url}, attempt {attempt + 1}/{max_retries}")
                    await asyncio.sleep(minimum_time * (attempt + 1))
                    continue

                # Wait for content to load
                await page.wait_for_selector("div.description-body", timeout=5000)
                content = await page.inner_text("div.description-body")


                # Success - apply rate limiting
                end = time.perf_counter()
                if end - start < minimum_time:
                    await asyncio.sleep(minimum_time - (end - start))
                return content

            except Exception as e:
                logger.warning(f"Error fetching {full_url} (attempt {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(minimum_time * (attempt + 1))
                else:
                    logger.error(f"Failed to fetch {full_url} after {max_retries} attempts: {e}")
                    return f"Error fetching content: {e}"

        return f"Error: Failed after {max_retries} retries"

    def update_descriptions(
        self,
        rate_time: float = 3.0, # Default jina rate limit for free tier for non-authenticated requests
        update_cached_data: bool = False, # Whether to update cached data or current data
    ):
        """Load descriptions for articles that don't have them yet."""
        start_time = time.perf_counter()
        data_source = self.cached_data if update_cached_data else self.data
        for article in tqdm(data_source, desc="Fetching descriptions"):
            if 'description_body' not in article or not article['description_body']:
                description = self.fetch_description_body_jina(
                    article['URL'],
                    rate_time,
                )
                article['description_body'] = description
                end_time = time.perf_counter()
                elapsed_time = end_time - start_time
                if elapsed_time < rate_time:
                    time.sleep(rate_time - elapsed_time)
                    logger.info(
                        f"Sleeping for {rate_time - elapsed_time:.2f} seconds to respect rate limit"
                    )

    async def async_update_descriptions(
        self,
        rate_time: float = 1.0, # Default jina rate limit for free tier for non-authenticated requests
        update_cached_data: bool = False, # Whether to update cached data or current data
        max_concurrent: int = 4, # Maximum number of concurrent requests
    ):
        """Load descriptions for articles that don't have them yet using parallel browser sessions with page pool."""
        data_source = self.cached_data if update_cached_data else self.data

        # Filter articles that need descriptions (skip Twitter and YouTube posts)
        articles_to_update = [
            article for article in data_source
            if ('description_body' not in article or not article['description_body'])
            and article.get('Source_Type') not in ['twitter', 'youtube']
        ]

        if not articles_to_update:
            logger.info("No articles need description updates.")
            return

        # Process articles in parallel with a pool of reusable pages
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=False)

            # Create a pool of pages
            page_pool = [await browser.new_page() for _ in range(max_concurrent)]
            page_queue = asyncio.Queue()
            for page in page_pool:
                await page_queue.put(page)

            async def fetch_with_page_pool(article):
                # Get a page from the pool
                page = await page_queue.get()
                try:
                    description = await self.async_fetch_description_body(
                        page,
                        article['URL'],
                        rate_time,
                    )
                    article['description_body'] = description
                finally:
                    # Return page to the pool
                    await page_queue.put(page)

            # Create tasks for all articles
            tasks = [fetch_with_page_pool(article) for article in articles_to_update]

            # Execute all tasks with progress bar
            for coro in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc="Fetching descriptions"):
                await coro

            # Close all pages in the pool
            for page in page_pool:
                await page.close()

            await browser.close()
            logger.info(f"Updated descriptions for {len(articles_to_update)} articles.")