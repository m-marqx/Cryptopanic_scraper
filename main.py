import asyncio
from api_news_scraper import CryptoPanicScraper

async def main():
    scraper = CryptoPanicScraper()
    scraper.filter = "all"
    scraper.limit = 50
    scraper.topic = "BTC"
    await scraper.run()

if __name__ == "__main__":
    asyncio.run(main())