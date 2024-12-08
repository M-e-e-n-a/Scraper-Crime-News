# crime_data_pipeline/main.py
import os
from datetime import datetime
import logging
from database import CrimeDatabase
from scrapers import NewsAPIScraper, PoliceScraper

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def main():
    # Initialize database
    db = CrimeDatabase('crime_data.db')
    
    # Initialize scrapers
    news_scraper = NewsAPIScraper(os.getenv('NEWS_API_KEY'))
    police_scraper = PoliceScraper()
    
    # Fetch and save news data
    logging.info("Fetching news data...")
    news_data = news_scraper.fetch_data()
    if not news_data.empty:
        saved = db.save_incidents(news_data)
        db.update_source('newsapi', 'success', saved)
        logging.info(f"Saved {saved} news incidents")
    
    # Fetch and save police data
    logging.info("Fetching police data...")
    police_data = police_scraper.fetch_data()
    if not police_data.empty:
        saved = db.save_incidents(police_data)
        db.update_source('police', 'success', saved)
        logging.info(f"Saved {saved} police incidents")

if __name__ == "__main__":
    main()



