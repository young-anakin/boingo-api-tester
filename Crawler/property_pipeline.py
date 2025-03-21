#!/usr/bin/env python3
import argparse
from celery import Celery, chain
from datetime import datetime
import logging
import re
import json
import redis

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Import agents
try:
    from crawleragent import process_scraping_sync
    from cleaner_agent import clean_all_listings_sync
    from formatter_agent import format_all_listings_sync
    logger.debug("Successfully imported agents")
except ImportError as e:
    logger.error(f"Failed to import agents: {str(e)}")
    raise

# Celery configuration
app = Celery('tasks', broker='redis://localhost:6379/0')
app.conf.task_queues = {
    'scraper_queue': {'exchange': 'scraper_queue', 'routing_key': 'scraper'},
    'cleaner_queue': {'exchange': 'cleaner_queue', 'routing_key': 'cleaner'},
    'formatter_queue': {'exchange': 'formatter_queue', 'routing_key': 'formatter'},
}
app.conf.task_routes = {
    'tasks.scrape_task': {'queue': 'scraper_queue'},
    'tasks.clean_task': {'queue': 'cleaner_queue'},
    'tasks.format_task': {'queue': 'formatter_queue'},
}

# Test Redis connection
try:
    r = redis.Redis(host='localhost', port=6379, db=0)
    r.ping()
    logger.debug("Successfully connected to Redis")
except redis.ConnectionError as e:
    logger.error(f"Failed to connect to Redis: {str(e)}")
    raise

# Define Celery tasks
@app.task(name='tasks.scrape_task')
def scrape_task(url, max_depth, max_pages):
    logger.debug(f"Starting scrape_task for {url}")
    try:
        num_listings = process_scraping_sync(url, max_depth, max_pages)
        output_file = f"raw_property_listings_{url_hash(url)}.json"
        logger.debug(f"Completed scrape_task for {url} with {num_listings} listings")
        return {"url": url, "raw_file": output_file, "num_listings": num_listings}
    except Exception as e:
        logger.error(f"Scrape task failed for {url}: {str(e)}")
        raise

@app.task(name='tasks.clean_task')
def clean_task(scrape_result):
    logger.debug(f"Starting clean_task for {scrape_result['url']}")
    try:
        raw_file = scrape_result["raw_file"]
        cleaned_file = f"cleaned_property_listings_{url_hash(scrape_result['url'])}.json"
        num_cleaned = clean_all_listings_sync(raw_file, cleaned_file)
        logger.debug(f"Completed clean_task for {scrape_result['url']} with {num_cleaned} listings")
        return {"url": scrape_result["url"], "cleaned_file": cleaned_file}
    except Exception as e:
        logger.error(f"Clean task failed for {scrape_result['url']}: {str(e)}")
        raise

@app.task(name='tasks.format_task')
def format_task(clean_result):
    logger.debug(f"Starting format_task for {clean_result['url']}")
    try:
        cleaned_file = clean_result["cleaned_file"]
        formatted_file = f"formatted_property_listings_{url_hash(clean_result['url'])}.md"
        format_all_listings_sync(cleaned_file, formatted_file)
        logger.debug(f"Completed format_task for {clean_result['url']}")
        return {"url": clean_result["url"], "formatted_file": formatted_file}
    except Exception as e:
        logger.error(f"Format task failed for {clean_result['url']}: {str(e)}")
        raise

def url_hash(url):
    return re.sub(r'[^a-zA-Z0-9]', '_', url)[:50]

def queue_tasks(urls, max_depth=2, max_pages=5):
    """Queue tasks for multiple URLs"""
    pipeline_start = datetime.now()
    logger.info("=" * 60)
    logger.info("PROPERTY LISTING PIPELINE (MULTI-SITE)")
    logger.info(f"Started at: {pipeline_start.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Processing {len(urls)} websites")
    logger.info("=" * 60)

    for url in urls:
        try:
            logger.debug(f"Queuing tasks for {url}")
            workflow = chain(
                scrape_task.s(url, max_depth, max_pages),
                clean_task.s(),
                format_task.s()
            )
            result = workflow.apply_async()
            logger.debug(f"Tasks queued for {url} with task ID: {result.task_id}")
        except Exception as e:
            logger.error(f"Failed to queue tasks for {url}: {str(e)}")
    logger.info("Tasks queued across scraper_queue, cleaner_queue, and formatter_queue.")

# For CLI testing (optional)
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Property Listing Pipeline with Celery')
    parser.add_argument('urls', help='Comma-separated URLs (e.g., "url1,url2")')
    parser.add_argument('--depth', type=int, default=2)
    parser.add_argument('--pages', type=int, default=5)
    args = parser.parse_args()
    urls = [url.strip() for url in args.urls.split(',') if url.strip()]
    queue_tasks(urls, args.depth, args.pages)