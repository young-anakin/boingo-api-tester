#!/usr/bin/env python3
import argparse
from celery import Celery, chain
from datetime import datetime
import logging
import re
import json
import redis
import os

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Import agents
try:
    from crawleragent import process_scraping_sync, process_scraping_fully_sync
    from cleaner_agent import clean_all_listings_sync, clean_all_listings_fully_sync
    from formatter_agent import format_all_listings_fully_sync
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
def scrape_task(url, max_depth, max_pages, max_chunks=None, delay_seconds=1.0, max_total_tokens=None):
    logger.debug(f"Starting scrape_task for {url}")
    try:
        # Check if we should use the fully synchronous version
        use_sync = os.getenv("USE_SYNC", "false").lower() == "true"
        
        output_dir = "output"
        os.makedirs(output_dir, exist_ok=True)
        
        if use_sync:
            num_listings = process_scraping_fully_sync(
                url, max_depth, max_pages, max_chunks, delay_seconds, max_total_tokens
            )
        else:
            num_listings = process_scraping_sync(
                url, max_depth, max_pages, max_chunks, delay_seconds, max_total_tokens
            )
            
        output_file = f"output/raw_property_listings_{url_hash(url)}.json"
        logger.debug(f"Completed scrape_task for {url} with {num_listings} listings")
        return {"url": url, "raw_file": output_file, "num_listings": num_listings}
    except Exception as e:
        logger.error(f"Scrape task failed for {url}: {str(e)}")
        raise

@app.task(name='tasks.clean_task')
def clean_task(scrape_result, delay_seconds=0.0):
    """Clean the scraped results"""
    logger.debug(f"Starting clean_task for {scrape_result['url']}")
    try:
        output_dir = "output"
        os.makedirs(output_dir, exist_ok=True)
        
        raw_file = scrape_result["raw_file"]
        safe_url = url_hash(scrape_result['url'])
        cleaned_file = os.path.join(output_dir, f"cleaned_property_listings_{safe_url}.json")
        
        if not os.path.exists(raw_file):
            logger.error(f"Raw file not found: {raw_file}")
            with open(cleaned_file, 'w', encoding='utf-8') as f:
                json.dump([], f)
            return {
                'url': scrape_result['url'],
                'cleaned_file': cleaned_file,
                'num_listings': 0,
                'error': 'Raw file not found'
            }
        
        cleaned_listings = clean_all_listings_fully_sync(raw_file, delay_seconds)
        
        with open(cleaned_file, 'w', encoding='utf-8') as f:
            json.dump([listing.dict() for listing in cleaned_listings], f, indent=2)
        
        logger.debug(f"Completed clean_task for {scrape_result['url']} with {len(cleaned_listings)} listings")
        return {
            'url': scrape_result['url'],
            'cleaned_file': cleaned_file,
            'num_listings': len(cleaned_listings)
        }
    except Exception as e:
        logger.error(f"Clean task failed for {scrape_result['url']}: {str(e)}")
        raise

@app.task(name='tasks.format_task')
def format_task(clean_result):
    """Format the cleaned results"""
    logger.debug(f"Starting format_task for {clean_result['url']}")
    try:
        output_dir = "output"
        os.makedirs(output_dir, exist_ok=True)
        
        cleaned_file = clean_result["cleaned_file"]
        safe_url = url_hash(clean_result['url'])
        formatted_file = os.path.join(output_dir, f"formatted_property_listings_{safe_url}.md")
        
        if not os.path.exists(cleaned_file):
            logger.error(f"Cleaned file not found: {cleaned_file}")
            return {
                'url': clean_result['url'],
                'formatted_file': formatted_file,
                'error': 'Cleaned file not found'
            }
        
        markdown_content = format_all_listings_fully_sync(cleaned_file)
        
        with open(formatted_file, 'w', encoding='utf-8') as f:
            f.write(markdown_content)
        
        with open(cleaned_file, 'r', encoding='utf-8') as f:
            listings_data = json.load(f)
        
        logger.debug(f"Completed format_task for {clean_result['url']}")
        return {
            'url': clean_result['url'],
            'formatted_file': formatted_file,
            'num_listings': len(listings_data)
        }
    except Exception as e:
        logger.error(f"Format task failed for {clean_result['url']}: {str(e)}")
        raise

def url_hash(url):
    """Create a safe filename from a URL"""
    return re.sub(r'[^a-zA-Z0-9]', '_', url)[:50]

def queue_tasks(urls, max_depth=2, max_pages=5, max_chunks=3, delay_seconds=2.0, cleaner_delay=1.0, max_total_tokens=10000):
    """Queue tasks for multiple URLs"""
    pipeline_start = datetime.now()
    logger.info("=" * 60)
    logger.info("PROPERTY LISTING PIPELINE (MULTI-SITE)")
    logger.info(f"Started at: {pipeline_start.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Processing {len(urls)} websites")
    logger.info(f"Configuration: max_depth={max_depth}, max_pages={max_pages}, max_chunks={max_chunks}")
    logger.info(f"API limits: max_total_tokens={max_total_tokens}")
    logger.info(f"Delays: scraper={delay_seconds}s, cleaner={cleaner_delay}s")
    logger.info("=" * 60)

    for url in urls:
        try:
            logger.debug(f"Queuing tasks for {url}")
            workflow = chain(
                scrape_task.s(url, max_depth, max_pages, max_chunks, delay_seconds, max_total_tokens),
                clean_task.s(cleaner_delay),
                format_task.s()
            )
            result = workflow.apply_async()
            logger.debug(f"Tasks queued for {url} with task ID: {result.task_id}")
        except Exception as e:
            logger.error(f"Failed to queue tasks for {url}: {str(e)}")
    logger.info("Tasks queued across scraper_queue, cleaner_queue, and formatter_queue.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Property Listing Pipeline with Celery')
    parser.add_argument('urls', help='Comma-separated URLs (e.g., "url1,url2")')
    parser.add_argument('--depth', type=int, default=2)
    parser.add_argument('--pages', type=int, default=5)
    parser.add_argument('--max-chunks', type=int, default=3)
    parser.add_argument('--delay', type=float, default=2.0)
    parser.add_argument('--cleaner-delay', type=float, default=1.0)
    parser.add_argument('--max-tokens', type=int, default=10000)
    parser.add_argument('--sync', action='store_true')
    args = parser.parse_args()
    
    if args.sync:
        os.environ["USE_SYNC"] = "true"
        logger.info("Running in fully synchronous mode")
    
    urls = [url.strip() for url in args.urls.split(',') if url.strip()]
    queue_tasks(
        urls, 
        args.depth, 
        args.pages, 
        args.max_chunks, 
        args.delay, 
        args.cleaner_delay,
        args.max_tokens
    )