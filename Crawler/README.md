# Boingo Property Crawler

A comprehensive system for scraping, cleaning, and formatting real estate listings from websites, using AI-powered data extraction and processing.

## System Overview

The Boingo Property Crawler consists of several components that work together to extract real estate listings from websites, clean the data, and generate formatted reports:

- **Web Crawler Agent**: Scrapes property listings from real estate websites
- **Cleaner Agent**: Processes raw data into standardized formats
- **Formatter Agent**: Converts clean data into professional Markdown reports
- **Property Pipeline**: Orchestrates the workflow using Celery for task distribution

## Prerequisites

- Python 3.8+
- Docker (for Redis)
- OpenAI API key (for GPT-powered extraction and formatting)
- Required Python packages (see below)

## Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/boingo-property-crawler.git
cd boingo-property-crawler
```

2. Install required packages:
```bash
pip install -r requirements.txt
```

3. Set up your OpenAI API key:
```bash
# Copy the example .env file
cp .env.example .env

# Edit the .env file with your API key
# Replace 'your_openai_api_key_here' with your actual OpenAI API key
```

## Setup Redis with Docker

Redis is used as a message broker for Celery. Set it up using Docker:

```bash
# Check existing containers
docker ps -a

# Stop and remove any existing Redis container (if needed)
docker stop redis-server
docker rm redis-server

# Start a new Redis container
docker run -d --name redis-server -p 6379:6379 redis:latest
```

## Running the System

### 1. Start Celery Workers

In one terminal, start the Celery workers to process tasks:

```bash
celery -A property_pipeline worker --loglevel=info --pool=solo -Q scraper_queue,cleaner_queue,formatter_queue
```

### 2. Launch the GUI Application

In another terminal, start the GUI application:

```bash
python crawleragent.py
```

This will open a user interface where you can:
- Enter URLs of real estate websites to scrape
- Set crawling parameters (depth and pages)
- Start the scraping process

## Using the Command Line Interface

Alternatively, you can use the command line interface:

```bash
python property_pipeline.py "https://www.example-real-estate.com/listings,https://another-site.com/properties" --depth 2 --pages 5
```

## Output Files

The system generates three types of files:
- `raw_property_listings_*.json`: Initial scraped data
- `cleaned_property_listings_*.json`: Standardized property data
- `formatted_property_listings_*.md`: Professional Markdown report

## Troubleshooting

- If you encounter connection errors with Redis, make sure the Docker container is running
- For OpenAI API errors, verify your API key is correctly set in your `.env` file
- If the Celery workers aren't processing tasks, check that they're running with the correct queue names

## Notes

- The system uses OpenAI's GPT-3.5 model for data extraction and formatting
- The `.env` file is included in `.gitignore` to prevent accidentally pushing your API key to GitHub
- The default crawling depth and pages are set conservatively; adjust based on your needs 