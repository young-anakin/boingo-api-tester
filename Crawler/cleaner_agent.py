import asyncio
import json
import logging
import re
import os
import time
from typing import List
from openai import AsyncOpenAI, OpenAI
from crawleragent import PropertyListing
from dotenv import load_dotenv
import tenacity
import tiktoken

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Set up OpenAI client
client = AsyncOpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# Initialize tokenizer
tokenizer = tiktoken.encoding_for_model("gpt-3.5-turbo")

# Retry decorator for API calls
@tenacity.retry(
    wait=tenacity.wait_exponential(multiplier=1, min=4, max=60),
    stop=tenacity.stop_after_attempt(3),
    retry=tenacity.retry_if_exception_type(Exception),
    before_sleep=lambda retry_state: logger.info(f"Retrying API call (attempt {retry_state.attempt_number}) after {retry_state.next_action.sleep} seconds...")
)
async def clean_single_listing_async(listing_json: str):
    """Clean a single listing using OpenAI API"""
    response = await client.chat.completions.create(
        model="gpt-3.5-turbo",
        messages=[
            {"role": "system", "content": (
                "You are a data cleaning assistant. Clean and standardize the provided JSON object representing a single real estate listing. "
                "The input may have nested fields (e.g., 'address' as an object with 'country', 'region', 'city', 'district'). "
                "Flatten the 'address' field into a single string (e.g., 'district, city, region, country') if it's an object. "
                "Ensure all required fields are present and valid: address (string), price (float), currency (string), "
                "bedrooms (float), bathrooms (float), listing_type ('rent' or 'buy'), property_type (string), "
                "description (string), image_link (string). Optional fields: square_footage (integer), year_built (integer), "
                "amenities (array of strings), additional_info (object). If required fields are missing or invalid, "
                "return an empty object {}. Standardize formats (e.g., price as float, remove extra text). "
                "Return a cleaned JSON object compatible with this structure."
            )},
            {"role": "user", "content": f"Clean this JSON listing:\n\n{listing_json}"}
        ]
    )
    return response.choices[0].message.content

@tenacity.retry(
    wait=tenacity.wait_exponential(multiplier=1, min=4, max=60),
    stop=tenacity.stop_after_attempt(3),
    retry=tenacity.retry_if_exception_type(Exception),
    before_sleep=lambda retry_state: logger.info(f"Retrying API call (attempt {retry_state.attempt_number}) after {retry_state.next_action.sleep} seconds...")
)
def clean_single_listing_sync(listing_json: str):
    """Synchronous version to clean a single listing using OpenAI API"""
    response = client.chat.completions.create(
        model="gpt-3.5-turbo",
        messages=[
            {"role": "system", "content": (
                "You are a data cleaning assistant. Clean and standardize the provided JSON object representing a single real estate listing. "
                "The input may have nested fields (e.g., 'address' as an object with 'country', 'region', 'city', 'district'). "
                "Flatten the 'address' field into a single string (e.g., 'district, city, region, country') if it's an object. "
                "Ensure all required fields are present and valid: address (string), price (float), currency (string), "
                "bedrooms (float), bathrooms (float), listing_type ('rent' or 'buy'), property_type (string), "
                "description (string), image_link (string). Optional fields: square_footage (integer), year_built (integer), "
                "amenities (array of strings), additional_info (object). If required fields are missing or invalid, "
                "return an empty object {}. Standardize formats (e.g., price as float, remove extra text). "
                "Return a cleaned JSON object compatible with this structure."
            )},
            {"role": "user", "content": f"Clean this JSON listing:\n\n{listing_json}"}
        ]
    )
    return response.choices[0].message.content

async def clean_all_listings(input_file: str, delay_seconds=0.0) -> List[PropertyListing]:
    """Clean raw listings one by one using OpenAI LLM asynchronously"""
    logger.info(f"Cleaning listings from {input_file}")
    try:
        with open(input_file, "r", encoding="utf-8") as f:
            raw_listings = json.load(f)
        
        if not isinstance(raw_listings, list):
            logger.error(f"Input file {input_file} must contain a JSON array")
            return []

        cleaned_listings = []
        for i, listing in enumerate(raw_listings):
            try:
                if i > 0 and delay_seconds > 0:
                    logger.info(f"Waiting {delay_seconds} seconds before processing listing {i+1}...")
                    await asyncio.sleep(delay_seconds)

                listing_json = json.dumps(listing, indent=2)
                response_text = await clean_single_listing_async(listing_json)
                
                json_match = re.search(r'```json\s*([\s\S]*?)\s*```', response_text)
                cleaned_data = json.loads(json_match.group(1)) if json_match else json.loads(response_text)
                
                if cleaned_data:  # Only add non-empty objects
                    cleaned_listing = PropertyListing(**cleaned_data)
                    cleaned_listings.append(cleaned_listing)
                    logger.info(f"Cleaned listing {i+1}: {cleaned_listing.address or 'Unknown address'}")
                else:
                    logger.warning(f"Listing {i+1} discarded: incomplete or invalid data")
            except Exception as e:
                logger.warning(f"Failed to clean listing {i+1}: {str(e)}")
        
        logger.info(f"Cleaned {len(cleaned_listings)} out of {len(raw_listings)} listings")
        return cleaned_listings
    except Exception as e:
        if "insufficient_quota" in str(e):
            logger.error(f"OpenAI quota exceeded: {str(e)}. Check your plan at https://platform.openai.com.")
        else:
            logger.error(f"Failed to clean {input_file}: {str(e)}")
        return []

def clean_all_listings_fully_sync(input_file: str, delay_seconds=0.0) -> List[PropertyListing]:
    """Fully synchronous version of clean_all_listings, processing listings one by one"""
    logger.info(f"Cleaning listings from {input_file}")
    try:
        with open(input_file, "r", encoding="utf-8") as f:
            raw_listings = json.load(f)
        
        if not isinstance(raw_listings, list):
            logger.error(f"Input file {input_file} must contain a JSON array")
            return []

        cleaned_listings = []
        for i, listing in enumerate(raw_listings):
            try:
                if i > 0 and delay_seconds > 0:
                    logger.info(f"Waiting {delay_seconds} seconds before processing listing {i+1}...")
                    time.sleep(delay_seconds)

                listing_json = json.dumps(listing, indent=2)
                response_text = clean_single_listing_sync(listing_json)
                
                json_match = re.search(r'```json\s*([\s\S]*?)\s*```', response_text)
                cleaned_data = json.loads(json_match.group(1)) if json_match else json.loads(response_text)
                
                if cleaned_data:  # Only add non-empty objects
                    cleaned_listing = PropertyListing(**cleaned_data)
                    cleaned_listings.append(cleaned_listing)
                    logger.info(f"Cleaned listing {i+1}: {cleaned_listing.address or 'Unknown address'}")
                else:
                    logger.warning(f"Listing {i+1} discarded: incomplete or invalid data")
            except Exception as e:
                logger.warning(f"Failed to clean listing {i+1}: {str(e)}")
        
        logger.info(f"Cleaned {len(cleaned_listings)} out of {len(raw_listings)} listings")
        return cleaned_listings
    except Exception as e:
        if "insufficient_quota" in str(e):
            logger.error(f"OpenAI quota exceeded: {str(e)}. Check your plan at https://platform.openai.com.")
        else:
            logger.error(f"Failed to clean {input_file}: {str(e)}")
        return []

def clean_all_listings_sync(input_file: str, output_file: str, delay_seconds=0.0) -> int:
    """Synchronous wrapper for cleaning"""
    use_sync = os.getenv("USE_SYNC", "false").lower() == "true"
    
    if use_sync:
        cleaned_listings = clean_all_listings_fully_sync(input_file, delay_seconds)
    else:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        cleaned_listings = loop.run_until_complete(clean_all_listings(input_file, delay_seconds))
        loop.close()
    
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump([listing.dict() for listing in cleaned_listings], f, indent=2)
    logger.info(f"Saved cleaned data to {output_file}")
    return len(cleaned_listings)

if __name__ == "__main__":
    clean_all_listings_sync("raw_property_listings_test.json", "cleaned_property_listings_test.json")