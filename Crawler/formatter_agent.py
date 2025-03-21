import asyncio
import json
import logging
import re
import os
from openai import AsyncOpenAI
from crawleragent import PropertyListing
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# OpenAI client
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
client = AsyncOpenAI(api_key=OPENAI_API_KEY)

async def format_all_listings(input_file: str) -> str:
    """Format cleaned listings into an official report using OpenAI LLM"""
    logger.info(f"Formatting listings from {input_file}")
    try:
        with open(input_file, "r", encoding="utf-8") as f:
            cleaned_listings = json.load(f)
        
        json_str = json.dumps(cleaned_listings, indent=2)
        response = await client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": "system", "content": (
                    "You are a formatting assistant. Convert the provided JSON array of real estate listings into a polished, "
                    "official-looking Markdown report. Include a header with title 'Official Property Listings Report', "
                    "generation date, and total listings. For each listing, create a section with: "
                    "title (address or property type), price (formatted with currency), listing type, bedrooms, bathrooms, "
                    "square footage, description, amenities (as a list), image link, and additional info (as a table). "
                    "Return the full Markdown text."
                )},
                {"role": "user", "content": f"Format this JSON into a Markdown report:\n\n{json_str}"}
            ]
        )
        markdown_text = response.choices[0].message.content.strip()
        if markdown_text.startswith("```markdown"):
            markdown_text = re.sub(r'```markdown\s*|\s*```', '', markdown_text)
        logger.info(f"Formatted {len(cleaned_listings)} listings")
        return markdown_text
    except Exception as e:
        logger.error(f"Failed to format {input_file}: {str(e)}")
        return "# Error\n\nFormatting failed."

def format_all_listings_sync(input_file: str, output_file: str):
    """Synchronous wrapper for formatting"""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    markdown_content = loop.run_until_complete(format_all_listings(input_file))
    loop.close()
    
    with open(output_file, "w", encoding="utf-8") as f:
        f.write(markdown_content)
    logger.info(f"Saved formatted report to {output_file}")