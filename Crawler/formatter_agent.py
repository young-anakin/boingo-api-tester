import json
import logging
import re
import os
from openai import OpenAI
from dotenv import load_dotenv
from datetime import datetime
import tiktoken
from openai import AsyncOpenAI

# Load environment variables
load_dotenv()

# Set up OpenAI client
client = AsyncOpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# Initialize tokenizer
tokenizer = tiktoken.encoding_for_model("gpt-3.5-turbo")

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def format_all_listings_fully_sync(input_file: str) -> str:
    """Fully synchronous version of format_all_listings without asyncio"""
    logger.info(f"Formatting listings from {input_file}")
    try:
        with open(input_file, "r", encoding="utf-8") as f:
            cleaned_listings = json.load(f)
        
        json_str = json.dumps(cleaned_listings, indent=2)
        response = client.chat.completions.create(
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

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Format property listings")
    parser.add_argument("input_file", help="Path to the cleaned JSON file")
    args = parser.parse_args()
    
    result = format_all_listings_fully_sync(args.input_file)
    print(result)