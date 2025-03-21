import asyncio
import os
import re
import json
from typing import List, Optional, Dict, Any
from datetime import datetime
from crawl4ai import AsyncWebCrawler
from crawl4ai.deep_crawling import BestFirstCrawlingStrategy
from crawl4ai.async_configs import BrowserConfig, CrawlerRunConfig
from openai import AsyncOpenAI
import tiktoken
from pydantic import BaseModel, Field, field_validator
import tkinter as tk
from tkinter import ttk, messagebox
import threading
import sys
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Set up OpenAI client
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
client = AsyncOpenAI(api_key=OPENAI_API_KEY)

# Initialize tokenizer
tokenizer = tiktoken.encoding_for_model("gpt-3.5-turbo")

# Set ProactorEventLoop for Windows compatibility with Playwright
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

class PropertyListing(BaseModel):
    address: Optional[str] = None
    price: Optional[float] = None
    currency: Optional[str] = None
    bedrooms: Optional[float] = None
    bathrooms: Optional[float] = None
    square_footage: Optional[int] = None
    property_type: Optional[str] = None
    listing_type: Optional[str] = None
    year_built: Optional[int] = None
    description: Optional[str] = None
    amenities: List[str] = Field(default_factory=list)
    url: Optional[str] = None
    source: Optional[str] = None
    listing_date: Optional[str] = None
    image_link: Optional[str] = None
    additional_info: Dict[str, Any] = Field(default_factory=dict)

    @field_validator('price', mode='before')
    def validate_price(cls, v):
        if v is None:
            return None
        if isinstance(v, (int, float)):
            return float(v)
        if isinstance(v, str):
            cleaned = re.sub(r'[^\d.]', '', v)
            return float(cleaned) if cleaned else None
        return None

    @field_validator('currency', mode='before')
    def validate_currency(cls, v, info):
        values = info.data
        if v is not None:
            v = v.upper()
            valid_currencies = {'USD', 'MXN', 'EUR', 'CAD'}
            if v in valid_currencies:
                return v
        price_str = values.get('price', '') if isinstance(values.get('price'), str) else ''
        if isinstance(price_str, str):
            if '$' in price_str and 'MXN' not in price_str:
                return 'USD'
            elif 'MXN' in price_str or '$' in price_str:
                return 'MXN'
        return 'MXN'

    @field_validator('listing_type')
    def validate_listing_type(cls, v):
        if v is None:
            return None
        v = v.lower()
        if v in ['rent', 'buy', 'sale', 'rental']:
            return 'rent' if v in ['rent', 'rental'] else 'buy'
        return None

    @field_validator('bedrooms', 'bathrooms', mode='before')
    def validate_rooms(cls, v):
        if v is None:
            return None
        if isinstance(v, (int, float)):
            return float(v)
        if isinstance(v, str):
            match = re.search(r'(\d+(\.\d+)?)', v)
            return float(match.group(1)) if match else None
        return None

    @field_validator('square_footage', mode='before')
    def validate_square_footage(cls, v):
        if v is None:
            return None
        if isinstance(v, (int, float)):
            return int(v)
        if isinstance(v, str):
            match = re.search(r'(\d+)', v.replace(',', ''))
            return int(match.group(1)) if match else None
        return None

    @field_validator('year_built', mode='before')
    def validate_year_built(cls, v):
        if v is None:
            return None
        if isinstance(v, int):
            return v
        if isinstance(v, str):
            match = re.search(r'(\d{4})', v)
            if match:
                year = int(match.group(1))
                current_year = datetime.now().year
                if 1800 <= year <= current_year:
                    return year
            return None
        return None

def clean_text(text):
    text = re.sub(r'<[^>]+>', '', text)
    text = re.sub(r'http\S+', '', text)
    text = re.sub(r'\[.*?\]', '', text)
    return ' '.join(text.split())

def create_smart_chunks(text, max_tokens=4000, overlap_tokens=200):
    cleaned_text = clean_text(text)
    listing_patterns = [
        r'\$[\d,]+', r'\d+ bed', r'\d+ bath', r'\d+\s+sq\s*\.?\s*ft',
        r'\d+\s+[A-Za-z\s]+(Street|St|Avenue|Ave|Road|Rd|Boulevard|Blvd|Drive|Dr|Lane|Ln|Court|Ct|Way|Place|Pl)'
    ]
    boundaries = [0]
    for pattern in listing_patterns:
        for match in re.finditer(pattern, cleaned_text, re.IGNORECASE):
            sentence_start = cleaned_text.rfind('.', 0, match.start())
            sentence_start = 0 if sentence_start == -1 else sentence_start + 1
            if sentence_start not in boundaries:
                boundaries.append(sentence_start)
    boundaries.sort()
    
    chunks = []
    current_start = 0
    current_tokens = 0
    
    for i, boundary in enumerate(boundaries[1:], 1):
        segment = cleaned_text[current_start:boundary].strip()
        segment_tokens = len(tokenizer.encode(segment))
        
        if current_tokens + segment_tokens > max_tokens:
            chunk_end = boundaries[i-1]
            chunks.append(cleaned_text[current_start:chunk_end])
            overlap_start = max(0, chunk_end - overlap_tokens)
            current_start = overlap_start
            current_tokens = len(tokenizer.encode(cleaned_text[overlap_start:boundary]))
        current_tokens += segment_tokens
    
    if current_start < len(cleaned_text):
        chunks.append(cleaned_text[current_start:])
    return chunks

async def extract_housing_info(text):
    chunks = create_smart_chunks(text)
    all_housing_info = []
    print(f"Processing {len(chunks)} chunks")

    for i, chunk in enumerate(chunks):
        try:
            response = await client.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=[
                    {"role": "system", "content": (
                        "You are a strict and precise assistant tasked with extracting reliable real estate listings from text. "
                        "Each listing must be a complete, legitimate property with structured details. "
                        "Incomplete or unclear listings must be discarded. "
                        "Required fields: address, price, currency, bedrooms, bathrooms, listing_type (rent or buy), "
                        "property_type, description, image_link. Optional: square_footage, year_built, amenities, additional_info. "
                        "Return a JSON array of valid listings."
                    )},
                    {"role": "user", "content": (
                        f"Extract real estate listings from the text below in JSON format. "
                        f"Required fields:\n"
                        f"- `address` (string)\n"
                        f"- `price` (float)\n"
                        f"- `currency` (string, e.g., 'USD', 'MXN')\n"
                        f"- `bedrooms` (float)\n"
                        f"- `bathrooms` (float)\n"
                        f"- `listing_type` (string, 'rent' or 'buy')\n"
                        f"- `property_type` (string, e.g., 'house')\n"
                        f"- `description` (string)\n"
                        f"- `image_link` (string, valid URL)\n"
                        f"Optional fields:\n"
                        f"- `square_footage` (integer or null)\n"
                        f"- `year_built` (integer or null)\n"
                        f"- `amenities` (array of strings)\n"
                        f"- `additional_info` (object)\n"
                        f"Discard incomplete listings. Infer currency if needed (e.g., MXN for Mexico). Text:\n\n{chunk}"
                    )}
                ]
            )
            response_text = response.choices[0].message.content
            json_match = re.search(r'```json\s*([\s\S]*?)\s*```', response_text)
            if json_match:
                response_text = json_match.group(1)
            
            chunk_info = json.loads(response_text)
            listings = chunk_info if isinstance(chunk_info, list) else [chunk_info]
            
            for listing_data in listings:
                if isinstance(listing_data, dict):
                    if 'source' not in listing_data:
                        listing_data['source'] = 'Casas y Terrenos'
                    validated_listing = PropertyListing(**listing_data)
                    all_housing_info.append(validated_listing)
                    print(f"Validated listing: {validated_listing.address or 'Unknown address'}")
        except Exception as e:
            print(f"Error processing chunk {i+1}: {str(e)}")
    return all_housing_info

async def scrape_with_playwright_crawl4ai(url, max_depth, max_pages):
    browser_config = BrowserConfig(headless=True)
    run_config = CrawlerRunConfig(
        deep_crawl_strategy=BestFirstCrawlingStrategy(
            max_depth=max_depth,
            include_external=False,
            max_pages=max_pages
        ),
        verbose=True
    )
    async with AsyncWebCrawler(config=browser_config) as crawler:
        results = await crawler.arun(
            url=url,
            config=run_config
        )
        return results  # Returns a list of CrawlResult objects

async def process_scraping(url, max_depth, max_pages):
    print(f"Scraping {url} with max_depth={max_depth}, max_pages={max_pages}...")
    results = await scrape_with_playwright_crawl4ai(url, max_depth, max_pages)
    
    # Combine content from all successful results
    combined_content = ""
    success = False
    for result in results:
        if result.success:
            combined_content += str(result) + "\n"
            success = True
    
    if not success:
        print(f"Failed to scrape {url}")
        return 0
    
    print("Scraped content successfully")
    housing_info = await extract_housing_info(combined_content)
    print(f"Extracted {len(housing_info)} listings")
    
    output_file = f"raw_property_listings_{url_hash(url)}.json"
    json_data = [listing.dict() for listing in housing_info]
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(json_data, f, indent=2)
    print(f"Saved raw data to {output_file}")
    return len(housing_info)

def process_scraping_sync(url, max_depth, max_pages):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    result = loop.run_until_complete(process_scraping(url, max_depth, max_pages))
    loop.close()
    return result

def url_hash(url):
    return re.sub(r'[^a-zA-Z0-9]', '_', url)[:50]

def run_gui():
    from property_pipeline import queue_tasks

    root = tk.Tk()
    root.title("Property Scraper")
    root.geometry("800x500")
    root.configure(bg="#2E2E2E")
    root.resizable(False, False)

    style = ttk.Style()
    style.theme_use("clam")
    style.configure("TLabel", background="#2E2E2E", foreground="#FFFFFF", font=("Helvetica", 12))
    style.configure("TButton", font=("Helvetica", 12, "bold"), padding=10, background="#4CAF50", foreground="#FFFFFF")
    style.map("TButton", background=[("active", "#45A049")])
    style.configure("TEntry", fieldbackground="#424242", foreground="#FFFFFF", font=("Helvetica", 11))

    ttk.Label(root, text="Property Web Scraper", font=("Helvetica", 18, "bold"), foreground="#4CAF50").pack(pady=15)

    frame = ttk.Frame(root, padding="20")
    frame.pack(fill="both", expand=True)

    ttk.Label(frame, text="Websites to Scrape (one per line):").grid(row=0, column=0, padx=10, pady=5, sticky="w")
    url_text = tk.Text(frame, height=5, width=60, bg="#424242", fg="#FFFFFF", insertbackground="white")
    url_text.grid(row=1, column=0, columnspan=2, padx=10, pady=5)
    url_text.insert("1.0", "https://www.casasyterrenos.com/jalisco/Puerto%20Vallarta/casas/venta?desde=0&hasta=5000000")

    ttk.Label(frame, text="Max Depth:").grid(row=2, column=0, padx=10, pady=5, sticky="e")
    depth_entry = ttk.Entry(frame, width=10)
    depth_entry.grid(row=2, column=1, padx=10, pady=5, sticky="w")
    depth_entry.insert(0, "2")

    ttk.Label(frame, text="Max Pages:").grid(row=3, column=0, padx=10, pady=5, sticky="e")
    pages_entry = ttk.Entry(frame, width=10)
    pages_entry.grid(row=3, column=1, padx=10, pady=5, sticky="w")
    pages_entry.insert(0, "5")

    status_label = ttk.Label(frame, text="Ready to scrape", foreground="#FFFFFF", font=("Helvetica", 11, "italic"))
    status_label.grid(row=4, column=0, columnspan=2, pady=10)

    submit_button = ttk.Button(frame, text="Start Scraping", command=lambda: on_submit())
    submit_button.grid(row=5, column=0, columnspan=2, pady=20)

    def on_submit():
        urls = [url.strip() for url in url_text.get("1.0", tk.END).splitlines() if url.strip()]
        try:
            max_depth = int(depth_entry.get().strip())
            max_pages = int(pages_entry.get().strip())
            if not urls or max_depth < 0 or max_pages < 1:
                raise ValueError
        except ValueError:
            messagebox.showerror("Input Error", "Enter valid URLs, non-negative depth, and pages >= 1.")
            return
        
        status_label.config(text=f"Queuing {len(urls)} websites...", foreground="#FFA500")
        submit_button.config(state="disabled")
        
        def queue_and_update():
            queue_tasks(urls, max_depth, max_pages)
            root.after(0, lambda: status_label.config(text=f"Queued {len(urls)} websites!", foreground="green"))
            root.after(0, lambda: submit_button.config(state="normal"))

        thread = threading.Thread(target=queue_and_update)
        thread.start()

    root.mainloop()

if __name__ == "__main__":
    run_gui()