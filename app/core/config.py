import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Boingo API Settings
BOINGO_API_URL = os.getenv("BOINGO_API_URL", "https://api.boingo.com")
BOINGO_EMAIL = os.getenv("BOINGO_EMAIL", "user@example.com")
BOINGO_PASSWORD = os.getenv("BOINGO_PASSWORD", "password123") 