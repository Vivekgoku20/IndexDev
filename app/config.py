import os
import redis.asyncio as redis
from dotenv import load_dotenv

load_dotenv()

# Redis configuration
REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
REDIS_DB = int(os.getenv('REDIS_DB', 0))

# Database configuration
DATABASE_URL = os.getenv('DATABASE_URL', 'sqlite+aiosqlite:///./stocks.db')

# Alpha Vantage API key
API_KEY_ALPHA_VANTAGE = "1TGM5D84GWXOA3VJ"
if not API_KEY_ALPHA_VANTAGE:
    raise ValueError("Alpha Vantage API key not found in environment variables")

# Create Redis client with connection pooling and retry
redis_client = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    db=REDIS_DB,
    decode_responses=True,  # Automatically decode responses to strings
    retry_on_timeout=True,  # Retry on timeout
    socket_keepalive=True,  # Keep connection alive
    health_check_interval=30  # Check connection health every 30 seconds
)
