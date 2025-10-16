import yfinance as yf
from alpha_vantage.timeseries import TimeSeries
import pandas as pd
import asyncio
from datetime import datetime, timedelta, date
from typing import List, Tuple, Optional

from app.cache.cache import RedisCache
from app.config import redis_client
from app.database.database import get_db_session
from app.database.init_db import init_db
from sqlalchemy import text
import requests
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
import json

class DataAcquisition:
    def __init__(self, api_key: str = None):
        """Initialize the data acquisition service

        Args:
            api_key (str, optional): Alpha Vantage API key. If not provided, will try to get from environment.
        """
        self.api_key = api_key
        self.alpha_vantage = TimeSeries(key=api_key) if api_key else None
        self.cache = RedisCache(redis_client)

        # Setup requests session with retry logic
        self.session = requests.Session()
        retries = Retry(total=4,
                       backoff_factor=0.1,
                       status_forcelist=[500, 502, 503, 504])
        self.session.mount('http://', HTTPAdapter(max_retries=retries))
        self.session.mount('https://', HTTPAdapter(max_retries=retries))

    async def ensure_db_initialized(self):
        """Ensure database tables are created before running acquisition"""
        try:
            await init_db()
            print("Database initialized successfully")
        except Exception as e:
            print(f"Error initializing database: {str(e)}")
            raise

    async def fetch_stocks_for_date(self, target_date: date) -> List[Tuple[str, str, str, float, float]]:
        # If not in cache, fetch from source
        exchanges = {
            'NASDAQ': 'https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqtraded.txt',
            'NYSE': 'https://www.nyse.com/api/quotes/filter',
            'AMEX': 'https://www.nyse.com/api/quotes/filter'
        }

        max_retries = 3
        retry_delay = 5  # seconds

        for attempt in range(max_retries):
            try:
                # Use the session with retry logic
                response = self.session.get(exchanges['NASDAQ'])
                response.raise_for_status()

                # Save content to a temporary file and read with pandas
                with open('temp_nasdaq.txt', 'wb') as f:
                    f.write(response.content)

                nasdaq_df = pd.read_csv('temp_nasdaq.txt', sep='|', dtype=str)

                # Add data validation to filter out invalid entries
                nasdaq_df = nasdaq_df[
                    (nasdaq_df['Symbol'].notna()) &
                    (nasdaq_df['Symbol'].astype(str).str.len() > 0) &
                    (~nasdaq_df['Symbol'].astype(str).str.contains('^[0-9]')) &
                    (nasdaq_df['ETF'] == 'N') &
                    (nasdaq_df['Financial Status'] == 'N') &
                    (nasdaq_df['Test Issue'] == 'N') &
                    (nasdaq_df['Security Name'].notna())
                ]

                # Get only required columns
                nasdaq_stocks = nasdaq_df[['Symbol', 'Security Name', 'Market Category']]

                market_caps = {}
                total_stocks = len(nasdaq_stocks)
                processed = 0

                # Get historical data for all stocks
                for symbol, name, category in nasdaq_stocks.values.tolist():
                    try:
                        processed += 1
                        if processed % 100 == 0:
                            print(f"Processing stock {processed}/{total_stocks}")

                        stock = yf.Ticker(symbol.strip())  # Ensure symbol is clean
                        hist = stock.history(start=target_date, end=target_date + timedelta(days=1))

                        if not hist.empty and 'Close' in hist.columns and 'Volume' in hist.columns:
                            price = hist['Close'].iloc[0]
                            volume = hist['Volume'].iloc[0]

                            # Additional validation for price and volume
                            if price > 0 and volume > 0:
                                market_cap = price * volume
                                market_caps[symbol] = {
                                    'market_cap': market_cap,
                                    'name': name,
                                    'category': category,
                                    'price': price,
                                    'volume': volume
                                }
                    except Exception as e:
                        print(f"Error fetching data for {symbol} on {target_date}: {str(e)}")
                        continue

                # Sort by market cap and get top 100
                top_100 = sorted(
                    market_caps.items(),
                    key=lambda x: x[1]['market_cap'],
                    reverse=True
                )[:100]

                return [
                    (symbol, data['name'], data['category'], data['price'], data['market_cap'])
                    for symbol, data in top_100
                ]

            except Exception as e:
                print(f"Attempt {attempt + 1}/{max_retries} failed: {str(e)}")
                if attempt < max_retries - 1:
                    print(f"Retrying in {retry_delay} seconds...")
                    await asyncio.sleep(retry_delay)
                else:
                    print(f"Error fetching stock listings for {target_date}: {str(e)}")
                    return []

    async def fetch_stock_data(self, symbol: str, target_date: date) -> Optional[Tuple[float, float, float]]:
        """Fetch single stock data for a specific date"""
        try:
            stock = yf.Ticker(symbol)
            hist = stock.history(start=target_date, end=target_date + timedelta(days=1))
            if not hist.empty:
                price = hist['Close'].iloc[0]
                volume = hist['Volume'].iloc[0]
                market_cap = price * volume
                return price, market_cap, volume
            return None
        except Exception as e:
            print(f"Error fetching data for {symbol} on {target_date}: {str(e)}")
            return None

    async def run_acquisition(self, days: int = 30):
        """Main function to run the data acquisition process"""
        # Initialize database tables first
        await self.ensure_db_initialized()

        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=days)
        current_date = start_date
        prev_symbols = set()  # Track previous day's symbols

        print(f"Starting data acquisition from {start_date} to {end_date}")

        while current_date < end_date:
            print(f"\nProcessing date: {current_date}")
            session = await get_db_session()

            try:
                # Get top 100 stocks for the current date
                daily_top_100 = await self.fetch_stocks_for_date(current_date)
                print(f"Found {len(daily_top_100)} stocks for {current_date}")

                # Cache the daily top 100 stocks in Redis
                redis_key = f"top100_stocks:{current_date.isoformat()}"
                cache_data = [
                    {
                        "symbol": symbol,
                        "name": name,
                        "category": category,
                        "price": price,
                        "market_cap": market_cap
                    }
                    for symbol, name, category, price, market_cap in daily_top_100
                ]
                await self.cache.set(redis_key, json.dumps(cache_data), expire = 60 * 60 * 24 * 30)  # 30 days expiry

                # Calculate total market cap for weight calculation
                total_market_cap = sum(market_cap for _, _, _, _, market_cap in daily_top_100)

                # Store metadata for current top 100 stocks
                current_symbols = set()
                for rank, (symbol, name, category, price, market_cap) in enumerate(daily_top_100, 1):
                    current_symbols.add(symbol)
                    try:
                        # Update or insert stock metadata
                        await session.execute(
                            text("""
                                INSERT OR REPLACE INTO stocks (symbol, company_name, sector)
                                VALUES (:symbol, :company_name, :sector)
                            """),
                            {
                                "symbol": symbol,
                                "company_name": name,
                                "sector": category
                            }
                        )

                        # Calculate weight as percentage of total market cap
                        weight = (market_cap / total_market_cap) * 100 if total_market_cap > 0 else 0

                        # Insert into index_composition to track index membership
                        await session.execute(
                            text("""
                                INSERT OR REPLACE INTO index_composition (date, symbol, weight, rank)
                                VALUES (:date, :symbol, :weight, :rank)
                            """),
                            {
                                "date": current_date,
                                "symbol": symbol,
                                "weight": weight,
                                "rank": rank
                            }
                        )
                        print("now inserting into daily data")
                        # Store daily price and market cap data
                        await session.execute(
                            text("""
                                INSERT OR REPLACE INTO daily_data 
                                (symbol, date, price, market_cap, volume)
                                VALUES (:symbol, :date, :price, :market_cap, :volume)
                            """),
                            {
                                "symbol": symbol,
                                "date": current_date,
                                "price": price,
                                "market_cap": market_cap,
                                "volume": market_cap / price if price > 0 else 0
                            }
                        )
                    except Exception as e:
                        print(f"Error storing data for {symbol} on {current_date}: {str(e)}")
                        continue

                # Fetch data for stocks that were in yesterday's top 100 but not today's
                dropped_symbols = prev_symbols - current_symbols
                for symbol in dropped_symbols:
                    print(f"Fetching data for dropped stock {symbol} on {current_date}")
                    data = await self.fetch_stock_data(symbol, current_date)
                    if data:
                        try:
                            price, market_cap, volume = data
                            await session.execute(
                                text("""
                                    INSERT OR REPLACE INTO daily_data 
                                    (symbol, date, price, market_cap, volume)
                                    VALUES (:symbol, :date, :price, :market_cap, :volume)
                                """),
                                {
                                    "symbol": symbol,
                                    "date": current_date,
                                    "price": price,
                                    "market_cap": market_cap,
                                    "volume": volume
                                }
                            )
                        except Exception as e:
                            print(f"Error storing dropped stock data for {symbol} on {current_date}: {str(e)}")
                            continue
                await session.commit()
                # Update previous day's symbols for next iteration
                prev_symbols = current_symbols

            except Exception as e:
                print(f"Error processing date {current_date}: {str(e)}")
                await session.rollback()
            finally:
                await session.close()

            current_date += timedelta(days=1)

        print("\nData acquisition completed successfully")

if __name__ == "__main__":
    acquisition = DataAcquisition()
    asyncio.run(acquisition.run_acquisition())
