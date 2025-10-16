import json
from collections import defaultdict

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from datetime import date, timedelta
from app.cache.cache import RedisCache

class IndexCalculator:
    def __init__(self, session: AsyncSession, cache: RedisCache):
        self.session = session
        self.cache = cache

    async def ensure_cache_ready(self):
        """Ensure Redis cache is connected and ready"""
        await self.cache.ensure_connected()

    async def read_from_cache(self, cache_key: str, as_json=True):
        """
        Helper method to read and decode data from cache
        Returns tuple of (success, value)
        """
        try:
            cached_value = await self.cache.get(cache_key)
            if cached_value is None:
                return False, None
            if as_json:
                if isinstance(cached_value, (str, bytes)):
                    return True, json.loads(cached_value.decode('utf-8') if isinstance(cached_value, bytes) else cached_value)
                return True, cached_value  # If it's already a dictionary
            else:
                # For non-JSON values (like daily returns that are strings)
                if isinstance(cached_value, (str, bytes)):
                    value = cached_value.decode('utf-8') if isinstance(cached_value, bytes) else cached_value
                    return True, value
                return True, cached_value
        except (json.JSONDecodeError, AttributeError, ValueError) as e:
            print(f"Error reading from cache for key {cache_key}: {e}")
            return False, None

    async def write_to_cache(self, cache_key: str, value, expire=86400, as_json=True):
        """
        Helper method to write data to cache with consistent serialization
        """
        try:
            if as_json:
                # If value is already a string, don't double-encode
                if not isinstance(value, str):
                    value = json.dumps(value)
            else:
                # For non-JSON values (like daily returns), convert to string
                value = str(value)
            await self.cache.set(cache_key, value, expire=expire)
            return True
        except Exception as e:
            print(f"Error writing to cache for key {cache_key}: {e}")
            return False

    async def execute_query(self, query, params=None):
        """Execute a SQL query and return results"""
        try:
            statement = text(query)
            result = await self.session.execute(statement, params or {})
            return result
        except Exception as e:
            print(f"An error occurred during query execution: {e}")
            return []

    async def build_index(self, start_date: date, end_date: date):
        """Build index for date range using stored data"""
        await self.ensure_cache_ready()

        previous_date = start_date - timedelta(days=1)
        current_date = start_date
        jsonResult = {"daily_index_returns": {}, "cumulative_index_returns": ""}
        index_dates = []
        index_returns = []
        cumulative_index_return = 1.0

        while current_date <= end_date:
            if(current_date.weekday() > 5):
                continue
            # Check if daily return exists in cache
            daily_return_key = self.cache.build_single_date_key('daily_return', current_date)
            success, cached_daily_return = await self.read_from_cache(daily_return_key, as_json=False)

            if success and cached_daily_return is not None:
                try:
                    # Use cached daily return
                    index_return = float(cached_daily_return)
                    index_dates.append(current_date)
                    index_returns.append(index_return)
                    cumulative_index_return *= (1 + index_return)
                    print(f"Using cached daily return for {current_date}: {index_return}")
                except ValueError:
                    # If we can't convert to float, calculate from database
                    cached_daily_return = None

            if not success or cached_daily_return is None:
                # Calculate daily return from database
                query_string = """
                           WITH FixedTop100 AS (SELECT symbol 
                                                FROM daily_data 
                                                WHERE date = :previous_date 
                           ORDER BY market_cap DESC LIMIT 100
                               ), TwoDayPrices AS (
                           SELECT t.symbol, t.date, t.price 
                           FROM daily_data AS t JOIN FixedTop100 AS f 
                           ON t.symbol = f.symbol 
                           WHERE t.date IN (:previous_date, :current_date)
                               ), DailyReturns AS (
                           SELECT symbol, date, (price / LAG(price, 1) OVER (PARTITION BY symbol ORDER BY date)) - 1 AS daily_return 
                           FROM TwoDayPrices
                               )
                           SELECT date, AVG (daily_return) AS equal_weighted_index_return 
                           FROM DailyReturns 
                           WHERE date = :current_date 
                           GROUP BY date 
                           ORDER BY date;"""
                try:
                    result = await self.execute_query(query_string, {
                        'previous_date': previous_date,
                        'current_date': current_date
                    })
                    row = result.fetchone()
                    if row:
                        index_return = float(row.equal_weighted_index_return)
                        index_dates.append(current_date)
                        index_returns.append(index_return)
                        cumulative_index_return *= (1 + index_return)
                        # Cache the daily return with a longer expiration (1 day)
                        await self.write_to_cache(daily_return_key, index_return, expire=86400, as_json=False)
                        print(f"Calculated and cached daily return for {current_date}: {index_return}")
                    else:
                        print(f"No results returned for {current_date}")

                except Exception as e:
                    print(f"An error occurred: {e}")

            previous_date = current_date
            current_date += timedelta(days=1)

        daily_returns_dict = {
            str(date): return_value for date, return_value in zip(index_dates, index_returns)
        }
        cumulative_index_return = cumulative_index_return -1
        jsonResult["daily_index_returns"] = daily_returns_dict
        jsonResult["cumulative_index_returns"] = cumulative_index_return
        cumulative_key = self.cache.build_key("cumulative_return", start_date, end_date)
        # Cache the cumulative return for this date range with a longer expiration (1 day)
        await self.write_to_cache(cumulative_key, str(cumulative_index_return), expire=86400, as_json=False)

        # Store in database
        records_to_insert = [
            {"date": date, "daily_return": return_value}
            for date, return_value in zip(index_dates, index_returns)
        ]

        sql_insert_query = """INSERT OR REPLACE INTO index_performance (date, daily_return)
                                VALUES (:date, :daily_return);"""
        await self.execute_query(sql_insert_query, records_to_insert)
        await self.session.commit()

        return jsonResult

    async def get_performance(self, start_date: date, end_date: date):
        """Get cached performance or calculate from database"""
        await self.ensure_cache_ready()

        # Check if we have the complete range result cached
        range_cache_key = f"index_perf:range:{start_date.isoformat()}:{end_date.isoformat()}"
        success, cached_range = await self.read_from_cache(range_cache_key)
        if success:
            return cached_range

        cache_prefix = "index_perf:daily:"
        current_date = start_date
        daily_returns = {}
        missing_dates = []

        while current_date <= end_date:
            daily_cache_key = f"{cache_prefix}{current_date.isoformat()}"
            success, cached_daily = await self.read_from_cache(daily_cache_key, as_json=False)

            if success:
                try:
                    daily_return = float(cached_daily)
                    daily_returns[current_date.isoformat()] = daily_return
                except ValueError:
                    missing_dates.append(current_date.isoformat())
            else:
                missing_dates.append(current_date.isoformat())

            current_date += timedelta(days=1)

        # If there are missing dates, fetch them from database
        if missing_dates:
            # Get a new database session
            try:
                # Build the IN clause parameters
                placeholders = ','.join("'" + date + "'" for date in missing_dates)

                # Direct SQL query for missing dates
                query = f"""
                    SELECT date, daily_return
                    FROM index_performance
                    WHERE date IN ({placeholders})
                    ORDER BY date
                """

                result = await self.execute_query(query)
                db_performances = result.fetchall()

                # Add fetched returns to daily_returns and cache them
                for perf in db_performances:
                    date_str = str(perf.date)  # Convert to string in case it's a different type
                    daily_return = float(perf.daily_return)
                    daily_returns[date_str] = daily_return

                    # Cache individual daily returns
                    daily_cache_key = f"{cache_prefix}{date_str}"
                    await self.write_to_cache(daily_cache_key, daily_return, expire=86400 * 30, as_json=False)
            except Exception as e:
                print(f"Error fetching performance data: {e}")

        # Calculate cumulative return
        cumulative_return = 1.0
        for date_str in sorted(daily_returns.keys()):
            cumulative_return *= (1 + daily_returns[date_str])

        cumulative_return = cumulative_return - 1  # Convert to percentage change

        # Create the final result
        performance_result = {
            "daily_returns": daily_returns,
            "cumulative_return": cumulative_return
        }

        # Cache the complete range result
        await self.write_to_cache(range_cache_key, performance_result, expire=86400)
        return performance_result

    async def get_composition_changes(self, start_date: date, end_date: date):
        current_date = start_date
        all_changes = defaultdict(dict)

        try:
            while current_date <= end_date:
                previous_date = current_date - timedelta(days=1)
                redis_key = f"composition_changes:{current_date.isoformat()}"

                success, cached_result = await self.read_from_cache(redis_key)
                if success:
                    if cached_result.get('added') or cached_result.get('removed'):
                        all_changes[current_date.isoformat()] = cached_result
                    current_date += timedelta(days=1)
                    continue

                # Get current date composition
                curr_query = """
                    SELECT symbol 
                    FROM index_composition
                    WHERE date = :date
                    ORDER BY symbol
                """

                curr_result = await self.execute_query(curr_query, {"date": current_date})
                current_symbols = {row[0] for row in curr_result.fetchall()}

                # Get previous date composition
                prev_query = """
                    SELECT symbol
                    FROM index_composition
                    WHERE date = :date
                    ORDER BY symbol
                """

                prev_result = await self.execute_query(prev_query, {"date": previous_date})
                previous_symbols = {row[0] for row in prev_result.fetchall()}

                # Calculate changes
                added_stocks = list(current_symbols - previous_symbols)
                removed_stocks = list(previous_symbols - current_symbols)

                # Store changes if any exist
                changes = {
                    "added": sorted(added_stocks),
                    "removed": sorted(removed_stocks)
                }

                # Cache the results for this date
                await self.write_to_cache(redis_key, changes, expire=86400 * 30)

                # Only add to all_changes if there were actual changes
                if added_stocks or removed_stocks:
                    all_changes[current_date.isoformat()] = changes

                current_date += timedelta(days=1)
            return all_changes

        except Exception as e:
            print(f"Error in composition changes: {e}")
            raise e

    async def get_composition_for_date(self, current_date: date):
        try:
            redis_key = f"top100_stocks:{current_date.isoformat()}"
            success, cached_composition = await self.read_from_cache(redis_key)
            if success and cached_composition:
                return cached_composition

            # More comprehensive query to get useful stock information
            query = """
                SELECT ic.symbol, s.company_name, s.sector, dd.price, dd.market_cap
                FROM index_composition ic
                LEFT JOIN stocks s ON ic.symbol = s.symbol
                LEFT JOIN daily_data dd ON ic.symbol = dd.symbol AND ic.date = dd.date
                WHERE ic.date = :date
                ORDER BY dd.market_cap DESC
            """

            result = await self.execute_query(query, {"date": current_date})
            rows = result.fetchall()

            composition = []
            for row in rows:
                composition.append({
                    "symbol": row.symbol,
                    "name": row.company_name if row.company_name else "",
                    "category": row.sector if row.sector else "",
                    "price": float(row.price) if row.price else 0.0,
                    "market_cap": float(row.market_cap) if row.market_cap else 0.0
                })

            # Cache the composition list
            if composition:
                await self.write_to_cache(redis_key, composition, expire=86400 * 30)

            return composition

        except Exception as e:
            print(f"Error in get_composition_for_date: {e}")
            raise e
