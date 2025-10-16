from sqlalchemy import MetaData, Table, Column, String, Float, Date, text, inspect, PrimaryKeyConstraint, Integer, DateTime, Enum, ForeignKey, BigInteger
from app.database.database import engine
import asyncio
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('database_init')

async def check_tables_exist(conn):
    """Check which tables exist in the database"""
    def _check_tables(connection):
        inspector = inspect(connection)
        return inspector.get_table_names()

    existing_tables = await conn.run_sync(_check_tables)
    logger.info(f"Found existing tables: {existing_tables}")
    return set(existing_tables)

async def init_db():
    """Initialize the database by creating all necessary tables if they don't exist."""
    logger.info("Starting database initialization")

    metadata = MetaData()

    # Define tables
    tables = {
        'stocks': Table(
            'stocks', metadata,
            Column('symbol', String, primary_key=True),
            Column('company_name', String, nullable=False),
            Column('sector', String),
            Column('created_at', DateTime, default=datetime.utcnow)
        ),
        'daily_data': Table(
            'daily_data', metadata,
            Column('symbol', String, ForeignKey('stocks.symbol'), primary_key=True),
            Column('date', Date, primary_key=True),
            Column('price', Float, nullable=False),
            Column('market_cap', Float, nullable=False),
            Column('volume', BigInteger)
        ),
        'composition_changes': Table(
            'composition_changes', metadata,
            Column('id', Integer, primary_key=True),
            Column('date', Date, nullable=False, index=True),
            Column('symbol', String, ForeignKey('stocks.symbol'), nullable=False),
            Column('change_type', String, nullable=False)
        ),
        'index_composition': Table(
            'index_composition', metadata,
            Column('date', Date, primary_key=True),
            Column('symbol', String, ForeignKey('stocks.symbol'), primary_key=True),
            Column('weight', Float, nullable=False),
            Column('rank', Integer)
        ),
        'index_performance': Table(
            'index_performance', metadata,
            Column('date', Date, primary_key=True),
            Column('daily_return', Float),
            Column('cumulative_return', Float)
        )
    }

    try:
        async with engine.begin() as conn:
            # Check existing tables
            existing_tables = await check_tables_exist(conn)

            # Determine which tables need to be created
            tables_to_create = []
            for table_name, table in tables.items():
                if table_name not in existing_tables:
                    tables_to_create.append(table)
                    logger.info(f"Table {table_name} does not exist, will create")

            if not tables_to_create:
                logger.info("All required tables already exist")
                return

            # Create only the missing tables
            for table in tables_to_create:
                def create_table(connection):
                    metadata.create_all(bind=connection, tables=[table])
                await conn.run_sync(create_table)
                logger.info(f"Created table {table.name}")

        logger.info("Database initialization completed successfully")
    except Exception as e:
        logger.error(f"Error during database initialization: {str(e)}")
        raise

if __name__ == "__main__":
    asyncio.run(init_db())
