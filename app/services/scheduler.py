from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from .data_acquisition import DataAcquisition
import asyncio
import pytz
import logging
import sys
from datetime import datetime, timedelta

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('data_acquisition_scheduler')

# Get API key from environment
ALPHA_VANTAGE_KEY = "1TGM5D84GWXOA3VJ"

async def run_acquisition_job(days=1):
    """Wrapper function to run the acquisition job with error handling"""
    try:
        logger.info(f"Starting data acquisition job for {days} days")
        acquisition = DataAcquisition(api_key=ALPHA_VANTAGE_KEY)
        await acquisition.run_acquisition(days=days)
        logger.info("Data acquisition job completed successfully")
    except Exception as e:
        logger.error(f"Error in data acquisition job: {str(e)}", exc_info=True)

async def initialize_data():
    """Initial data backfill for 30 days"""
    logger.info("Starting initial data backfill")
    await run_acquisition_job(days=30)
    logger.info("Initial data backfill completed")

async def main():
    # Check if initialization mode is requested
    initialize = len(sys.argv) > 1 and sys.argv[1] == '--initialize'

    # Create scheduler with timezone awareness
    scheduler = AsyncIOScheduler(timezone=pytz.timezone('America/New_York'))

    if initialize:
        logger.info("Running in initialization mode")
        # Run initial backfill immediately
        await initialize_data()

    # Schedule the daily data acquisition to run at 4:30 AM Eastern Time
    scheduler.add_job(
        run_acquisition_job,
        CronTrigger(
            hour=4,
            minute=30,
            timezone=pytz.timezone('America/New_York')
        ),
        id='daily_acquisition',
        name='Daily stock data acquisition',
        coalesce=True,
        max_instances=1,
        misfire_grace_time=3600
    )

    logger.info("Starting scheduler")
    scheduler.start()
    try:
        # Keep the scheduler running
        await asyncio.get_event_loop().create_future()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Shutting down scheduler")
        scheduler.shutdown()

if __name__ == "__main__":
    asyncio.run(main())
