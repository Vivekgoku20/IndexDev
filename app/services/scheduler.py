from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from .data_acquisition import DataAcquisition
import asyncio
import pytz
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('data_acquisition_scheduler')

# Get API key from environment
ALPHA_VANTAGE_KEY = "1TGM5D84GWXOA3VJ"

async def run_acquisition_job():
    print("running for test")
    # """Wrapper function to run the acquisition job with error handling"""
    # try:
    #     logger.info("Starting scheduled data acquisition job")
    #     # Initialize with API key
    #     acquisition = DataAcquisition(api_key=ALPHA_VANTAGE_KEY)
    #     # Run for last 2 days to ensure we have complete data
    #     await acquisition.run_acquisition(days=30)
    #     logger.info("Scheduled data acquisition job completed successfully")
    # except Exception as e:
    #     logger.error(f"Error in scheduled data acquisition job: {str(e)}", exc_info=True)

async def test_job():
    print("hellooo")

async def main():
    # Create scheduler with timezone awareness
    scheduler = AsyncIOScheduler(timezone=pytz.timezone('America/New_York'))

    # Schedule the data acquisition to run at 4:30 AM Eastern Time
    scheduler.add_job(
        test_job,
        CronTrigger(
            hour=0,
            minute=32,
            timezone=pytz.timezone('Asia/Kolkata')
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
        next_run = scheduler.get_job('daily_acquisition').next_run_time
        logger.info(f"Next scheduled run at: {next_run}")
        # Keep the script running
        while True:
            await asyncio.sleep(60)

    except (KeyboardInterrupt, SystemExit):
        logger.info("Shutting down scheduler")
        scheduler.shutdown()
    except Exception as e:
        logger.error(f"Unexpected error in scheduler: {str(e)}", exc_info=True)
        scheduler.shutdown()
        raise

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        print(f"Test failed: {str(e)}")
        raise

