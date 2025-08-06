# scheduler_custom.py

import logging
import time
import datetime # Make sure datetime is imported
import threading
import os
import sys

from dotenv import load_dotenv

from flask import Flask
from models import db
import config as app_config
from services.communication_engine import send_all_weekly_digests
from services.email_service import send_email

logger = logging.getLogger(__name__)
flask_app_instance = None

def create_and_setup_flask_app():
    # This function remains unchanged...
    global flask_app_instance
    try:
        current_app = Flask(__name__)
        logger.info("CREATE_APP: Flask app instance created.")
        for key_conf in dir(app_config):
            if key_conf.isupper() and not key_conf.startswith('_'):
                current_app.config[key_conf] = getattr(app_config, key_conf)
        if not current_app.config.get('SQLALCHEMY_DATABASE_URI'):
            logger.error("CREATE_APP: CRITICAL - SQLALCHEMY_DATABASE_URI is not set.")
            return None
        db.init_app(current_app)
        with current_app.app_context():
            db.create_all()
        flask_app_instance = current_app
        return flask_app_instance
    except Exception as e_create_app:
        logger.error(f"CREATE_APP: CRITICAL error: {e_create_app}", exc_info=True)
        return None

def run_weekly_digest_task():
    # This function remains unchanged...
    global flask_app_instance
    if not flask_app_instance:
        logger.error("RUN_WEEKLY_DIGEST_TASK: Flask app instance is None.")
        return
    try:
        with flask_app_instance.app_context():
            logger.info("RUN_WEEKLY_DIGEST_TASK: Calling send_all_weekly_digests...")
            send_all_weekly_digests()
            logger.info("RUN_WEEKLY_DIGEST_TASK: send_all_weekly_digests completed.")
    except Exception as e_task:
        logger.error(f"RUN_WEEKLY_DIGEST_TASK: Error during task execution: {e_task}", exc_info=True)

def check_schedule():
    """
    Main scheduler loop. MODIFIED FOR A ONE-TIME TEST.
    """
    logger.info("SCHEDULER_THREAD: `check_schedule` function started.")
    last_run_day = None

    # --- DATE MOCKING FOR WEEK 2 TEST ---
    original_datetime_class = datetime.datetime
    DAY_TO_SIMULATE_FOR_WEEK_2 = 10 # Any day between 8 and 14
    class MockDateTime(datetime.datetime):
        @classmethod
        def now(cls, tz=None):
            # Force the date to be the 10th of the current month/year
            current_real_year = original_datetime_class.now().year
            current_real_month = original_datetime_class.now().month
            return original_datetime_class(current_real_year, current_real_month, DAY_TO_SIMULATE_FOR_WEEK_2, tzinfo=tz)
    datetime.datetime = MockDateTime # Apply the mock
    logger.warning(f"DATE IS MOCKED! The script will think it is {datetime.datetime.now().date()} to generate a Week 2 report.")
    # --- END OF MOCKING ---


    while True:
        try:
            current_local_time = original_datetime_class.now() # Use original datetime for the real-time check
            current_day_of_week = current_local_time.weekday()
            current_hour = current_local_time.hour
            current_minute = current_local_time.minute
            current_day = current_local_time.date()

            # --- DEFINE TEST SCHEDULE (Set to a few minutes from now in UTC) ---
            # To run now, find current UTC time and set this target just ahead.
            # Example: If it's 11:30 AM PDT, it's 18:30 UTC. Set target to 18:35 UTC.
            target_day_of_week = 2  # Wednesday (0=Mon, 1=Tue, 2=Wed)
            target_hour = 18        # SET TO CURRENT UTC HOUR
            target_minute = 35    # SET TO A FEW MINUTES FROM NOW (UTC)

            logger.debug(f"SCHEDULER_THREAD: Check at {current_local_time.strftime('%Y-%m-%d %H:%M:%S')}. Target: Day={target_day_of_week}, Time={target_hour:02d}:{target_minute:02d} UTC")

            if (current_day_of_week == target_day_of_week and
                current_hour == target_hour and
                current_minute == target_minute and
                current_day != last_run_day):

                logger.info(f">>> SCHEDULER TRIGGERED FOR TEST <<<")
                run_weekly_digest_task()
                last_run_day = current_day
                logger.info("Test task finished. Exiting scheduler loop.")
                break # Exit the loop after running the test once

            time.sleep(58)

        except Exception as e_loop:
            logger.error(f"SCHEDULER_THREAD: Error in loop: {str(e_loop)}", exc_info=True)
            time.sleep(60)

    # Restore original datetime class
    datetime.datetime = original_datetime_class
    logger.info("Original datetime restored. Test finished.")


# The `if __name__ == "__main__"` block remains unchanged
if __name__ == "__main__":
    # --- This block remains unchanged ---
    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG, format='%(asctime)s - %(name)s [%(levelname)s] - %(module)s.%(funcName)s - %(message)s', force=True)
    load_dotenv()
    logger.info("--- Starting Custom Scheduler Script for a ONE-TIME TEST ---")
    if not create_and_setup_flask_app():
        logger.error("SCHEDULER_MAIN: Failed to create Flask app. Exiting.")
        sys.exit(1)
    
    # The startup sanity check email will still run, which is fine.
    try:
        # Code for the sanity check email remains here...
        pass
    except Exception as test_email_err:
        logger.error(f"Error during startup email test: {test_email_err}", exc_info=True)
    
    logger.info("Starting scheduler check thread...")
    checker_thread = threading.Thread(target=check_schedule, name="SchedulerCheckThread", daemon=True)
    checker_thread.start()
    
    # We can let the main thread join the checker thread to wait for it to finish
    checker_thread.join() 
    logger.info("Scheduler thread has completed. Main thread exiting.")