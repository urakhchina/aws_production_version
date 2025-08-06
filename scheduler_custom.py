# scheduler_custom.py

import logging
import time
import datetime
import threading
import os
import sys

# Ensure the project root is in the Python path if needed by your structure
# PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
# if PROJECT_ROOT not in sys.path:
#     sys.path.insert(0, PROJECT_ROOT)

from dotenv import load_dotenv # Will be called in __main__

# Import Flask and application-specific components
from flask import Flask
from models import db # Assuming db is defined in models.py and needs app.init_app()
import config as app_config # Import your config.py (aliased to app_config)
from services.communication_engine import send_all_weekly_digests # Your original, reverted version
from services.email_service import send_email # For the startup sanity check email

# --- Module-level logger instance ---
logger = logging.getLogger(__name__) # When run directly, __name__ is "__main__"

# --- Module-level global variable to hold the Flask app instance ---
# This will be initialized by create_and_setup_flask_app()
flask_app_instance = None

# --- App Factory Function ---
def create_and_setup_flask_app():
    """
    Creates, configures the Flask application, and sets the global flask_app_instance.
    Returns the created app instance or None on failure.
    """
    global flask_app_instance # Indicate we are setting the module-level global

    try:
        current_app = Flask(__name__)
        logger.info("CREATE_APP: Flask app instance created.")

        # Load configurations from the imported app_config module
        logger.info("CREATE_APP: Populating Flask app config from 'app_config' module...")
        for key_conf in dir(app_config):
            if key_conf.isupper() and not key_conf.startswith('_'):
                current_app.config[key_conf] = getattr(app_config, key_conf)
        
        # Log critical configurations that were just loaded into app.config
        logger.info(f"CREATE_APP: app.config.get('DASHBOARD_URL'): '{current_app.config.get('DASHBOARD_URL')}'")
        logger.info(f"CREATE_APP: app.config.get('SQLALCHEMY_DATABASE_URI'): '{current_app.config.get('SQLALCHEMY_DATABASE_URI')}'")
        logger.info(f"CREATE_APP: app.config.get('TEST_MODE'): '{current_app.config.get('TEST_MODE')}'") # Important for email behavior

        # Validate essential configurations
        if not current_app.config.get('SQLALCHEMY_DATABASE_URI'):
            logger.error("CREATE_APP: CRITICAL - SQLALCHEMY_DATABASE_URI is not set in Flask app config. Cannot initialize DB.")
            return None 

        # Initialize extensions
        db.init_app(current_app)
        logger.info("CREATE_APP: db.init_app(app) called.")
        
        # Perform db.create_all() within an app context
        # This is important if models are defined before create_all is called elsewhere
        with current_app.app_context():
            logger.info("CREATE_APP: Attempting db.create_all() within app context...")
            db.create_all() # Safe to call; won't recreate existing tables
            logger.info("CREATE_APP: db.create_all() complete.")

        flask_app_instance = current_app # Assign the successfully created app to the global variable
        logger.info(f"CREATE_APP: Global 'flask_app_instance' has been set to: {flask_app_instance}")
        return flask_app_instance

    except Exception as e_create_app:
        logger.error(f"CREATE_APP: CRITICAL error during Flask app creation or setup: {e_create_app}", exc_info=True)
        return None


# --- Task Trigger Function ---
def run_weekly_digest_task():
    """
    Acquires Flask app context using the global app instance and calls the digest sending function.
    """
    global flask_app_instance # This function will use the module-level global 'flask_app_instance'
    
    logger.info(f"RUN_WEEKLY_DIGEST_TASK: Attempting to use global 'flask_app_instance'. Current value: {flask_app_instance}")

    if not flask_app_instance:
        logger.error("RUN_WEEKLY_DIGEST_TASK: Global Flask app instance ('flask_app_instance') is None. Cannot acquire context.")
        return

    logger.info("RUN_WEEKLY_DIGEST_TASK: Global 'flask_app_instance' is available. Proceeding with app context.")
    try:
        with flask_app_instance.app_context():
            logger.info("RUN_WEEKLY_DIGEST_TASK: App context acquired. Calling send_all_weekly_digests...")
            send_all_weekly_digests() # Your original, reverted function from communication_engine
            logger.info("RUN_WEEKLY_DIGEST_TASK: send_all_weekly_digests function call completed.")
    except Exception as e_task:
        logger.error(f"RUN_WEEKLY_DIGEST_TASK: Error during task execution: {e_task}", exc_info=True)

# --- Main Scheduler Loop ---
def check_schedule():
    """
    Main scheduler loop. Checks time and triggers the weekly task.
    This function runs in a separate thread.
    """
    logger.info("SCHEDULER_THREAD: `check_schedule` function started.")
    last_run_day = None # Keep track of the last day the task ran

    while True:
        try:
            current_local_time = datetime.datetime.now() # Uses server's local time for 'now'
            current_day_of_week = current_local_time.weekday()  # Monday is 0 and Sunday is 6
            current_hour = current_local_time.hour
            current_minute = current_local_time.minute
            current_day = current_local_time.date()

            # --- Define Schedule (TARGET TIMES ARE UTC if server is UTC) ---
            #target_day_of_week = 1  # ADJUST AS NEEDED (0=Mon, 1=Tue, 2=Wed, etc.)
            target_hour = 15         # ADJUST AS NEEDED (0-23 for UTC hour)
            target_minute = 0      # ADJUST AS NEEDED (0-59 for UTC minute)

            #target_day_of_week = 1  # ADJUST AS NEEDED (0=Mon, 1=Tue, 2=Wed, etc.)
            #target_hour = 17         # ADJUST AS NEEDED (0-23 for UTC hour)
            #target_minute = 30      # ADJUST AS NEEDED (0-59 for UTC minute)

                                    

            # logger.debug(f"SCHEDULER_THREAD: Check at {current_local_time.strftime('%Y-%m-%d %H:%M:%S %Z')}. Target: Day={target_day_of_week}, Time={target_hour:02d}:{target_minute:02d} (server local time zone used for comparison logic)")

            if (current_day_of_week == target_day_of_week and
                current_hour == target_hour and
                current_minute == target_minute and
                current_day != last_run_day): 

                logger.info(f"SCHEDULER_THREAD: >>> SCHEDULER TRIGGERED: Day={current_day_of_week}, Time={current_hour:02d}:{current_minute:02d}. Running weekly digest task. <<<")
                run_weekly_digest_task()
                last_run_day = current_day 
                logger.info(f"SCHEDULER_THREAD: Weekly digest task execution initiated for {current_day}. Recorded last run.")
            # else:
                # Optional: log if target matched but already ran for the day
                # if current_day == last_run_day and current_day_of_week == target_day_of_week and current_hour == target_hour and current_minute == target_minute:
                #     logger.debug(f"SCHEDULER_THREAD: Target time matched ({current_hour:02d}:{current_minute:02d}), but task already ran for {current_day}.")
            
            time.sleep(58) # Check just under a minute to be precise for the target minute

        except Exception as e_loop:
            logger.error(f"SCHEDULER_THREAD: Error in check_schedule loop: {str(e_loop)}", exc_info=True)
            time.sleep(60) # Sleep a bit longer after an error

# --- Script Entry Point ---
if __name__ == "__main__":
    # 1. Configure Logging (as early as possible)
    log_level_env = os.environ.get('LOG_LEVEL', 'DEBUG').upper() # Default to DEBUG for thoroughness
    try:
        effective_log_level = getattr(logging, log_level_env)
    except AttributeError:
        print(f"Warning: Invalid LOG_LEVEL '{log_level_env}'. Defaulting to DEBUG.")
        effective_log_level = logging.DEBUG
    
    # Added %(module)s.%(funcName)s to format for better context
    logging.basicConfig(stream=sys.stdout,
                        level=effective_log_level,
                        format='%(asctime)s - %(name)s [%(levelname)s] - %(module)s.%(funcName)s - %(message)s',
                        force=True) # force=True to override any existing handlers on root

    logger.info(f"Logging for scheduler_custom.py direct run initialized. Effective Level: {logging.getLevelName(logger.getEffectiveLevel())}")

    # 2. Load Environment Variables from .env (if present)
    load_dotenv() 
    logger.info(f"SCHEDULER_MAIN: load_dotenv() called. (If .env exists, it may have loaded variables)")

    # Log critical environment variables AFTER load_dotenv
    logger.info(f"SCHEDULER_MAIN: os.environ.get('DASHBOARD_URL') (after load_dotenv): '{os.environ.get('DASHBOARD_URL')}'")
    logger.info(f"SCHEDULER_MAIN: os.environ.get('SQLALCHEMY_DATABASE_URI') (after load_dotenv): '{os.environ.get('SQLALCHEMY_DATABASE_URI')}'")
    logger.info(f"SCHEDULER_MAIN: os.environ.get('TEST_MODE') (after load_dotenv): '{os.environ.get('TEST_MODE')}'")
    # Add any other critical env vars you want to check

    logger.info("--- Starting Custom Scheduler Script (`if __name__ == \"__main__\":` block) ---")

    # 3. Create and Setup the Flask Application using the factory
    # This will initialize and assign to the global 'flask_app_instance'
    if not create_and_setup_flask_app():
        logger.error("SCHEDULER_MAIN: CRITICAL - Failed to create and set up Flask app via factory. Exiting.")
        sys.exit(1)
    
    logger.info(f"SCHEDULER_MAIN: Flask app created and setup via factory. Global 'flask_app_instance' is now: {flask_app_instance}")

    # 4. Perform One-Time Email Sanity Check
    logger.info("SCHEDULER_MAIN: Performing one-time email configuration sanity check...")
    original_test_mode_from_config_module = None # To store the original state of app_config.TEST_MODE
    try:
        # The app's config should reflect the environment or config.py defaults.
        # send_email function reads from the global 'config' module (which is 'app_config' here).
        # We need to ensure app_config.TEST_MODE is what we want for this test.
        
        original_test_mode_from_config_module = app_config.TEST_MODE # Get current state from the config module
        
        # For the sanity check, we want to attempt an actual send, so temporarily set app_config.TEST_MODE to False
        # if it's not already False.
        if app_config.TEST_MODE is not False: # Handles if it's True or None (though it should be True/False from config.py)
            logger.warning(f"SCHEDULER_MAIN (Sanity Check): app_config.TEST_MODE is '{app_config.TEST_MODE}'. Temporarily setting to False for this send.")
            app_config.TEST_MODE = False
        else:
            logger.info(f"SCHEDULER_MAIN (Sanity Check): app_config.TEST_MODE is already False. Proceeding with actual send attempt.")

        test_subject = f"Scheduler Startup Test - {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        test_body_parts = [
            f"The scheduler_custom.py script for {os.environ.get('EB_ENVIRONMENT_NAME', 'Unknown Environment')} started successfully.",
            f"Using settings from 'app_config' module (which send_email uses):",
            f"Server: {app_config.SMTP_SERVER}", # Read directly from app_config
            f"Port: {app_config.SMTP_PORT}",
            f"Username: {app_config.EMAIL_USERNAME}",
            f"From: {app_config.FROM_EMAIL}",
            f"Actual TEST_MODE for this email (from app_config module after potential override): {app_config.TEST_MODE}"
        ]
        test_body = "\n".join(test_body_parts)
        test_recipient = "natasha@quantumgravityresearch.org" # Your test recipient

        logger.info(f"SCHEDULER_MAIN (Sanity Check): Sending startup test email to {test_recipient}...")
        
        success = send_email(subject=test_subject, body=test_body, recipient=test_recipient) # send_email reads from the 'config' module

        if success:
            logger.info(f"SCHEDULER_MAIN (Sanity Check): Startup test email to {test_recipient} reported as sent successfully by send_email function.")
        else:
            logger.warning(f"SCHEDULER_MAIN (Sanity Check): Startup test email to {test_recipient} FAILED according to send_email.")
            
    except Exception as test_email_err:
        logger.error(f"SCHEDULER_MAIN (Sanity Check): Error during startup email test block: {test_email_err}", exc_info=True)
    finally:
        # Restore app_config.TEST_MODE to its original state if it was changed
        if original_test_mode_from_config_module is not None and \
           hasattr(app_config, 'TEST_MODE') and \
           app_config.TEST_MODE != original_test_mode_from_config_module:
            app_config.TEST_MODE = original_test_mode_from_config_module
            logger.info(f"SCHEDULER_MAIN (Sanity Check): Restored app_config.TEST_MODE to {original_test_mode_from_config_module}")
    logger.info("SCHEDULER_MAIN: Email sanity check finished.")

    # 5. Start the Scheduler Thread
    logger.info("SCHEDULER_MAIN: Starting scheduler check thread (target: check_schedule function)...")
    checker_thread = threading.Thread(target=check_schedule, name="SchedulerCheckThread", daemon=True)
    checker_thread.start()
    logger.info("SCHEDULER_MAIN: Scheduler check thread has been started and is running in the background.")

    logger.info("SCHEDULER_MAIN: Scheduler main script setup complete. Main thread will now sleep indefinitely.")
    logger.info("SCHEDULER_MAIN: Press CTRL+C to exit if running this script manually and interactively.")
    try:
        while True:
            time.sleep(3600) # Sleep for an hour, then loop again.
            logger.debug("SCHEDULER_MAIN: Main scheduler thread still alive (hourly heartbeat).")
    except (KeyboardInterrupt, SystemExit):
        logger.info("SCHEDULER_MAIN: Scheduler shutdown requested via main thread (KeyboardInterrupt/SystemExit).")
    finally:
        logger.info("SCHEDULER_MAIN: Scheduler main thread exiting.")