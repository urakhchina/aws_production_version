# communication_engine.py

import logging
import datetime # Ensure datetime itself is imported
import json
import sys
import time
import re
from sqlalchemy import select, func 
from sqlalchemy.orm import Session as SQLAlchemySession 
import numpy as np
import pandas as pd
from datetime import date, timedelta # date and timedelta are used
from dateutil.relativedelta import relativedelta # For month calculations


# --- Imports for Models, Config, Email Service ---
try:
    import config
    logging.info("Configuration loaded successfully from config.py")
except ImportError:
    logging.error("Could not import config.py.")
    class DummyConfig: TEST_MODE=True; SALES_REP_MAPPING={}; DASHBOARD_URL="#"; SQLALCHEMY_DATABASE_URI=""; FROM_EMAIL=""; SMTP_SERVER=""; SMTP_PORT=0; EMAIL_USERNAME=""; EMAIL_PASSWORD=""; 
    config = DummyConfig()
    logging.warning("Using dummy configuration.")

try:
    from models import db, AccountPrediction, AccountHistoricalRevenue
    logging.info("Models loaded successfully.")
except ImportError as e:
    logging.warning(f"Could not import models: {e}")
    class MockDB: session = None; Model = object; 
    db = MockDB(); AccountPrediction = object(); AccountHistoricalRevenue = object()

try:
    from services.email_service import send_email, print_email_instead_of_sending
    logging.info("Email service loaded.")
except ImportError as e_email:
    logging.error(f"Could not import email functions: {e_email}")
    def send_email(*args, **kwargs): logger.error("send_email dummy used!"); return False
    def print_email_instead_of_sending(*args, **kwargs): logger.error("print_email dummy used!"); return True

logger = logging.getLogger(__name__)


# === START DEBUGGING CODE FOR DASHBOARD_URL IN COMMUNICATION_ENGINE ===
_env_val_direct_in_comm_engine = os.environ.get('DASHBOARD_URL')
_config_module_val_in_comm_engine = getattr(config, 'DASHBOARD_URL', 'DASHBOARD_URL_NOT_FOUND_IN_CONFIG_MODULE')

logger.info(f"COMM_ENGINE (Top): os.environ.get('DASHBOARD_URL') directly accessible here: '{_env_val_direct_in_comm_engine}'")
logger.info(f"COMM_ENGINE (Top): Value of config.DASHBOARD_URL from imported module: '{_config_module_val_in_comm_engine}'")
# === END DEBUGGING CODE FOR DASHBOARD_URL IN COMMUNICATION_ENGINE ===


# This line already exists or is similar:
DASHBOARD_BASE_URL = getattr(config, 'DASHBOARD_URL', '#') # Fallback to '#' if not in config for some reason

# Add a log for the final value used
logger.info(f"COMM_ENGINE (Top): DASHBOARD_BASE_URL that will be used for email links: '{DASHBOARD_BASE_URL}'")


# --- Use config settings with safe defaults ---
#TEST_MODE = getattr(config, 'TEST_MODE', True)
#DASHBOARD_BASE_URL = getattr(config, 'DASHBOARD_URL', '#')


# --- Helper Functions (Keep formatters) ---
def format_currency(value):
    if value is None or not isinstance(value, (int, float)): return "$0.00"
    try: return f"${value:,.2f}"
    except (TypeError, ValueError): return "$0.00"

def format_currency_short(value):
    if value is None: 
        return '$0'
    try: 
        n = float(value)
    except (ValueError, TypeError): 
        return '$0'
    if not np.isfinite(n): 
        return '$0'
    
    sign = '-' if n < 0 else ''
    abs_n = abs(n)
    
    if abs_n == 0: 
        return '$0'
    if abs_n >= 1e9: 
        return f"{sign}${(abs_n / 1e9):.1f}B"
    if abs_n >= 1e6: 
        return f"{sign}${(abs_n / 1e6):.1f}M"
    if abs_n >= 1e3: 
        return f"{sign}${(abs_n / 1e3):.1f}K"
    return format_currency(n)


# --- Helper to Fetch Previous Year Data (Essential and Unchanged) ---
def get_previous_year_revenue(account_canonical_codes, prev_year):
    """Fetches total_revenue for specific accounts for the specified previous year."""
    if not account_canonical_codes or not prev_year: return {}
    logger.debug(f"DB Query: Fetching PY revenue for {len(account_canonical_codes)} accounts, year {prev_year}")
    revenue_map = {}
    try:
        stmt = select(
            AccountHistoricalRevenue.canonical_code,
            AccountHistoricalRevenue.total_revenue
        ).where(
            AccountHistoricalRevenue.canonical_code.in_(account_canonical_codes),
            AccountHistoricalRevenue.year == prev_year
        )
        results = db.session.execute(stmt).all() 
        for row in results:
            revenue_map[row.canonical_code] = row.total_revenue or 0.0
        logger.debug(f"DB Query: Fetched PY revenue for {len(revenue_map)} accounts.")
    except Exception as e:
        logger.error(f"DB Query Error in get_previous_year_revenue: {e}", exc_info=True)
    return revenue_map


# --- Main Weekly Digest Function (Week 4 Disabled) ---
def send_weekly_digest_email_for_rep(rep_id, rep_name, rep_email):
    """
    Generates and sends the weekly digest email focusing on account pacing
    based on the week of the month (Weeks 1-3 only).
    Week 4 is currently disabled.
    Excludes new accounts with no sales YTD from all reports.
    """
    logger.info(f"Generating Weekly Pacing Digest for Rep: {rep_name} ({rep_id}) Email: {rep_email}")
    if not rep_email:
        logger.error(f"No email address for rep {rep_name} ({rep_id}). Skipping digest.")
        return False

    try:
        # --- Define Time Periods & Pacing Thresholds ---
        today = datetime.datetime.now().date() # Uses potentially mocked datetime.datetime.now()
        current_year = today.year
        prev_year = current_year - 1

        remaining_full_months = 12 - today.month

        PACING_THRESHOLD_SEVERE = -20.0
        PACING_THRESHOLD_MODERATE = -10.0
        PACING_THRESHOLD_MILD = 0.0

        day_of_month = today.day
        week_num_for_title = 0
        section_title = ""
        section_description = ""
        filter_min_pace = 0.0
        filter_max_pace = 0.0
        is_negative_pacing_week = False
        process_this_week = True # Flag to control processing

        if 1 <= day_of_month <= 7: # Week 1
            week_num_for_title = 1
            section_title = f"Accounts Pacing < {PACING_THRESHOLD_SEVERE:.0f}% vs LY"
            section_description = "These accounts are significantly behind last year's pace and require immediate attention."
            filter_min_pace = -float('inf')
            filter_max_pace = PACING_THRESHOLD_SEVERE
            is_negative_pacing_week = True
        elif 8 <= day_of_month <= 14: # Week 2
            week_num_for_title = 2
            section_title = f"Accounts Pacing {PACING_THRESHOLD_SEVERE:.0f}% to < {PACING_THRESHOLD_MODERATE:.0f}% vs LY"
            section_description = "These accounts are moderately behind last year's pace."
            filter_min_pace = PACING_THRESHOLD_SEVERE
            filter_max_pace = PACING_THRESHOLD_MODERATE
            is_negative_pacing_week = True
        elif 15 <= day_of_month <= 21: # Week 3
            week_num_for_title = 3
            section_title = f"Accounts Pacing {PACING_THRESHOLD_MODERATE:.0f}% to < {PACING_THRESHOLD_MILD:.0f}% vs LY"
            section_description = "These accounts are slightly behind or near last year's pace."
            filter_min_pace = PACING_THRESHOLD_MODERATE
            filter_max_pace = PACING_THRESHOLD_MILD
            is_negative_pacing_week = True
        else: # Week 4 (22nd to end of month) - DISABLED
            week_num_for_title = 4 # Still set for logging/quiet email title
            process_this_week = False # Signal to skip processing for Week 4
            logger.info(f"Week 4 ({day_of_month}th day of month) - Pacing report generation is currently disabled for this week.")
            # Optional: Send a specific "no report this week" email for Week 4
            subject_quiet = f"Weekly Account Pacing - Wk {week_num_for_title}, {today.strftime('%B %Y')} (No Report This Week)"
            dashboard_link = f"{DASHBOARD_BASE_URL}/strategic?sales_rep={rep_id}" # Ensure DASHBOARD_BASE_URL is accessible
            body_quiet = f"Hi {rep_name.split()[0]},\n\nThere is no specific account pacing report scheduled for this week (Week {week_num_for_title}).\n\nPlease continue to monitor your accounts via the dashboard.\n\n🚀 View Your Full Dashboard: {dashboard_link}\n\nBest regards,\nThe Sales Intelligence Team"
            email_func = print_email_instead_of_sending if TEST_MODE else send_email # Ensure TEST_MODE and email_func are accessible
            email_func(subject=subject_quiet, body=body_quiet, recipient=rep_email, from_email=config.FROM_EMAIL, smtp_server=config.SMTP_SERVER, smtp_port=config.SMTP_PORT, username=config.EMAIL_USERNAME, password=config.EMAIL_PASSWORD) # Ensure config attributes are accessible
            return True # Successfully handled by sending a "no report" email or by design

        if not process_this_week: # Should be caught by the Week 4 logic above, but as a safeguard
            logger.info(f"Skipping digest generation for rep {rep_name} as process_this_week is False (Likely Week 4).")
            return True # Indicate successful handling (by skipping)

        logger.debug(f"Today (effective): {today}, Week of Month: {week_num_for_title}, Pacing Filter: {filter_min_pace}% to {filter_max_pace}%")

        # === Query ALL Accounts for the Rep ===
        # (This section remains the same)
        logger.debug(f"Querying ALL accounts for Rep ID: {rep_id}")
        all_accounts_stmt = select(AccountPrediction).where(
            AccountPrediction.sales_rep == rep_id
        )
        rep_accounts_all_objects = db.session.execute(all_accounts_stmt).scalars().all()
        logger.info(f"Found {len(rep_accounts_all_objects)} total accounts for {rep_name}.")

        if not rep_accounts_all_objects:
            logger.info(f"No accounts found for rep {rep_name}. Sending quiet week email.")
            subject_quiet = f"Your Weekly Pacing Report - Wk {week_num_for_title}, {today.strftime('%B %Y')}"
            dashboard_link = f"{DASHBOARD_BASE_URL}/strategic?sales_rep={rep_id}"
            body_quiet = f"Hi {rep_name.split()[0]},\n\nNo accounts assigned to you were found in the system for this week's pacing report.\n\n🚀 View Your Dashboard (if applicable): {dashboard_link}\n\nBest regards,\nThe Sales Intelligence Team"
            email_func = print_email_instead_of_sending if TEST_MODE else send_email
            email_func(subject=subject_quiet, body=body_quiet, recipient=rep_email, from_email=config.FROM_EMAIL, smtp_server=config.SMTP_SERVER, smtp_port=config.SMTP_PORT, username=config.EMAIL_USERNAME, password=config.EMAIL_PASSWORD)
            return True


        # === Fetch Previous Year Revenue for ALL Rep Accounts ===
        # (This section remains the same)
        py_revenue_map = {}
        account_codes = [acc.canonical_code for acc in rep_accounts_all_objects if acc.canonical_code]
        if account_codes:
            py_revenue_map = get_previous_year_revenue(account_codes, prev_year)
            logger.debug(f"Fetched PY revenue for {len(py_revenue_map)} accounts.")

        # === Filter Accounts Based on Pacing for the Current Week ===
        # (This section remains the same, but will only be effective for Weeks 1-3)
        email_accounts_for_week = []
        for acc in rep_accounts_all_objects:
            py_rev = py_revenue_map.get(acc.canonical_code, 0.0)
            yep_rev = acc.yep_revenue
            cytd_rev = acc.cytd_revenue

            current_pace_pct_display = "N/A"
            current_pace_pct_numeric = -float('inf')
            is_new_account = (py_rev == 0)

            if is_new_account:
                # New accounts with sales might have been relevant for old Week 4,
                # but since Week 4 is disabled, we just exclude new accounts with no sales.
                if not ((yep_rev is not None and yep_rev > 0) or \
                        (cytd_rev is not None and cytd_rev > 0)): # New, and no sales
                    logger.debug(f"Excluding new account with no sales YTD: {acc.canonical_code}")
                    continue
                else: # New, with some sales. Not relevant for W1-3 negative pacing.
                      # And W4 is disabled. So, effectively, these are also skipped for W1-3.
                    logger.debug(f"Skipping new account with sales {acc.canonical_code} for W1-3.")
                    continue

            elif yep_rev is None: # Existing account, YEP undefined
                current_pace_pct_display = "YEP Undefined"
            elif py_rev > 0: # Existing account with PY revenue
                current_pace_pct_numeric = ((yep_rev / py_rev) - 1) * 100.0
                current_pace_pct_display = f"{current_pace_pct_numeric:+.1f}%"

            # Apply filter logic (only W1, W2, W3 will have conditions met now)
            passes_filter = False
            if week_num_for_title == 1: # < SEVERE
                if current_pace_pct_numeric < filter_max_pace:
                    passes_filter = True
            # Week 4 is handled by process_this_week = False
            # elif week_num_for_title == 4: ...
            elif week_num_for_title == 2 or week_num_for_title == 3:
                if filter_min_pace <= current_pace_pct_numeric < filter_max_pace:
                    passes_filter = True

            if passes_filter:
                amount_needed_str = "N/A"
                target_to_display_str = "N/A"

                if is_negative_pacing_week and not is_new_account: # Should always be true if passes_filter for W1-3
                    if py_rev > 0 and cytd_rev is not None:
                        target_revenue_for_plus_1_pct = py_rev * 1.01
                        amount_needed_raw = target_revenue_for_plus_1_pct - cytd_rev
                        amount_needed_str = format_currency(amount_needed_raw)
                        target_to_display_str = format_currency_short(target_revenue_for_plus_1_pct)

                email_accounts_for_week.append({
                    'obj': acc, 'name': getattr(acc, 'name', 'Unknown Account'),
                    'canonical_code': acc.canonical_code,
                    'base_card_code': getattr(acc, 'base_card_code', 'N/A'),
                    'full_address': getattr(acc, 'full_address', 'Address not available'),
                    'pace_display': current_pace_pct_display,
                    'pace_numeric': current_pace_pct_numeric,
                    'py_rev': py_rev, 'yep_rev': yep_rev, 'cytd_rev': cytd_rev,
                    'amount_needed_str': amount_needed_str,
                    'target_to_display_str': target_to_display_str,
                    'is_new': is_new_account # will be False if it reaches here for W1-3
                })

        # Sort for W1-3
        if is_negative_pacing_week: # This will be true for W1-3
            email_accounts_for_week.sort(key=lambda x: x['pace_numeric'])
        # else: Week 4 sorting not needed as it's disabled

        if not email_accounts_for_week: # No accounts for W1, W2, or W3
            logger.info(f"No accounts fit Week {week_num_for_title} pacing criteria for rep {rep_name} (after exclusions). Sending quiet week email.")
            subject_quiet = f"Your Weekly Pacing Report - Wk {week_num_for_title}, {today.strftime('%B %Y')}"
            dashboard_link = f"{DASHBOARD_BASE_URL}/strategic?sales_rep={rep_id}"
            body_quiet = f"Hi {rep_name.split()[0]},\n\nNo accounts met the criteria for this week's pacing report (Week {week_num_for_title}: {section_title}).\n\n🚀 View Your Full Dashboard: {dashboard_link}\n\nBest regards,\nThe Sales Intelligence Team"
            email_func = print_email_instead_of_sending if TEST_MODE else send_email
            email_func(subject=subject_quiet, body=body_quiet, recipient=rep_email, from_email=config.FROM_EMAIL, smtp_server=config.SMTP_SERVER, smtp_port=config.SMTP_PORT, username=config.EMAIL_USERNAME, password=config.EMAIL_PASSWORD)
            return True

        # === Build Email Body ===
        # (This section remains largely the same, but will only populate for W1-3)
        subject = f"Your Weekly Account Pacing Report - Wk {week_num_for_title}, {today.strftime('%B %Y')}"
        body_lines = [
            f"Hi {rep_name.split()[0]},", "",
            f"Here is your account pacing report for Week {week_num_for_title} of {today.strftime('%B')}.",
            f"🚀 View Your Full Dashboard: {DASHBOARD_BASE_URL}/strategic?sales_rep={rep_id}\n",
            "---",
            f"**{section_title.upper()}**",
            f"*{section_description}*\n"
        ]

        if is_negative_pacing_week and remaining_full_months >= 0: # True for W1-3
            month_str = "month" if remaining_full_months == 1 else "months"
            if remaining_full_months > 0:
                body_lines.append(f"*You have approximately **{remaining_full_months} full {month_str}** remaining in the year to reach targets.*\n")
            elif today.month == 12:
                 body_lines.append(f"*This is the final month to reach targets.*\n")

        display_limit = 20
        for i, acc_data in enumerate(email_accounts_for_week[:display_limit]):
            acc_name = acc_data['name']
            account_detail_link = f"{DASHBOARD_BASE_URL}/account/{acc_data['canonical_code']}"

            body_lines.append(f"**{i+1}. {acc_name}** ([Details]({account_detail_link}))")
            body_lines.append(f"   *   CardCode: {acc_data['base_card_code']}")
            body_lines.append(f"   *   Address: {acc_data['full_address']}")

            body_lines.append(f"   *   Pacing: {acc_data['pace_display']}")
            # acc_data['is_new'] will be False here for W1-3 reports
            body_lines[-1] += f" (YEP: {format_currency_short(acc_data['yep_rev'])} vs PY: {format_currency_short(acc_data['py_rev'])})"

            # This block will only execute if is_negative_pacing_week is true (W1-3)
            # and acc_data['is_new'] is false
            ytd_display = format_currency_short(acc_data['cytd_rev']) if acc_data['cytd_rev'] is not None else "N/A"
            body_lines.append(f"   *   To Reach +1% Pace: **{acc_data['amount_needed_str']}** (Target: {acc_data['target_to_display_str']}, Current YTD: {ytd_display})")
            body_lines.append("")

        if len(email_accounts_for_week) > display_limit:
            filter_param = f"pacing_w{week_num_for_title}"
            body_lines.append(f"*... and {len(email_accounts_for_week) - display_limit} more. [View All on Dashboard]({DASHBOARD_BASE_URL}/strategic?sales_rep={rep_id}&filter={filter_param})*\n")

        body_lines.append("---\nBest regards,\nThe Sales Intelligence Team")
        body = "\n".join(body_lines)

        email_func = print_email_instead_of_sending if TEST_MODE else send_email
        log_prefix = "[TEST MODE] Would print" if TEST_MODE else "Sent"
        try:
            success = email_func(
                subject=subject, body=body, recipient=rep_email,
                from_email=config.FROM_EMAIL, smtp_server=config.SMTP_SERVER,
                smtp_port=config.SMTP_PORT, username=config.EMAIL_USERNAME,
                password=config.EMAIL_PASSWORD
            )
            if success:
                logger.info(f"{log_prefix} weekly pacing digest (Week {week_num_for_title}) to {rep_email} ({len(email_accounts_for_week)} accounts).")
                return True
            else:
                logger.error(f"Email function returned False for weekly pacing digest to {rep_email}.")
                return False
        except Exception as mail_err:
            logger.error(f"Failed to send weekly pacing digest email to {rep_email}: {mail_err}", exc_info=True)
            return False

    except Exception as e:
        logger.error(f"Error generating weekly pacing digest for rep {rep_name} ({rep_id}): {str(e)}", exc_info=True)
        try:
            if db.session.is_active:
                 db.session.rollback()
                 logger.warning("Rolled back DB session due to error during weekly digest generation.")
        except Exception as rb_err:
            logger.error(f"Error during DB rollback attempt: {rb_err}")
        return False


# --- send_all_weekly_digests (Largely unchanged, calls the new digest function) ---
def send_all_weekly_digests():
    """Queries all reps and triggers individual digest emails."""
    logger.info("Starting send_all_weekly_digests (Pacing Focus)...")
    reps_to_email = []
    try:
        stmt = select(
            AccountPrediction.sales_rep,
            AccountPrediction.sales_rep_name
        ).where(
            AccountPrediction.sales_rep.isnot(None),
            AccountPrediction.sales_rep != ''
        ).distinct()
        distinct_reps_rows = db.session.execute(stmt).all()

        for row in distinct_reps_rows:
            rep_id = row.sales_rep
            rep_name = row.sales_rep_name
            if not rep_id or not rep_name: continue
            rep_email = getattr(config, 'SALES_REP_MAPPING', {}).get(rep_name)
            if rep_email: reps_to_email.append({'id': rep_id, 'name': rep_name, 'email': rep_email})
            else: logger.warning(f"No email mapping for {rep_name} (ID: {rep_id})")

        logger.info(f"Found {len(reps_to_email)} assigned reps with emails to process for pacing digest.")
        processed_count = 0; failed_count = 0
        for rep_info in reps_to_email:
            try:
                logger.info(f"Processing pacing digest for {rep_info['name']} ({rep_info['id']})")
                success = send_weekly_digest_email_for_rep(
                    rep_info['id'], rep_info['name'], rep_info['email']
                )
                if success: processed_count += 1
                else: failed_count += 1
                time.sleep(0.5) 
            except Exception as e_inner:
                logger.error(f"Unhandled error processing pacing digest for {rep_info['name']}: {e_inner}", exc_info=True)
                failed_count += 1

        logger.info(f"Pacing digest processing complete. Success: {processed_count}, Failed: {failed_count}, Skipped (no email): {len(distinct_reps_rows) - len(reps_to_email)}")

    except Exception as e_outer:
         logger.error(f"Major error in send_all_weekly_digests (Pacing Focus): {e_outer}", exc_info=True)
    logger.info("Finished send_all_weekly_digests (Pacing Focus).")


# --- Standalone Test Block (Updated for New Pacing Digest) ---
if __name__ == "__main__":
    print("--- Running Communication Engine Standalone Test (New Pacing Digest) ---")

    log_level = logging.DEBUG
    log_formatter = logging.Formatter('%(asctime)s - %(name)s [%(levelname)s] %(message)s')
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level)
    console_handler.setFormatter(log_formatter)

    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)
    if not root_logger.hasHandlers(): 
        root_logger.addHandler(console_handler)
    
    logger.setLevel(log_level) 
    logger.info(f"Configured loggers for DEBUG level.")
    
    try: from flask import Flask
    except ImportError as e: print(f"ERROR: Flask not found: {e}."); exit(1)

    app = Flask(__name__)
    try:
        app.config['SQLALCHEMY_DATABASE_URI'] = getattr(config, 'SQLALCHEMY_DATABASE_URI')
        app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
        # app.config['SQLALCHEMY_ECHO'] = True 
        logger.info(f"Using DB URI: {app.config['SQLALCHEMY_DATABASE_URI']}")
    except AttributeError: print("ERROR: SQLALCHEMY_DATABASE_URI not found in config.py."); exit(1)
    except Exception as config_err: print(f"ERROR loading config: {config_err}"); exit(1)

    try: 
        db.init_app(app)
        print("Database initialized with Flask app.")
    except Exception as db_err: print(f"ERROR initializing database with Flask app: {db_err}"); exit(1)

    effective_test_mode = getattr(config, 'TEST_MODE', True)
    print(f"\n--- TEST MODE: {'ON (Printing Emails)' if effective_test_mode else 'OFF (Attempting SMTP Send)'} ---")

    # --- Test Execution ---
    # TEST_REP_NAME = "Mariano Cruz" 
    TEST_REP_NAME = None 

    with app.app_context():
        print(f"App context established for testing.")
        
        original_datetime_module = datetime.datetime 

        #class MockDateTime(datetime.datetime): 
        #    @classmethod
        #    def now(cls, tz=None):
                # FOR WEEK 1 TEST: (e.g. Jan 3rd)
                #return original_datetime_module(2023, 1, 3, tzinfo=tz)  
                # FOR WEEK 2 TEST: (e.g. Jan 10th)
                #return original_datetime_module(2023, 1, 10, tzinfo=tz) 
                # FOR WEEK 3 TEST: (e.g. Jan 17th)
                #return original_datetime_module(2023, 1, 17, tzinfo=tz) 
                # FOR WEEK 4 TEST: (e.g. Jan 24th)
                #return original_datetime_module(2023, 1, 24, tzinfo=tz) 
        
        #datetime.datetime = MockDateTime 
        #logger.info(f"MOCKING datetime.datetime.now() to return date: {datetime.datetime.now().date()} for testing Week 1 logic.")
        
        if TEST_REP_NAME:
            test_rep_id = None
            test_rep_email = getattr(config, 'SALES_REP_MAPPING', {}).get(TEST_REP_NAME)
            if not test_rep_email: print(f"ERROR: Email not found for '{TEST_REP_NAME}' in config.SALES_REP_MAPPING.")
            else:
                try: 
                    stmt = select(AccountPrediction.sales_rep).where(AccountPrediction.sales_rep_name == TEST_REP_NAME).limit(1)
                    result = db.session.execute(stmt).scalar_one_or_none()
                    if result: test_rep_id = result; print(f"Found Rep ID for {TEST_REP_NAME}: {test_rep_id}")
                    else: print(f"WARNING: Could not find Rep ID for {TEST_REP_NAME} in AccountPrediction table.")
                except Exception as db_err: print(f"ERROR: DB error finding Rep ID for {TEST_REP_NAME}: {db_err}")

                if test_rep_id and test_rep_email:
                    print(f"\n--- Testing SINGLE Rep Pacing Digest: {TEST_REP_NAME} (ID: {test_rep_id}) ---")
                    try:
                        success = send_weekly_digest_email_for_rep(test_rep_id, TEST_REP_NAME, test_rep_email)
                        if success: print(f"\n--- Test for {TEST_REP_NAME} Completed ---")
                        else: print(f"\n--- Test for {TEST_REP_NAME} Indicated Failure ---")
                    except Exception as e: logger.error(f"Exception during single rep pacing digest test: {e}", exc_info=True)
                else: print(f"\n--- Skipping single rep pacing digest test for {TEST_REP_NAME} due to missing ID or Email. ---")
        else:
            print(f"\n--- Testing ALL Reps Pacing Digest ---")
            send_all_weekly_digests()
        
        datetime.datetime = original_datetime_module 
        logger.info("Restored original datetime.datetime class.")

    print("\n--- Standalone Pacing Digest Test Finished ---")