# communication_engine.py

import logging
import datetime
import json
import sys
import time
import re
from sqlalchemy import select, func, desc, or_, and_, distinct, case
from sqlalchemy.orm import Session as SQLAlchemySession # For type hinting
import numpy as np
import pandas as pd

# --- Imports for Models, Config, Email Service ---
try:
    import config
    logging.info("Configuration loaded successfully from config.py")
except ImportError:
    logging.error("Could not import config.py.")
    # Define dummy config if needed for basic loading
    class DummyConfig: TEST_MODE=True; SALES_REP_MAPPING={}; TOP_30_SET=set(); DASHBOARD_URL="#"; HIGH_RISK_THRESHOLD=70; HEALTH_POOR_THRESHOLD=40; EMAIL_DUE_SOON_DAYS=7; SQLALCHEMY_DATABASE_URI=""; FROM_EMAIL=""; SMTP_SERVER=""; SMTP_PORT=0; EMAIL_USERNAME=""; EMAIL_PASSWORD="";
    config = DummyConfig()
    logging.warning("Using dummy configuration.")

try:
    from models import db, AccountPrediction, AccountHistoricalRevenue
    logging.info("Models loaded successfully.")
except ImportError as e:
    logging.warning(f"Could not import models: {e}")
    # Define dummy models if needed
    class MockDB: session = None; Model = object; # etc.
    db = MockDB(); AccountPrediction = object(); AccountHistoricalRevenue = object()

try:
    # Use the actual function names from your service
    from services.email_service import send_email, print_email_instead_of_sending
    logging.info("Email service loaded.")
except ImportError as e_email:
    logging.error(f"Could not import email functions: {e_email}")
    def send_email(*args, **kwargs): logger.error("send_email dummy used!"); return False
    def print_email_instead_of_sending(*args, **kwargs): logger.error("print_email dummy used!"); return True

logger = logging.getLogger(__name__)

# --- Use config settings with safe defaults ---
TEST_MODE = getattr(config, 'TEST_MODE', True)
DASHBOARD_BASE_URL = getattr(config, 'DASHBOARD_URL', '#')
HIGH_RISK_THRESHOLD = getattr(config, 'CHURN_HIGH_RISK_THRESHOLD', 70)
HEALTH_THRESHOLD = getattr(config, 'HEALTH_POOR_THRESHOLD', 40)
DUE_SOON_DAYS = getattr(config, 'EMAIL_DUE_SOON_DAYS', 7)
TOP_30_SET = getattr(config, 'TOP_30_SET', set())
if not TOP_30_SET: logger.warning("TOP_30_SET is empty in config!")

# --- Helper Functions (Keep formatters, normalize_product_name, generate_status_tags) ---
# ... (Keep format_currency, format_currency_short, normalize_product_name, generate_status_tags) ...
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
    
    # Define abs_n here, after all the early returns
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
def normalize_product_name(product_name):
    if not product_name: return ""
    name = str(product_name).lower(); name = re.sub(r'\b\d+[a-z]+\b', '', name); name = re.sub(r'\b\d+in\d+\b', '', name)
    name = name.replace('-', ' ').replace('+', ' plus ');
    if "testosterone" in name and "up" in name: name = re.sub(r'testosterone\s+up', 'testosterone up', name)
    name = re.sub(r'\s+', ' ', name); return name.strip()
def generate_status_tags(account):
    tags = []
    # Use health score for primary risk indication now
    if getattr(account, 'health_score', 100) < HEALTH_THRESHOLD: tags.append(f"❤️‍🩹Low Health ({getattr(account, 'health_score', 0):.0f})")
    if getattr(account, 'rfm_segment', None) in ["At Risk", "Can't Lose"]: tags.append(f"🚨{getattr(account, 'rfm_segment')}")
    days_overdue = getattr(account, 'days_overdue', 0)
    if days_overdue is not None and days_overdue > 0: tags.append(f"❗Overdue {days_overdue}d")
    if getattr(account, 'rfm_segment', None) == 'Champions' and "Can't Lose" not in getattr(account, 'rfm_segment', ''): tags.append("⭐Champion") # Avoid duplicate high-value tags
    if getattr(account, 'rfm_segment', None) == 'New Customers': tags.append("🌱New")
    return tags

# --- Modified Helper to Fetch Only Previous Year Data ---
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
        results = db.session.execute(stmt).all() # Use db.session assuming app context
        for row in results:
            revenue_map[row.canonical_code] = row.total_revenue or 0.0
        logger.debug(f"DB Query: Fetched PY revenue for {len(revenue_map)} accounts.")
    except Exception as e:
        logger.error(f"DB Query Error in get_previous_year_revenue: {e}", exc_info=True)
    return revenue_map


# --- Main Weekly Digest Function (Refactored) ---
def send_weekly_digest_email_for_rep(rep_id, rep_name, rep_email):
    """
    Generates and sends the enhanced weekly digest email for a SINGLE sales rep,
    incorporating new metrics and sections. Uses SQLAlchemy 2.x.
    """
    logger.info(f"Generating weekly digest V4 for Rep: {rep_name} ({rep_id}) Email: {rep_email}")
    if not rep_email:
        logger.error(f"No email address for rep {rep_name} ({rep_id}). Skipping digest.")
        return False

    try:
        today = datetime.datetime.now().date()
        current_year = today.year
        prev_year = current_year - 1

        # Define Time Windows
        start_of_this_week = today
        end_of_this_week = today + datetime.timedelta(days=DUE_SOON_DAYS) # Use config var
        start_of_next_week = end_of_this_week + datetime.timedelta(days=1)
        end_of_next_week = start_of_next_week + datetime.timedelta(days=7)

        # --- Query Data Needed for All Sections ---
        # Query accounts due this week (fetch full object for details)
        due_this_week_stmt = select(AccountPrediction).where(
            AccountPrediction.sales_rep == rep_id,
            AccountPrediction.next_expected_purchase_date.isnot(None),
            func.date(AccountPrediction.next_expected_purchase_date) >= today,
            func.date(AccountPrediction.next_expected_purchase_date) <= end_of_this_week
        ).order_by(AccountPrediction.next_expected_purchase_date.asc())

        # Query accounts due next week (fetch only necessary fields)
        due_next_week_stmt = select(
            AccountPrediction.canonical_code, AccountPrediction.name, AccountPrediction.next_expected_purchase_date
        ).where(
             AccountPrediction.sales_rep == rep_id,
             AccountPrediction.next_expected_purchase_date.isnot(None),
             func.date(AccountPrediction.next_expected_purchase_date) >= start_of_next_week,
             func.date(AccountPrediction.next_expected_purchase_date) <= end_of_next_week
        ).order_by(AccountPrediction.next_expected_purchase_date.asc())

        # Query accounts falling behind (Overdue > 0, sorted, limit 10)
        falling_behind_stmt = select(
            AccountPrediction.canonical_code, AccountPrediction.name,
            AccountPrediction.days_overdue, AccountPrediction.last_purchase_date
        ).where(
            AccountPrediction.sales_rep == rep_id,
            AccountPrediction.days_overdue > 0
        ).order_by(AccountPrediction.days_overdue.desc()).limit(10)

        # Query accounts pacing the most behind LY (Updated query)
        pacing_behind_stmt = select(
            AccountPrediction.canonical_code, AccountPrediction.name,
            AccountPrediction.yep_revenue, AccountPrediction.pace_vs_ly
        ).where(
            AccountPrediction.sales_rep == rep_id,
            AccountPrediction.pace_vs_ly.isnot(None)  # Show all accounts with pace data, not just negative
        ).order_by(AccountPrediction.pace_vs_ly.asc()).limit(10)  # ORDER BY pace_vs_ly ASC (Most negative first)

        # Execute queries
        accounts_due_this_week = db.session.execute(due_this_week_stmt).scalars().all()
        accounts_due_next_week_rows = db.session.execute(due_next_week_stmt).all()
        accounts_falling_behind = db.session.execute(falling_behind_stmt).all() # Row objects
        accounts_pacing_behind = db.session.execute(pacing_behind_stmt).all() # Row objects

        # Check if there's anything to report (Updated check)
        if not accounts_due_this_week and not accounts_falling_behind and not accounts_pacing_behind:
            logger.info(f"No accounts requiring attention (due, behind, pacing) for rep {rep_name}.")
            # Optionally send a "Quiet Week" email or simply return
            return False
        
        # --- Calculate Summary Statistics ---
        count_due_this_week = len(accounts_due_this_week)
        count_overdue = len(accounts_falling_behind) # This query is already limited to 10, need total count ideally
        # Let's query the *total* overdue count for the summary
        try:
             total_overdue_stmt = select(func.count(AccountPrediction.id)).where(
                  AccountPrediction.sales_rep == rep_id, AccountPrediction.days_overdue > 0
             )
             total_overdue_count = db.session.execute(total_overdue_stmt).scalar_one()
        except Exception:
             total_overdue_count = count_overdue # Fallback to list length if query fails

        # Count how many of those due this week are low health
        count_due_low_health = sum(1 for acc in accounts_due_this_week if getattr(acc, 'health_score', 100) < HEALTH_THRESHOLD)
        logger.info(f"Summary Stats for {rep_name}: Due This Week={count_due_this_week}, Total Overdue={total_overdue_count}, Due & Low Health={count_due_low_health}")
        # --- End Summary Statistics ---

        # --- Fetch Previous Year Revenue for Relevant Accounts ---
        # Collect all canonical codes appearing in the lists that need PY comparison
        codes_for_py_rev = set()
        for acc in accounts_due_this_week: codes_for_py_rev.add(acc.canonical_code)
        # Include accounts from pacing_behind list as they NEED py_revenue for % calc
        for row in accounts_pacing_behind: codes_for_py_rev.add(row.canonical_code)
        
        # Fetch PY Revenue only if needed
        py_revenue_map = {}
        if codes_for_py_rev:
             py_revenue_map = get_previous_year_revenue(list(codes_for_py_rev), prev_year)


        # --- Build Email Body ---
        subject = f"Your Weekly Sales Digest & Action Plan - Week of {start_of_this_week.strftime('%b %d')}"
        body_lines = [
            f"Hi {rep_name.split()[0]},",
            "",
            "Welcome to your weekly AI-powered sales digest! Our system analyzes purchasing patterns and trends to help you focus your efforts on the right accounts at the right time.",
            "",
            f"**Summary for the Week of {start_of_this_week.strftime('%B %d, %Y')}:**",
            f"*   **Accounts Due This Week:** {count_due_this_week}"
            ""
            # Optional: Add a summary of Top Pacing/Declining if desired
        ]

        # --- Section 1: Accounts Due This Week ---
        body_lines.extend([ "========================================", f"📈 ACCOUNTS DUE THIS WEEK ({start_of_this_week.strftime('%b %d')} - {end_of_this_week.strftime('%b %d')})", "========================================\n"])
        if not accounts_due_this_week:
             body_lines.append("*(No accounts scheduled due in the upcoming week)*\n")
        else:
            for i, acc in enumerate(accounts_due_this_week):
                # Fetch PY Revenue for this account
                py_total_revenue = py_revenue_map.get(acc.canonical_code, 0.0)

                # Calculate Pacing % vs LY (Updated Logic)
                pace_pct_vs_ly_str = "N/A"
                if pd.notna(acc.pace_vs_ly):  # Check if pace value exists first
                    if py_total_revenue > 0:  # If PY exists and > 0, calc %
                        pace_pct = (acc.pace_vs_ly / py_total_revenue) * 100.0
                        pace_pct_vs_ly_str = f"{pace_pct:+.1f}%"
                    # If pace exists but PY is 0, check YEP for New Growth status
                    elif pd.notna(acc.yep_revenue) and acc.yep_revenue > 0:
                        pace_pct_vs_ly_str = "🌱 New Growth"
                # If pace_vs_ly is None/NaN, it remains "N/A"

                # Calculate Cadence Lag (Updated None handling)
                cadence_lag_str = "N/A"
                if pd.notna(acc.avg_interval_cytd) and pd.notna(acc.avg_interval_py):
                    lag = acc.avg_interval_cytd - acc.avg_interval_py
                    cadence_lag_str = f"{lag:+.0f}d"
                elif pd.notna(acc.avg_interval_cytd): # Has CYTD but no PY
                    cadence_lag_str = "(New Cadence)"

                # Product Coverage / Opportunity (Simplified check)
                top_30_carried_count = 0
                missing_list = []
                try:
                    if acc.carried_top_products_json:
                         carried = json.loads(acc.carried_top_products_json)
                         top_30_carried_count = len(carried) if isinstance(carried, list) else 0
                    if acc.missing_top_products_json:
                         missing = json.loads(acc.missing_top_products_json)
                         missing_list = missing if isinstance(missing, list) else []
                except Exception as json_err: logger.warning(f"JSON error for products {acc.canonical_code}: {json_err}")

                opportunity_str = f"Suggest -> {', '.join(missing_list[:3])}" + (", ..." if len(missing_list) > 3 else "") if missing_list else "None"

                # Format Output Lines
                status_tags = generate_status_tags(acc)
                body_lines.append(f"**{i+1}. {acc.name}** {' | '.join(status_tags)}")
                body_lines.append(f"   CardCode: {acc.base_card_code}")
                #body_lines.append(f"   Code: {acc.canonical_code}")
                body_lines.append(f"   Recommended Due: **{acc.next_expected_purchase_date.strftime('%a, %b %d')}**")
                body_lines.append(f"   Last Order: {acc.last_purchase_date.strftime('%b %d, %Y') if acc.last_purchase_date else 'N/A'} ({format_currency(acc.last_purchase_amount)})")
                body_lines.append(f"   Performance: CYTD: {format_currency_short(acc.cytd_revenue)} | PY: {format_currency_short(py_total_revenue)} | YEP: {format_currency_short(acc.yep_revenue)} (Pacing: {format_currency_short(acc.pace_vs_ly)} / {pace_pct_vs_ly_str} vs LY)")
                cytd_interval_str = f"{acc.avg_interval_cytd:.0f}d" if pd.notna(acc.avg_interval_cytd) else "N/A"
                py_interval_str = f"{acc.avg_interval_py:.0f}d" if pd.notna(acc.avg_interval_py) else "N/A"
                
                body_lines.append(f"   Cadence: CYTD Avg: {cytd_interval_str} | PY Avg: {py_interval_str}")
                #body_lines.append(f"   Scores: Priority: {getattr(acc,'enhanced_priority_score',0.0):.1f} | Health: {getattr(acc,'health_score',0.0):.1f}")
                body_lines.append(f"   Top 30: {top_30_carried_count}/{len(TOP_30_SET)} products")
                body_lines.append(f"   Opportunity: {opportunity_str}")
                body_lines.append("") # Spacer

        # --- Section 2: Falling Behind ---
        body_lines.extend(["----------------------------------------", "📉 TOP ACCOUNTS - Falling Behind Cadence", "----------------------------------------\n"])
        if not accounts_falling_behind:
            body_lines.append("*(No accounts currently overdue)*\n")
        else:
            for idx, row in enumerate(accounts_falling_behind):
                last_order_str = row.last_purchase_date.strftime('%b %d, %Y') if row.last_purchase_date else 'N/A'
                body_lines.append(f"{idx+1}. {row.name} (Overdue: {row.days_overdue} days | Last Order: {last_order_str})")
            body_lines.append("")

        # --- Section 3: Largest Pace Decline vs LY (Updated Section) ---
        body_lines.extend(["----------------------------------------", "📉 TOP ACCOUNTS - Largest Pace Decline vs LY", "----------------------------------------\n"])
        if not accounts_pacing_behind:
            body_lines.append("*(No accounts currently projected to decline vs LY)*\n") # Updated message
        else:
            for idx, row in enumerate(accounts_pacing_behind):
                # Get PY revenue from the map we already fetched
                py_total_revenue = py_revenue_map.get(row.canonical_code, 0.0)

                # Calculate Pace Percentage vs LY OR Status String (Updated Logic)
                pace_pct_vs_ly_str = "N/A" # Default
                if pd.notna(row.pace_vs_ly):
                     if py_total_revenue > 0: # Has PY revenue, calculate actual %
                          pace_pct = (row.pace_vs_ly / py_total_revenue) * 100.0
                          pace_pct_vs_ly_str = f"{pace_pct:+.1f}%"
                     # Only show "New Growth" if YEP is positive AND PY was zero
                     elif pd.notna(row.yep_revenue) and row.yep_revenue > 0 and py_total_revenue == 0:
                          pace_pct_vs_ly_str = "🌱 New Growth"
                     # Otherwise (e.g., YEP is 0 or None, PY is 0), it remains N/A

                # Format dollar amounts
                pace_usd_str = f"{format_currency_short(row.pace_vs_ly)}" if pd.notna(row.pace_vs_ly) else "N/A"
                yep_str = f"{format_currency_short(row.yep_revenue)}" if pd.notna(row.yep_revenue) else "N/A"

                # Construct the output line
                body_lines.append(f"{idx+1}. {row.name} (Pacing: {pace_usd_str} / {pace_pct_vs_ly_str} vs LY | Proj. YEP: {yep_str})")
            body_lines.append("")

        # --- Section 4: Due Next Week ---
        body_lines.extend(["----------------------------------------", f"🗓️ Heads Up: Due Next Week ({start_of_next_week.strftime('%b %d')} - {end_of_next_week.strftime('%b %d')})","----------------------------------------\n"])
        if not accounts_due_next_week_rows:
            body_lines.append("*(No accounts currently projected due next week.)*\n")
        else:
             for row in accounts_due_next_week_rows:
                 body_lines.append(f"- {row.name} (Due: {row.next_expected_purchase_date.strftime('%a, %b %d')})")
             body_lines.append("")

        # --- Footer ---
        strategic_dashboard_link = f"{DASHBOARD_BASE_URL}/strategic?sales_rep={rep_id}" # Link to rep's view
        body_lines.append(f"\n🚀 View full details & history on your Dashboard: {strategic_dashboard_link}")
        body_lines.append("\nBest regards,\nThe Sales Intelligence Team")

        body = "\n".join(body_lines)

        # --- Send Email ---
        email_func = print_email_instead_of_sending if TEST_MODE else send_email
        log_prefix = "[TEST MODE] Would print" if TEST_MODE else "Sent"
        try:
            # Pass config values explicitly if send_email is not aware of Flask app config
            success = email_func(
                subject=subject, body=body, recipient=rep_email,
                from_email=config.FROM_EMAIL, smtp_server=config.SMTP_SERVER,
                smtp_port=config.SMTP_PORT, username=config.EMAIL_USERNAME,
                password=config.EMAIL_PASSWORD
            )
            if success:
                logger.info(f"{log_prefix} weekly digest to {rep_email} ({len(accounts_due_this_week)} accounts due this week).")
                return True
            else:
                logger.error(f"Email function returned False for {rep_email}.")
                return False
        except Exception as mail_err:
            logger.error(f"Failed to send weekly digest email to {rep_email}: {mail_err}", exc_info=True)
            return False

    except Exception as e:
        logger.error(f"Error generating weekly digest for rep {rep_name} ({rep_id}): {str(e)}", exc_info=True)
        return False


# --- send_all_weekly_digests (No major changes needed, uses updated helpers) ---
def send_all_weekly_digests():
    """Queries all reps and triggers individual digest emails using SQLAlchemy 2.x."""
    logger.info("Starting send_all_weekly_digests...")
    reps_to_email = []
    try:
        # Query distinct reps using SQLAlchemy 2.x
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
            rep_email = getattr(config, 'SALES_REP_MAPPING', {}).get(rep_name) # Safer getattr
            if rep_email: reps_to_email.append({'id': rep_id, 'name': rep_name, 'email': rep_email})
            else: logger.warning(f"No email mapping for {rep_name} (ID: {rep_id})")

        logger.info(f"Found {len(reps_to_email)} assigned reps with emails to process.")
        processed_count = 0; failed_count = 0
        for rep_info in reps_to_email:
            try:
                logger.info(f"Processing digest for {rep_info['name']} ({rep_info['id']})")
                success = send_weekly_digest_email_for_rep( # This now calls the V3 version
                    rep_info['id'], rep_info['name'], rep_info['email']
                )
                if success: processed_count += 1
                else: failed_count += 1
                time.sleep(0.5) # Keep small delay
            except Exception as e_inner:
                logger.error(f"Unhandled error processing digest for {rep_info['name']}: {e_inner}", exc_info=True)
                failed_count += 1

        logger.info(f"Digest processing complete. Success: {processed_count}, Failed: {failed_count}, Skipped: {len(distinct_reps_rows) - len(reps_to_email)}")

    except Exception as e_outer:
         logger.error(f"Major error in send_all_weekly_digests: {e_outer}", exc_info=True)
    logger.info("Finished send_all_weekly_digests.")


# --- Standalone Test Block (Refactored) ---
if __name__ == "__main__":
    print("--- Running Communication Engine Standalone Test (v3 - New Metrics) ---")

    # --- Manual Logging Setup ---
    log_level = logging.DEBUG # Set desired level (DEBUG to see everything)
    log_formatter = logging.Formatter('%(asctime)s - %(name)s [%(levelname)s] %(message)s')

    # Configure the console handler (writes to stdout)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level)
    console_handler.setFormatter(log_formatter)

    # Get the specific loggers we want to control
    logger_main = logging.getLogger(__name__) # Gets the '__main__' logger
    logger_engine = logging.getLogger('services.communication_engine') # Gets the engine's logger

    # Set the level for both loggers
    logger_main.setLevel(log_level)
    logger_engine.setLevel(log_level)

    # IMPORTANT: Add the handler ONLY to the top-level logger you want output from.
    # Adding to both might cause duplicate messages if propagation is on.
    # Let's add it to the __main__ logger. Since propagate defaults to True,
    # messages from logger_engine should bubble up to __main__ and be handled.
    # Clear existing handlers first just in case something else added one
    logger_main.handlers.clear()
    logger_main.addHandler(console_handler)
    # Optional: Prevent __main__ from propagating further up to the root logger
    # logger_main.propagate = False

    # Ensure the engine logger propagates messages up to logger_main (or root)
    # This should be the default, but explicit is safe.
    logger_engine.propagate = True
    # Optional: Clear any handlers directly attached to logger_engine if needed
    # logger_engine.handlers.clear()


    logger_main.info(f"Configured loggers '{logger_main.name}' and '{logger_engine.name}' for DEBUG level.")
    # --- End Manual Logging Setup ---


    try: from flask import Flask
    except ImportError as e: print(f"ERROR: Flask not found: {e}."); exit(1)

    # Create minimal app for context
    app = Flask(__name__)
    try:
        # Load necessary config for DB connection
        app.config['SQLALCHEMY_DATABASE_URI'] = getattr(config, 'SQLALCHEMY_DATABASE_URI')
        app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
        logger.info(f"Using DB URI: {app.config['SQLALCHEMY_DATABASE_URI']}")
    except AttributeError: print("ERROR: SQLALCHEMY_DATABASE_URI not found in config.py."); exit(1)
    except Exception as config_err: print(f"ERROR loading config: {config_err}"); exit(1)

    try: db.init_app(app); print("Database initialized.")
    except Exception as db_err: print(f"ERROR initializing database: {db_err}"); exit(1)

    effective_test_mode = getattr(config, 'TEST_MODE', True)
    print(f"\n--- TEST MODE: {'ON (Printing Emails)' if effective_test_mode else 'OFF (Attempting SMTP Send)'} ---")

    # --- Test Execution ---
    TEST_REP_NAME = "Mariano Cruz" # Or choose another rep
    #TEST_REP_NAME = None # Set to None to test the multi-rep logic (send_all_weekly_digests)

    with app.app_context():
        if TEST_REP_NAME:
            # --- Test Single Rep ---
            test_rep_id = None
            test_rep_email = getattr(config, 'SALES_REP_MAPPING', {}).get(TEST_REP_NAME)
            if not test_rep_email: print(f"ERROR: Email not found for '{TEST_REP_NAME}'.")
            else:
                try: # Find Rep ID
                    stmt = select(AccountPrediction.sales_rep).where(AccountPrediction.sales_rep_name == TEST_REP_NAME).limit(1)
                    test_rep_id = db.session.scalar(stmt)
                    if test_rep_id: print(f"Found Rep ID for {TEST_REP_NAME}: {test_rep_id}")
                    else: print(f"WARNING: Could not find Rep ID for {TEST_REP_NAME}.")
                except Exception as db_err: print(f"WARNING: DB error finding Rep ID: {db_err}")

                print(f"\n--- Testing SINGLE Rep: {TEST_REP_NAME} (ID: {test_rep_id}) ---")
                try:
                    success = send_weekly_digest_email_for_rep(test_rep_id, TEST_REP_NAME, test_rep_email)
                    if success: print(f"\n--- Test for {TEST_REP_NAME} Completed ---")
                    else: print(f"\n--- Test for {TEST_REP_NAME} Indicated Failure ---")
                except Exception as e: logger.error(f"Exception during single rep test: {e}", exc_info=True)
        else:
            # --- Test All Reps ---
             print(f"\n--- Testing ALL Reps ---")
             send_all_weekly_digests() # This function logs its own progress/summary

    print("\n--- Standalone Test Finished ---")



"""
if __name__ == "__main__":
    # This block runs only when the script is executed directly (python communication_engine.py)

    # --- Basic Setup for Standalone Testing ---
    print("--- Running Communication Engine Standalone Test ---")
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    print("Logging level set to DEBUG for detailed product debugging")

    # Create a temporary Flask app for context
    # This assumes config.py is in the parent directory or accessible via python path
    try:
        from flask import Flask
        import config # Import config here again if needed globally
        from models import db
    except ImportError as e:
        print(f"Error importing Flask/config/models: {e}. Make sure dependencies are installed and script is run correctly.")
        exit()

    app = Flask(__name__)
    # Ensure DB URI uses the correct path relative to *this* script if needed
    # Or better, ensure config.py resolves BASE_DIR correctly
    app.config['SQLALCHEMY_DATABASE_URI'] = config.SQLALCHEMY_DATABASE_URI
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
    db.init_app(app)

    # --- Test Configuration ---
    # Ensure TEST_MODE is True in config.py or override here
    #TEST_MODE = True
    #print(f"TEST_MODE is ON: Emails will be printed to console.")

    # *** CHOOSE A REP TO TEST ***
    test_rep_name = "Mariano Cruz" # Example - CHOOSE A REAL NAME FROM YOUR MAPPING
    test_rep_id = None # Will try to find ID based on name later if needed, or set manually e.g., '10.0'
    test_rep_email = config.SALES_REP_MAPPING.get(test_rep_name)

    if not test_rep_email:
        print(f"ERROR: Could not find email for test rep '{test_rep_name}' in config.SALES_REP_MAPPING.")
        exit()

    # Find the Rep ID if not set manually (optional but good practice)
    if test_rep_id is None:
         with app.app_context():
             rep_record = db.session.query(AccountPrediction.sales_rep).filter(AccountPrediction.sales_rep_name == test_rep_name).first()
             if rep_record:
                 test_rep_id = rep_record.sales_rep
                 print(f"Found Rep ID for {test_rep_name}: {test_rep_id}")
             else:
                 print(f"WARNING: Could not find Rep ID for {test_rep_name} in DB. Usingcd None.")
                 # Test might still work if rep_id isn't strictly needed by all sub-queries

    if test_rep_id is None:
         print("Proceeding without Rep ID, some queries might behave unexpectedly.")
         # Assign a placeholder if strictly needed, though None might be okay depending on queries
         # test_rep_id = 'UNKNOWN'


    # --- Execute Test ---
    print(f"\nAttempting to generate and 'send' digest for: {test_rep_name} ({test_rep_id}) to {test_rep_email}")
    with app.app_context(): # Establish context for DB queries within the function
        try:
            success = send_weekly_digest_email_for_rep(test_rep_id, test_rep_name, test_rep_email)
            if success:
                print("\n--- Test Completed Successfully (Email Printed Above) ---")
            else:
                print("\n--- Test Function Indicated Failure (Check Logs/Output) ---")
        except Exception as e:
            print(f"\n--- An Exception Occurred During Test ---")
            logger.error(f"Error during manual test: {e}", exc_info=True)
"""

"""
# ==========================================
#  *** ADD THIS BLOCK AT THE END ***
# ==========================================
if __name__ == "__main__":
    # This block runs only when the script is executed directly (python communication_engine.py)

    # --- Basic Setup for Standalone Testing ---
    print("--- Running Communication Engine Standalone Test ---")
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    print("Logging level set to DEBUG for detailed product debugging")

    # Create a temporary Flask app for context
    # This assumes config.py is in the parent directory or accessible via python path
    try:
        from flask import Flask
        import config # Import config here again if needed globally
        from models import db
    except ImportError as e:
        print(f"Error importing Flask/config/models: {e}. Make sure dependencies are installed and script is run correctly.")
        exit()

    app = Flask(__name__)
    # Ensure DB URI uses the correct path relative to *this* script if needed
    # Or better, ensure config.py resolves BASE_DIR correctly
    app.config['SQLALCHEMY_DATABASE_URI'] = config.SQLALCHEMY_DATABASE_URI
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
    db.init_app(app)

    # --- Test Configuration ---
    # Ensure TEST_MODE is True in config.py or override here
    TEST_MODE = True
    print(f"TEST_MODE is ON: Emails will be printed to console.")

    # Get list of all sales reps from the mapping
    all_reps = list(config.SALES_REP_MAPPING.keys())
    print(f"Found {len(all_reps)} sales reps in config.SALES_REP_MAPPING")
    
    # Debug information about Top 30 products
    print(f"\n--- Top 30 Products Debug Info ---")
    print(f"TOP_30_SET contains {len(config.TOP_30_SET)} products")
    print("First 5 products in TOP_30_SET:")
    for i, product in enumerate(list(config.TOP_30_SET)[:5]):
        print(f"  {i+1}. {product}")
    print("...")
    
    # Initialize counters
    successful_digests = 0
    failed_digests = 0
    skipped_digests = 0
    reps_processed = 0
    
    print("\n--- Testing Weekly Digests for All Reps ---")
    with app.app_context():
        # First, query to find all reps who have accounts due this week
        today = datetime.datetime.now().date()
        end_of_this_week = today + datetime.timedelta(days=7)
        
        # Find all reps who have accounts due this week
        try:
            reps_with_accounts_due = db.session.query(
                AccountPrediction.sales_rep,
                AccountPrediction.sales_rep_name,
                func.count(AccountPrediction.id).label('account_count')
            ).filter(
                AccountPrediction.next_expected_purchase_date.isnot(None),
                func.date(AccountPrediction.next_expected_purchase_date) >= today,
                func.date(AccountPrediction.next_expected_purchase_date) <= end_of_this_week
            ).group_by(
                AccountPrediction.sales_rep, 
                AccountPrediction.sales_rep_name
            ).having(
                func.count(AccountPrediction.id) > 0
            ).order_by(
                func.count(AccountPrediction.id).desc()
            ).limit(10).all()  # Limit to 10 reps to avoid excessive output
            
            print(f"Found {len(reps_with_accounts_due)} reps who have accounts due this week")
            
            # Process each rep with accounts due
            for rep_record in reps_with_accounts_due:
                rep_id = rep_record.sales_rep
                rep_name = rep_record.sales_rep_name
                account_count = rep_record.account_count
                rep_email = config.SALES_REP_MAPPING.get(rep_name)
                
                if not rep_email:
                    print(f"\nSKIPPING: No email found for rep '{rep_name}' ({rep_id})")
                    skipped_digests += 1
                    continue
                
                reps_processed += 1
                print(f"\n--- Rep {reps_processed}: {rep_name} ({rep_id}) - {account_count} accounts due ---")
                
                try:
                    success = send_weekly_digest_email_for_rep(rep_id, rep_name, rep_email)
                    if success:
                        print(f"SUCCESS: Generated digest for {rep_name}")
                        successful_digests += 1
                    else:
                        print(f"FAILURE: Could not generate digest for {rep_name}")
                        failed_digests += 1
                except Exception as e:
                    print(f"ERROR: Exception while generating digest for {rep_name}: {str(e)}")
                    logger.error(f"Error during digest for {rep_name}: {e}", exc_info=True)
                    failed_digests += 1
        
        except Exception as e:
            print(f"ERROR querying reps with accounts due: {str(e)}")
            logger.error(f"Error querying reps with accounts due: {e}", exc_info=True)
            
    # Print summary
    print("\n=== Test Summary ===")
    print(f"Processed {reps_processed} reps")
    print(f"Successful digests: {successful_digests}")
    print(f"Failed digests: {failed_digests}")
    print(f"Skipped digests: {skipped_digests}")
    print("===================")

"""