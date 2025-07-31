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


# --- Scoring/Threshold Constants from Config (Used for Filtering/Display) ---
HEALTH_POOR_THRESHOLD = getattr(config, 'HEALTH_POOR_THRESHOLD', 40)
PRIORITY_PACE_DECLINE_PCT_THRESHOLD = getattr(config, 'PRIORITY_PACE_DECLINE_PCT_THRESHOLD', -10)
GROWTH_PACE_INCREASE_PCT_THRESHOLD = getattr(config, 'GROWTH_PACE_INCREASE_PCT_THRESHOLD', 10)
GROWTH_HEALTH_THRESHOLD = getattr(config, 'GROWTH_HEALTH_THRESHOLD', 60)
GROWTH_MISSING_PRODUCTS_THRESHOLD = getattr(config, 'GROWTH_MISSING_PRODUCTS_THRESHOLD', 3)
# PRIORITY_HEALTH_THRESHOLD is implicitly defined by HEALTH_POOR_THRESHOLD in generate_reason_action



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


# --- Generate Reason and Action (Keep as is, reads metrics) ---
def generate_reason_action(account: AccountPrediction, py_revenue: float) -> tuple[str, str]:
    """Generates a concise reason and suggested action based on account metrics."""
    reasons = []
    now = datetime.datetime.now()
    today_date = now.date()

    # Use the config threshold
    current_health_threshold = HEALTH_POOR_THRESHOLD

    # Check factors in rough order of importance / commonality
    days_overdue = getattr(account, 'days_overdue', 0)
    health = getattr(account, 'health_score', 100)
    pace_vs_ly = getattr(account, 'pace_vs_ly', None)
    due_date = getattr(account, 'next_expected_purchase_date', None)
    missing_products = getattr(account, 'missing_top_products', []) # Uses property
    rfm_segment = getattr(account, 'rfm_segment', '')

    # Overdue is primary driver
    if days_overdue is not None and days_overdue > 7: # Significant overdue
        reasons.append(f"Overdue ({days_overdue}d)")
    elif days_overdue is not None and days_overdue > 0: # Slightly overdue
        reasons.append(f"Due ({days_overdue}d ago)")

    # Low Health
    if health is not None and health < current_health_threshold:
        reasons.append(f"Low Health ({health:.0f})")
    elif health is not None and health < GROWTH_HEALTH_THRESHOLD: # Avg Health
         if len(reasons) < 2: reasons.append(f"Avg Health ({health:.0f})") # Add if space

    # Pace Decline
    if pace_vs_ly is not None and py_revenue is not None and py_revenue > 0:
        pace_pct = (pace_vs_ly / py_revenue) * 100.0
        if pace_pct < PRIORITY_PACE_DECLINE_PCT_THRESHOLD: # Use config threshold
            reasons.append(f"Pace ({pace_pct:+.0f}%)")
    elif pace_vs_ly is not None and pace_vs_ly < -500: # Significant absolute drop
         if len(reasons) < 2: reasons.append(f"Pace ({format_currency_short(pace_vs_ly)})")

    # Upcoming Due Date (if not already overdue)
    if not any("Overdue" in r or "Due" in r for r in reasons) and due_date:
        if isinstance(due_date, datetime.datetime): due_date_only = due_date.date()
        elif isinstance(due_date, datetime.date): due_date_only = due_date
        else: due_date_only = None
        if due_date_only:
            days_until_due = (due_date_only - today_date).days
            if 0 <= days_until_due <= 7:
                reasons.append(f"Due Soon ({due_date_only.strftime('%a')})")

    # High-Risk RFM
    if rfm_segment in ["Can't Lose", "At Risk"] and len(reasons) < 2:
        reasons.append(f"{rfm_segment}")

    # Upsell Opportunity (if space allows)
    # Ensure missing_products is treated as a list/iterable
    if hasattr(missing_products, '__iter__') and not isinstance(missing_products, str) and missing_products and len(reasons) < 2:
        try:
           missing_count = len(missing_products)
           if missing_count >= GROWTH_MISSING_PRODUCTS_THRESHOLD: # Only show if significant
                reasons.append(f"Upsell Opp ({missing_count} items)")
        except TypeError: # Handle case where missing_products might not have len()
            pass

    # Determine Primary Reason for Action Mapping
    primary_reason = reasons[0] if reasons else "General Check-in"
    reason_str = " | ".join(reasons[:2]) # Show top 1 or 2 reasons

    # Map primary reason to action
    if "Overdue" in primary_reason or "Due (" in primary_reason:
        action = "Recover Order"
    elif "Low Health" in primary_reason:
        action = "Health Check-in"
    elif "Pace" in primary_reason:
        action = "Discuss Performance"
    elif "Due Soon" in primary_reason:
         action = "Confirm Order"
    elif "Upsell Opp" in primary_reason:
        action = "Explore Expansion"
    elif "Can't Lose" in primary_reason or "At Risk" in primary_reason:
        action = "Proactive Outreach"
    else:
        action = "General Check-in"

    return reason_str, action



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
    Generates and sends the Action-Focused weekly digest email (V6) using
    the pre-calculated 'enhanced_priority_score'.
    """
    logger.info(f"Generating Action-Focused Weekly Digest V6 for Rep: {rep_name} ({rep_id}) Email: {rep_email}")
    if not rep_email:
        logger.error(f"No email address for rep {rep_name} ({rep_id}). Skipping digest.")
        return False

    try:
        today = datetime.datetime.now().date()
        current_year = today.year
        prev_year = current_year - 1
        next_14_days_end = today + datetime.timedelta(days=14) # For growth opp check
        end_of_this_week = today + datetime.timedelta(days=6) # Today + 6 days = 7 day window inclusive

        # === 1. Query Top 10 Priority Accounts ===
        # Query all necessary fields for display and reasoning, sort by pre-calculated score
        logger.debug(f"Querying Top 10 Priority Accounts for Rep ID: {rep_id}")
        top_10_stmt = select(AccountPrediction).where(
            AccountPrediction.sales_rep == rep_id,
            AccountPrediction.enhanced_priority_score.isnot(None) # Ensure score exists
        ).order_by(
            AccountPrediction.enhanced_priority_score.desc() # Higher score = higher priority
        ).limit(10)

        top_10_priority_accounts = db.session.execute(top_10_stmt).scalars().all()
        logger.info(f"Found {len(top_10_priority_accounts)} top priority accounts for {rep_name}.")
        top_10_codes = {acc.canonical_code for acc in top_10_priority_accounts} # Set for quick lookup

        # === 2. Query Growth Opportunities (Candidates) ===
        logger.debug(f"Querying growth opportunity candidates for Rep ID: {rep_id}")
        # Fetch a pool of potential candidates excluding the Top 10
        # Filter conditions check basic criteria achievable in SQL
        growth_conditions = and_(
            AccountPrediction.sales_rep == rep_id,
            AccountPrediction.health_score >= GROWTH_HEALTH_THRESHOLD, # Config: Good health
            AccountPrediction.canonical_code.notin_(top_10_codes), # Exclude Top 10
            or_(
                 # Has positive pace (more filtering needed later for %)
                 AccountPrediction.pace_vs_ly > 0,
                 # Missing products JSON indicates potential (more filtering needed later)
                 AccountPrediction.missing_top_products_json.isnot(None),
                 AccountPrediction.missing_top_products_json != '[]',
                 # High-value segment due relatively soon
                 and_(
                      AccountPrediction.rfm_segment.in_(["Champions", "Loyal Customers"]),
                      func.date(AccountPrediction.next_expected_purchase_date) <= next_14_days_end
                 )
            )
        )
        # Fetch more candidates than needed initially for Python filtering
        growth_stmt = select(AccountPrediction).where(growth_conditions).limit(25)
        growth_candidate_accounts = db.session.execute(growth_stmt).scalars().all()
        logger.debug(f"Found {len(growth_candidate_accounts)} growth candidates for {rep_name} pre-filtering.")


        # === 3. Fetch Previous Year Revenue (for Top 10 reasons & Growth filtering) ===
        codes_for_py_rev = set(top_10_codes)
        codes_for_py_rev.update(acc.canonical_code for acc in growth_candidate_accounts)
        py_revenue_map = {}
        if codes_for_py_rev:
             py_revenue_map = get_previous_year_revenue(list(codes_for_py_rev), prev_year)

        # === 4. Filter Growth Opportunities (Using Python) ===
        growth_accounts_filtered = []
        for acc in growth_candidate_accounts:
            py_rev = py_revenue_map.get(acc.canonical_code, 0.0)
            pace_vs_ly = getattr(acc, 'pace_vs_ly', None)
            missing_products = getattr(acc, 'missing_top_products', []) # Use property

            is_growth_opp = False
            # Check Pace % (using config threshold)
            if pace_vs_ly is not None and py_rev is not None and py_rev > 0:
                pace_pct = (pace_vs_ly / py_rev) * 100.0
                if pace_pct >= GROWTH_PACE_INCREASE_PCT_THRESHOLD:
                    is_growth_opp = True

            # Check Missing Products (using config threshold)
            if not is_growth_opp and hasattr(missing_products, '__iter__') and not isinstance(missing_products, str):
                try:
                     if len(missing_products) >= GROWTH_MISSING_PRODUCTS_THRESHOLD:
                           is_growth_opp = True
                except TypeError: pass # Ignore if len not applicable

            # Check High Value Segment Due Soon (already part of SQL, refine if needed)
            if not is_growth_opp and acc.rfm_segment in ["Champions", "Loyal Customers"]:
                 # Maybe add extra check: due within 14 days?
                 due_date = getattr(acc, 'next_expected_purchase_date', None)
                 if due_date:
                      due_date_only = due_date.date() if isinstance(due_date, datetime.datetime) else due_date
                      if isinstance(due_date_only, datetime.date) and due_date_only <= next_14_days_end:
                           is_growth_opp = True

            if is_growth_opp:
                growth_accounts_filtered.append(acc)
            if len(growth_accounts_filtered) >= 5: break # Limit to top 5 growth opps
        logger.info(f"Identified {len(growth_accounts_filtered)} growth opportunity accounts post-filtering for {rep_name}.")

        # === 5. Query Portfolio Snapshot Data ===
        logger.debug(f"Querying snapshot data for Rep ID: {rep_id}")
        snapshot_counts = {'overdue': 0, 'low_health': 0, 'high_pace': 0, 'low_pace': 0, 'due_this_week': 0}
        try:
            # Query basic counts using SQL
            snapshot_stmt = select(
                func.count(case((AccountPrediction.days_overdue > 0, 1))).label("overdue"),
                func.count(case((AccountPrediction.health_score < HEALTH_POOR_THRESHOLD, 1))).label("low_health"),
                func.count(case((and_(
                    func.date(AccountPrediction.next_expected_purchase_date) >= today,
                    func.date(AccountPrediction.next_expected_purchase_date) <= end_of_this_week
                ), 1))).label("due_this_week")
            ).where(AccountPrediction.sales_rep == rep_id)
            snapshot_results = db.session.execute(snapshot_stmt).first()
            if snapshot_results:
                 snapshot_counts['overdue'] = snapshot_results.overdue or 0
                 snapshot_counts['low_health'] = snapshot_results.low_health or 0
                 snapshot_counts['due_this_week'] = snapshot_results.due_this_week or 0

            # Calculate Pace counts post-query for accuracy (Requires fetching all accounts or using pre-calculated)
            # Option A: Fetch all for pace count (less efficient for just counts)
            all_rep_accounts_stmt = select(AccountPrediction.canonical_code, AccountPrediction.pace_vs_ly).where(AccountPrediction.sales_rep == rep_id)
            all_rep_accounts_rows = db.session.execute(all_rep_accounts_stmt).all()
            all_rep_codes = [r.canonical_code for r in all_rep_accounts_rows]
            full_py_revenue_map = get_previous_year_revenue(all_rep_codes, prev_year) # Fetch PY for all

            high_pace_count = 0
            low_pace_count = 0
            for row in all_rep_accounts_rows:
                py_rev = full_py_revenue_map.get(row.canonical_code, 0.0)
                pace_val = row.pace_vs_ly
                if pace_val is not None and py_rev is not None and py_rev > 0:
                    pace_pct = (pace_val / py_rev) * 100.0
                    if pace_pct >= GROWTH_PACE_INCREASE_PCT_THRESHOLD:
                        high_pace_count += 1
                    elif pace_pct <= PRIORITY_PACE_DECLINE_PCT_THRESHOLD:
                        low_pace_count += 1
            snapshot_counts['high_pace'] = high_pace_count
            snapshot_counts['low_pace'] = low_pace_count

        except Exception as snap_err:
            logger.error(f"Error querying snapshot data for rep {rep_id}: {snap_err}", exc_info=True)

        # === 6. Check if any activity to report ===
        if not top_10_priority_accounts and not growth_accounts_filtered:
             logger.info(f"No priority or growth accounts found for rep {rep_name}. Sending quiet week email.")
             subject_quiet = f"Your Weekly Sales Digest - Week of {today.strftime('%b %d')}"
             body_quiet = f"Hi {rep_name.split()[0]},\n\nNo accounts flagged for specific attention or growth opportunities this week based on current data.\n\n🚀 View Your Full Dashboard: {DASHBOARD_BASE_URL}/strategic?sales_rep={rep_id}\n\nBest regards,\nThe Sales Intelligence Team"
             email_func = print_email_instead_of_sending if TEST_MODE else send_email
             email_func(subject=subject_quiet, body=body_quiet, recipient=rep_email, from_email=config.FROM_EMAIL, smtp_server=config.SMTP_SERVER, smtp_port=config.SMTP_PORT, username=config.EMAIL_USERNAME, password=config.EMAIL_PASSWORD)
             return True # Indicate success (sent quiet email)

        # === 7. Build Email Body (Using V6 Template) ===
        subject = f"Your Weekly Sales Action Plan & Key Metrics - Week of {today.strftime('%b %d')}"
        body_lines = [
            f"Hi {rep_name.split()[0]},",
            "",
            "Here's your AI-powered action plan focusing on your highest priority accounts this week, along with key portfolio insights.",
            # Add dashboard link here
            f"🚀 **View Your Full Interactive Dashboard:** {DASHBOARD_BASE_URL}/strategic/{rep_id}\n",
            "---"
        ]

        # --- Portfolio Snapshot Section ---
        # Define placeholder links - replace {filter_type} with actual query param used by your dashboard
        overdue_link = f"{DASHBOARD_BASE_URL}/strategic?sales_rep={rep_id}&filter=overdue"
        low_health_link = f"{DASHBOARD_BASE_URL}/strategic?sales_rep={rep_id}&filter=low_health"
        high_pace_link = f"{DASHBOARD_BASE_URL}/strategic?sales_rep={rep_id}&filter=high_pace"
        low_pace_link = f"{DASHBOARD_BASE_URL}/strategic?sales_rep={rep_id}&filter=low_pace"
        due_this_week_link = f"{DASHBOARD_BASE_URL}/strategic?sales_rep={rep_id}&filter=due_this_week"
        main_dashboard_link = f"{DASHBOARD_BASE_URL}/strategic/{rep_id}"

        body_lines.extend([
            "**📊 Your Portfolio Snapshot:**",
            f"*   Accounts Due This Week: {snapshot_counts['due_this_week']}",
            f"*   Accounts Overdue: {snapshot_counts['overdue']}",
            f"*   Low Health (<{HEALTH_POOR_THRESHOLD}): {snapshot_counts['low_health']}",
            f"*   Strong Pace (>+{GROWTH_PACE_INCREASE_PCT_THRESHOLD}%): {snapshot_counts['high_pace']}",
            f"*   Pace Decline (<{PRIORITY_PACE_DECLINE_PCT_THRESHOLD}%): {snapshot_counts['low_pace']}",
            # Add the new line directing to the dashboard
            f"\n*   For details on these groups, [View Your Dashboard]({main_dashboard_link})",
            "", # Keep empty line for spacing
            "---"
        ])

        # --- Section 1: Top Priority Actions ---
        body_lines.extend([ f"**🔥 TOP PRIORITY ACTIONS (Top {len(top_10_priority_accounts)} Ranked by Score)**", "*These accounts require your immediate attention based on a combination of urgency, risk, and opportunity.*\n"])
        if not top_10_priority_accounts:
             body_lines.append("*(No accounts currently flagged as high priority)*\n")
        else:
            for i, acc in enumerate(top_10_priority_accounts):
                py_rev = py_revenue_map.get(acc.canonical_code, 0.0)
                reason, action = generate_reason_action(acc, py_rev)
                account_detail_link = f"{DASHBOARD_BASE_URL}/account/{acc.canonical_code}" # Link to specific account page

                # Calculate Pace % string for display
                pace_pct_vs_ly_str = "N/A"
                if pd.notna(acc.pace_vs_ly):
                    if py_rev > 0:
                        pace_pct = (acc.pace_vs_ly / py_rev) * 100.0
                        pace_pct_vs_ly_str = f"{pace_pct:+.1f}%"
                    elif pd.notna(acc.yep_revenue) and acc.yep_revenue > 0:
                        pace_pct_vs_ly_str = "🌱 New Growth"

                # Format other details
                due_str = acc.next_expected_purchase_date.strftime('%a, %b %d') if acc.next_expected_purchase_date else 'N/A'
                last_order_str = acc.last_purchase_date.strftime('%b %d, %Y') if acc.last_purchase_date else 'N/A'
                last_amount_str = f"({format_currency(acc.last_purchase_amount)})" if pd.notna(acc.last_purchase_amount) else ""
                cytd_interval_str = f"{acc.avg_interval_cytd:.0f}d" if pd.notna(acc.avg_interval_cytd) else "N/A"
                py_interval_str = f"{acc.avg_interval_py:.0f}d" if pd.notna(acc.avg_interval_py) else "N/A"
                # Top 30 products
                carried_count = 0
                try:
                    if acc.carried_top_products_json:
                         carried = json.loads(acc.carried_top_products_json)
                         carried_count = len(carried) if isinstance(carried, list) else 0
                except Exception: pass # Ignore JSON errors
                top_30_str = f"{carried_count}/{len(TOP_30_SET)}" if TOP_30_SET else "N/A"
                # Opportunity products
                opp_list = []
                try:
                     if acc.missing_top_products_json:
                          missing = json.loads(acc.missing_top_products_json)
                          opp_list = missing if isinstance(missing, list) else []
                except Exception: pass
                opp_str = f"Suggest -> {', '.join(opp_list[:3])}" + (", ..." if len(opp_list) > 3 else "") if opp_list else "None"


                body_lines.append(f"**{i+1}. {acc.name}** ([Details]({account_detail_link}))")
                body_lines.append(f"   *   **Reason:** {reason}")
                body_lines.append(f"   *   **Suggested Action:** {action}")
                body_lines.append(f"   *   CardCode: {getattr(acc, 'base_card_code', 'N/A')}") # Show both if helpful
                body_lines.append(f"   *   Recommended Due: **{due_str}**")
                body_lines.append(f"   *   Last Order: {last_order_str} {last_amount_str}")
                body_lines.append(f"   *   Performance: CYTD: {format_currency_short(acc.cytd_revenue)} | PY: {format_currency_short(py_rev)} | YEP: {format_currency_short(acc.yep_revenue)} (Pacing: {format_currency_short(acc.pace_vs_ly)} / {pace_pct_vs_ly_str} vs LY)")
                body_lines.append(f"   *   Cadence: CYTD Avg: {cytd_interval_str} | PY Avg: {py_interval_str}")
                #body_lines.append(f"   *   Health: {acc.health_score:.0f} ({acc.health_category}) | RFM: {acc.rfm_segment}" )
                body_lines.append(f"   *   Top 30: {top_30_str} products")
                body_lines.append(f"   *   Opportunity: {opp_str}")
                body_lines.append("") # Spacer

        # --- Section 2: Growth & Engagement Opportunities ---
        body_lines.extend(["---", f"**📈 GROWTH & ENGAGEMENT OPPORTUNITIES (Top {len(growth_accounts_filtered)})**", "*Consider proactive outreach to these healthy or high-potential accounts.*\n"])
        if not growth_accounts_filtered:
            body_lines.append("*(No specific growth opportunities flagged this week)*\n")
        else:
            for i, acc in enumerate(growth_accounts_filtered):
                 py_rev = py_revenue_map.get(acc.canonical_code, 0.0)
                 account_detail_link = f"{DASHBOARD_BASE_URL}/account/{acc.canonical_code}"

                 # Determine primary growth driver for display
                 growth_driver = ""
                 pace_vs_ly = getattr(acc, 'pace_vs_ly', None)
                 missing_products = getattr(acc, 'missing_top_products', [])
                 if pace_vs_ly is not None and py_rev is not None and py_rev > 0:
                      pace_pct = (pace_vs_ly / py_rev) * 100.0
                      if pace_pct >= GROWTH_PACE_INCREASE_PCT_THRESHOLD:
                           growth_driver = f"Strong Pace ({pace_pct:+.0f}%)"
                 if not growth_driver and hasattr(missing_products, '__iter__') and not isinstance(missing_products, str) and missing_products and len(missing_products) >= GROWTH_MISSING_PRODUCTS_THRESHOLD:
                      growth_driver = f"Upsell Opp ({len(missing_products)} items)"
                 if not growth_driver and acc.rfm_segment in ["Champions", "Loyal Customers"]:
                      growth_driver = f"{acc.rfm_segment}"
                 if not growth_driver: growth_driver="Proactive Engagement" # Fallback

                 # Format other details (similar to priority section)
                 due_str = acc.next_expected_purchase_date.strftime('%a, %b %d') if acc.next_expected_purchase_date else 'N/A'
                 last_order_str = acc.last_purchase_date.strftime('%b %d, %Y') if acc.last_purchase_date else 'N/A'
                 last_amount_str = f"({format_currency(acc.last_purchase_amount)})" if pd.notna(acc.last_purchase_amount) else ""
                 pace_pct_vs_ly_str = "N/A" # Recalculate pace string for growth context
                 if pd.notna(acc.pace_vs_ly):
                    if py_rev > 0: pace_pct_vs_ly_str = f"{(acc.pace_vs_ly / py_rev * 100.0):+.1f}%"
                    elif pd.notna(acc.yep_revenue) and acc.yep_revenue > 0: pace_pct_vs_ly_str = "🌱 New Growth"
                 cytd_interval_str = f"{acc.avg_interval_cytd:.0f}d" if pd.notna(acc.avg_interval_cytd) else "N/A"
                 py_interval_str = f"{acc.avg_interval_py:.0f}d" if pd.notna(acc.avg_interval_py) else "N/A"
                 carried_count = 0; opp_list = [] # Recalculate product info
                 try: carried_count = len(json.loads(acc.carried_top_products_json or '[]'))
                 except Exception: pass
                 try: opp_list = json.loads(acc.missing_top_products_json or '[]')
                 except Exception: pass
                 top_30_str = f"{carried_count}/{len(TOP_30_SET)}" if TOP_30_SET else "N/A"
                 opp_str = f"Suggest -> {', '.join(opp_list[:3])}" + (", ..." if len(opp_list) > 3 else "") if opp_list else "None"


                 body_lines.append(f"**{i+1}. {acc.name}** ([Details]({account_detail_link}))")
                 body_lines.append(f"   *   **Opportunity:** {growth_driver}") # Main reason for being here
                 # Maybe a simpler action suggestion for growth
                 body_lines.append(f"   *   **Suggested Action:** {'Explore Expansion' if 'Upsell' in growth_driver else 'Proactive Engagement'}")
                 body_lines.append(f"   *   CardCode: {getattr(acc, 'base_card_code', 'N/A')}")
                 body_lines.append(f"   *   Recommended Due: **{due_str}**")
                 body_lines.append(f"   *   Last Order: {last_order_str} {last_amount_str}")
                 body_lines.append(f"   *   Performance: YEP: {format_currency_short(acc.yep_revenue)} (Pacing: {pace_pct_vs_ly_str} vs LY)")
                 body_lines.append(f"   *   Cadence: CYTD Avg: {cytd_interval_str} | PY Avg: {py_interval_str}")
                 #body_lines.append(f"   *   Health: {acc.health_score:.0f} ({acc.health_category}) | RFM: {acc.rfm_segment}" )
                 body_lines.append(f"   *   Top 30: {top_30_str} products")
                 body_lines.append(f"   *   Opportunity Details: {opp_str}")
                 body_lines.append("") # Spacer

        # --- Footer ---
        body_lines.append("---\nBest regards,\nThe Sales Intelligence Team")
        body = "\n".join(body_lines)

        # --- Send Email ---
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
                logger.info(f"{log_prefix} action-focused digest V6 to {rep_email} ({len(top_10_priority_accounts)} priority, {len(growth_accounts_filtered)} growth).")
                return True
            else:
                logger.error(f"Email function returned False for {rep_email}.")
                return False
        except Exception as mail_err:
            logger.error(f"Failed to send action-focused digest email to {rep_email}: {mail_err}", exc_info=True)
            return False

    except Exception as e:
        logger.error(f"Error generating action-focused digest V6 for rep {rep_name} ({rep_id}): {str(e)}", exc_info=True)
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


# --- Standalone Test Block (Updated for V6) ---
if __name__ == "__main__":
    print("--- Running Communication Engine Standalone Test (V6 - Pre-calculated Score) ---")

    # --- Manual Logging Setup ---
    log_level = logging.DEBUG # Keep DEBUG to see details
    log_formatter = logging.Formatter('%(asctime)s - %(name)s [%(levelname)s] %(message)s')
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level)
    console_handler.setFormatter(log_formatter)
    # Configure root logger for SQLAlchemy logs if needed
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level) # Set root logger level
    root_logger.handlers.clear() # Clear any default handlers
    root_logger.addHandler(console_handler) # Add our handler
    # Prevent double logging if __name__ logger also has handler and propagates
    logging.getLogger(__name__).propagate = False
    logger.info(f"Configured loggers for DEBUG level.")
    # --- End Manual Logging Setup ---

    try: from flask import Flask; import sqlalchemy as sa
    except ImportError as e: print(f"ERROR: Flask or SQLAlchemy not found: {e}."); exit(1)

    # Create minimal app for context
    app = Flask(__name__)
    try:
        app.config['SQLALCHEMY_DATABASE_URI'] = getattr(config, 'SQLALCHEMY_DATABASE_URI')
        app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
        # Optional: Add Echo for debugging SQL directly generated by SQLAlchemy
        # app.config['SQLALCHEMY_ECHO'] = True
        logger.info(f"Using DB URI: {app.config['SQLALCHEMY_DATABASE_URI']}")
    except AttributeError: print("ERROR: SQLALCHEMY_DATABASE_URI not found in config.py."); exit(1)
    except Exception as config_err: print(f"ERROR loading config: {config_err}"); exit(1)

    try: db.init_app(app); print("Database initialized.")
    except Exception as db_err: print(f"ERROR initializing database: {db_err}"); exit(1)

    effective_test_mode = getattr(config, 'TEST_MODE', True)
    print(f"\n--- TEST MODE: {'ON (Printing Emails)' if effective_test_mode else 'OFF (Attempting SMTP Send)'} ---")

    # --- Test Execution ---
    TEST_REP_NAME = "Mariano Cruz" # Set to a specific rep name found in your config.SALES_REP_MAPPING
    #TEST_REP_NAME = None # Set to None to test the multi-rep logic

    with app.app_context():
        if TEST_REP_NAME:
            # --- Test Single Rep ---
            test_rep_id = None
            test_rep_email = getattr(config, 'SALES_REP_MAPPING', {}).get(TEST_REP_NAME)
            if not test_rep_email: print(f"ERROR: Email not found for '{TEST_REP_NAME}' in config.SALES_REP_MAPPING.")
            else:
                try: # Find Rep ID
                    stmt = select(AccountPrediction.sales_rep).where(AccountPrediction.sales_rep_name == TEST_REP_NAME).limit(1)
                    result = db.session.execute(stmt).scalar_one_or_none()
                    if result: test_rep_id = result; print(f"Found Rep ID for {TEST_REP_NAME}: {test_rep_id}")
                    else: print(f"WARNING: Could not find Rep ID for {TEST_REP_NAME} in AccountPrediction table.")
                except Exception as db_err: print(f"ERROR: DB error finding Rep ID for {TEST_REP_NAME}: {db_err}")

                if test_rep_id and test_rep_email:
                    print(f"\n--- Testing SINGLE Rep: {TEST_REP_NAME} (ID: {test_rep_id}) ---")
                    try:
                        success = send_weekly_digest_email_for_rep(test_rep_id, TEST_REP_NAME, test_rep_email)
                        if success: print(f"\n--- Test for {TEST_REP_NAME} Completed ---")
                        else: print(f"\n--- Test for {TEST_REP_NAME} Indicated Failure ---")
                    except Exception as e: logger.error(f"Exception during single rep test: {e}", exc_info=True)
                else: print(f"\n--- Skipping single rep test for {TEST_REP_NAME} due to missing ID or Email. ---")
        else:
            # --- Test All Reps ---
             print(f"\n--- Testing ALL Reps ---")
             send_all_weekly_digests()

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