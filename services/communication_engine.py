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
import os


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
    Includes a summary table at the top.
    """
    logger.info(f"Generating HTML Weekly Pacing Digest for Rep: {rep_name} ({rep_id}) Email: {rep_email}")
    if not rep_email:
        logger.error(f"No email address for rep {rep_name} ({rep_id}). Skipping digest.")
        return False

    try:
        today = datetime.datetime.now().date()
        current_year = today.year
        prev_year = current_year - 1
        remaining_full_months = 12 - today.month

        PACING_THRESHOLD_SEVERE = -20.0
        PACING_THRESHOLD_MODERATE = -10.0
        PACING_THRESHOLD_MILD = 0.0

        day_of_month = today.day
        week_num_for_title, section_title, section_description = 0, "", ""
        filter_min_pace, filter_max_pace = 0.0, 0.0
        is_negative_pacing_week, process_this_week = False, True

        # Determine week and section details (same logic as before)
        if 1 <= day_of_month <= 7: # Week 1
            week_num_for_title = 1; section_title = f"Accounts Pacing < {PACING_THRESHOLD_SEVERE:.0f}% vs LY"; section_description = "These accounts are significantly behind their previous year's pace. Focus on understanding reasons and potential immediate actions."; filter_min_pace = -float('inf'); filter_max_pace = PACING_THRESHOLD_SEVERE; is_negative_pacing_week = True
        elif 8 <= day_of_month <= 14: # Week 2
            week_num_for_title = 2; section_title = f"Accounts Pacing {PACING_THRESHOLD_SEVERE:.0f}% to < {PACING_THRESHOLD_MODERATE:.0f}% vs LY"; section_description = "These accounts are moderately behind. Review their recent activity and consider outreach."; filter_min_pace = PACING_THRESHOLD_SEVERE; filter_max_pace = PACING_THRESHOLD_MODERATE; is_negative_pacing_week = True
        elif 15 <= day_of_month <= 21: # Week 3
            week_num_for_title = 3; section_title = f"Accounts Pacing {PACING_THRESHOLD_MODERATE:.0f}% to < {PACING_THRESHOLD_MILD:.0f}% vs LY"; section_description = "These accounts are slightly behind. A small boost could get them on track."; filter_min_pace = PACING_THRESHOLD_MODERATE; filter_max_pace = PACING_THRESHOLD_MILD; is_negative_pacing_week = True
        elif 22 <= day_of_month <= 31: # Week 4 - NOW FOR POSITIVE/GROWTH Accounts
            week_num_for_title = 4
            section_title = f"Top Growth Opportunities: Accounts Pacing ≥ 0% vs LY"
            section_description = "These accounts are performing well! Let's explore opportunities for even greater growth with stretch targets."
            # Define filter for positive pacing
            filter_min_pace = 0.0  # Accounts pacing at 0% or better
            filter_max_pace = float('inf')
            is_negative_pacing_week = False  # This is a positive pacing week
            process_this_week = True
            logger.info(f"Week 4 ({day_of_month}th day) - Report for Positively Pacing Accounts.")
        else: # Should not happen if day_of_month is always 1-31
            logger.warning(f"Unexpected day_of_month: {day_of_month}. Defaulting to no report.")
            process_this_week = False
            subject_quiet = f"Weekly Account Pacing - Wk 4, {today.strftime('%B %Y')} (Unexpected Date)"
            dashboard_link_html = f'<a href="{DASHBOARD_BASE_URL}/strategic" style="color: #007bff; text-decoration: none;">View Your Full Dashboard</a>'
            body_quiet_html = (f"<p style='font-family: Arial, sans-serif; font-size: 14px; line-height: 1.6;'>Hi {rep_name.split()[0]},</p>"
                               f"<p style='font-family: Arial, sans-serif; font-size: 14px; line-height: 1.6;'>There was an unexpected issue with the weekly report scheduling.</p>"
                               f"<p style='font-family: Arial, sans-serif; font-size: 14px; line-height: 1.6;'>Please continue to monitor your accounts via the dashboard: {dashboard_link_html}</p>"
                               f"<p style='font-family: Arial, sans-serif; font-size: 14px; line-height: 1.6;'>Best regards,<br/>The Sales Intelligence Team</p>")
            email_func = print_email_instead_of_sending if config.TEST_MODE else send_email
            email_func(subject=subject_quiet, body=body_quiet_html, recipient=rep_email, is_html=True, from_email=config.FROM_EMAIL, smtp_server=config.SMTP_SERVER, smtp_port=config.SMTP_PORT, username=config.EMAIL_USERNAME, password=config.EMAIL_PASSWORD)
            return True

        if not process_this_week: return True

        logger.debug(f"Today: {today}, Week: {week_num_for_title}, Filter: {filter_min_pace}% to {filter_max_pace}%")

        all_accounts_stmt = select(AccountPrediction).where(AccountPrediction.sales_rep == rep_id)
        rep_accounts_all_objects = db.session.execute(all_accounts_stmt).scalars().all()
        logger.info(f"Found {len(rep_accounts_all_objects)} accounts for {rep_name}.")

        if not rep_accounts_all_objects:
            subject_quiet = f"Your Weekly Pacing Report - Wk {week_num_for_title}, {today.strftime('%B %Y')}"
            dashboard_link_html = f'<a href="{DASHBOARD_BASE_URL}/strategic" style="color: #007bff; text-decoration: none;">View Your Dashboard (if applicable)</a>'
            body_quiet_html = (f"<p style='font-family: Arial, sans-serif; font-size: 14px; line-height: 1.6;'>Hi {rep_name.split()[0]},</p>"
                               f"<p style='font-family: Arial, sans-serif; font-size: 14px; line-height: 1.6;'>No accounts assigned to you were found in the system for this week's pacing report.</p>"
                               f"<p style='font-family: Arial, sans-serif; font-size: 14px; line-height: 1.6;'>🚀 {dashboard_link_html}</p>"
                               f"<p style='font-family: Arial, sans-serif; font-size: 14px; line-height: 1.6;'>Best regards,<br/>The Sales Intelligence Team</p>")
            email_func = print_email_instead_of_sending if config.TEST_MODE else send_email
            email_func(subject=subject_quiet, body=body_quiet_html, recipient=rep_email, is_html=True, from_email=config.FROM_EMAIL, smtp_server=config.SMTP_SERVER, smtp_port=config.SMTP_PORT, username=config.EMAIL_USERNAME, password=config.EMAIL_PASSWORD)
            return True

        py_revenue_map = {}
        account_codes = [acc.canonical_code for acc in rep_accounts_all_objects if acc.canonical_code]
        if account_codes: py_revenue_map = get_previous_year_revenue(account_codes, prev_year)

        email_accounts_for_week = []
        for acc in rep_accounts_all_objects:
            py_rev = py_revenue_map.get(acc.canonical_code, 0.0)
            yep_rev = acc.yep_revenue
            cytd_rev = acc.cytd_revenue
            avg_order_cytd_acc = getattr(acc, 'avg_order_amount_cytd', None)

            current_pace_pct_display = "N/A"; current_pace_pct_numeric = -float('inf'); is_new_account = (py_rev == 0)

            if is_new_account:
                if not ((yep_rev is not None and yep_rev > 0) or (cytd_rev is not None and cytd_rev > 0)): 
                    continue
                else: 
                    continue
            elif yep_rev is None: 
                current_pace_pct_display = "YEP Undefined"
            elif py_rev > 0: 
                current_pace_pct_numeric = ((yep_rev / py_rev) - 1) * 100.0
                current_pace_pct_display = f"{current_pace_pct_numeric:+.1f}%"
            
            passes_filter = False
            if is_negative_pacing_week:
                passes_filter = (week_num_for_title == 1 and current_pace_pct_numeric < filter_max_pace) or \
                                ((week_num_for_title == 2 or week_num_for_title == 3) and filter_min_pace <= current_pace_pct_numeric < filter_max_pace)
            else: # Week 4 - Positive Pacing
                passes_filter = (current_pace_pct_numeric >= filter_min_pace) # Pace >= 0%

            if passes_filter:
                # --- USE PRE-CALCULATED VALUES FROM AccountPrediction OBJECT ---
                target_total_for_year_val = getattr(acc, 'target_yep_plus_1_pct', None)
                amount_needed_val = getattr(acc, 'additional_revenue_needed_eoy', None)
                suggested_next_order_val = getattr(acc, 'suggested_next_purchase_amount', None)
                avg_order_cytd_for_display = getattr(acc, 'avg_order_amount_cytd', None)

                amount_needed_str = "N/A"
                target_to_display_str = "N/A"
                suggested_next_order_size_str = "N/A"

                # Determine the target percentage that was used by the pipeline for this account
                target_percentage_achieved_str = "+1% LY"  # Default assumption
                if target_total_for_year_val and py_rev > 0:
                    if (target_total_for_year_val / py_rev) > 1.05:  # Arbitrary threshold to detect if it was a +10% target
                        target_percentage_achieved_str = "+10% LY (Stretch)"
                elif target_total_for_year_val and (py_rev is None or py_rev == 0) and yep_rev > 0:
                     if (target_total_for_year_val / yep_rev) > 1.05:
                         target_percentage_achieved_str = "+10% YEP (Stretch)"
                     else:
                         target_percentage_achieved_str = "+1% YEP"

                # Format amount needed string
                if amount_needed_val is not None:
                    if amount_needed_val > 0:
                        amount_needed_str = format_currency(amount_needed_val)
                    else: # Target met or exceeded
                        amount_needed_str = f"Target ({target_percentage_achieved_str}) Met!"
                
                # Format target display string
                if target_total_for_year_val is not None:
                    target_to_display_str = format_currency_short(target_total_for_year_val)
                
                # Format suggested next order string
                if suggested_next_order_val is not None and suggested_next_order_val > 0:
                    suggested_next_order_size_str = format_currency(suggested_next_order_val)
                elif amount_needed_val is not None and amount_needed_val <= 0: # If target met
                    suggested_next_order_size_str = "Maintain Momentum!"
                # --- END PRE-CALCULATED VALUES SECTION ---

                email_accounts_for_week.append({
                    'name': getattr(acc, 'name', 'N/A'), 
                    'canonical_code': acc.canonical_code, 
                    'base_card_code': getattr(acc, 'base_card_code', 'N/A'), 
                    'full_address': getattr(acc, 'full_address', 'N/A'), 
                    'pace_display': current_pace_pct_display, 
                    'pace_numeric': current_pace_pct_numeric, 
                    'py_rev_short': format_currency_short(py_rev),
                    'yep_rev_short': format_currency_short(yep_rev) if yep_rev is not None else "N/A",
                    'cytd_rev_short': format_currency_short(cytd_rev) if cytd_rev is not None else "N/A",
                    'amount_needed_str': amount_needed_str, # Now from acc.additional_revenue_needed_eoy
                    'target_overall_display_str': target_to_display_str, # Renamed for clarity in email template
                    'target_percentage_achieved_str': target_percentage_achieved_str, # New field for email context
                    'suggested_next_order_size_str': suggested_next_order_size_str, # From acc.suggested_next_purchase_amount
                    'avg_order_cytd_str': format_currency(avg_order_cytd_for_display) if avg_order_cytd_for_display else "N/A"
                })

        if is_negative_pacing_week: 
            email_accounts_for_week.sort(key=lambda x: x['pace_numeric'], reverse=True)
        else: # Week 4 - Sort by highest positive pace first
            email_accounts_for_week.sort(key=lambda x: x['pace_numeric'], reverse=True)

        if not email_accounts_for_week:
            subject_quiet = f"Your Weekly Pacing Report - Wk {week_num_for_title}, {today.strftime('%B %Y')}"
            dashboard_link_html = f'<a href="{DASHBOARD_BASE_URL}/strategic" style="color: #007bff; text-decoration: none;">View Your Full Dashboard</a>'
            body_quiet_html = (f"<p style='font-family: Arial, sans-serif; font-size: 14px; line-height: 1.6;'>Hi {rep_name.split()[0]},</p>"
                               f"<p style='font-family: Arial, sans-serif; font-size: 14px; line-height: 1.6;'>No accounts met the criteria for this week's pacing report (Week {week_num_for_title}: {section_title}).</p>"
                               f"<p style='font-family: Arial, sans-serif; font-size: 14px; line-height: 1.6;'>🚀 {dashboard_link_html}</p>"
                               f"<p style='font-family: Arial, sans-serif; font-size: 14px; line-height: 1.6;'>Best regards,<br/>The Sales Intelligence Team</p>")
            email_func = print_email_instead_of_sending if config.TEST_MODE else send_email
            email_func(subject=subject_quiet, body=body_quiet_html, recipient=rep_email, is_html=True, from_email=config.FROM_EMAIL, smtp_server=config.SMTP_SERVER, smtp_port=config.SMTP_PORT, username=config.EMAIL_USERNAME, password=config.EMAIL_PASSWORD)
            return True

        # === Build Main HTML Email Body ===
        rep_first_name = rep_name.split()[0]  # Extract first name
        subject = f"✨ Winner!! Your Week {week_num_for_title} Pacing Report."
        full_dashboard_link_html = f'<a href="{DASHBOARD_BASE_URL}/strategic" style="color: #007bff; text-decoration: none;">View Your Full Dashboard</a>'
        
        body_html_lines = [
            f"<p style='font-family: Arial, sans-serif; font-size: 14px; line-height: 1.6;'>Hi {rep_name.split()[0]},</p>",
            f"<p style='font-family: Arial, sans-serif; font-size: 14px; line-height: 1.6;'>Here is your account pacing report for Week {week_num_for_title} of {today.strftime('%B')}.</p>",
            f"<p style='font-family: Arial, sans-serif; font-size: 14px; line-height: 1.6;'>🚀 {full_dashboard_link_html}</p><br/>",
        ]

        # --- Generate Summary Table HTML ---
        if email_accounts_for_week:
            summary_table_html = "<h3 style='font-family: Arial, sans-serif; color: #333;'>Summary of Accounts in this Report:</h3>"
            summary_table_html += "<table border='1' cellpadding='5' cellspacing='0' style='border-collapse: collapse; width: 100%; font-family: Arial, sans-serif; font-size: 12px;'>"
            summary_table_html += "<thead style='background-color: #f2f2f2;'>"
            header_needed_col = "Needed for Growth Target" if not is_negative_pacing_week else "Needed for +1% Target"
            # --- MODIFICATION START ---
            summary_table_html += f"<tr><th>#</th><th>Account Name</th><th>Card Code</th><th>Pace vs LY</th><th>YEP</th><th>{header_needed_col}</th><th>Suggested Next Order</th></tr>"
            # --- MODIFICATION END ---
            summary_table_html += "</thead><tbody>"
            
            for i, acc_data in enumerate(email_accounts_for_week):
                account_detail_link = f"{DASHBOARD_BASE_URL}/account/{acc_data['canonical_code']}"
                summary_table_html += f"<tr>"
                summary_table_html += f"<td style='text-align: center;'>{i+1}</td>"
                summary_table_html += f"<td><a href='{account_detail_link}' style='color: #007bff; text-decoration: none;'>{acc_data['name']}</a></td>"
                # --- MODIFICATION START ---
                summary_table_html += f"<td style='text-align: left;'>{acc_data['base_card_code']}</td>"
                # --- MODIFICATION END ---
                summary_table_html += f"<td style='text-align: right;'>{acc_data['pace_display']}</td>"
                summary_table_html += f"<td style='text-align: right;'>{acc_data['yep_rev_short']}</td>"
                summary_table_html += f"<td style='text-align: right;'>{acc_data['amount_needed_str']}</td>"
                summary_table_html += f"<td style='text-align: right;'>{acc_data['suggested_next_order_size_str']}</td>"
                summary_table_html += f"</tr>"
            
            summary_table_html += "</tbody></table><br/><hr style='border: 0; border-top: 1px solid #eee;'/><br/>"
            body_html_lines.append(summary_table_html)

        body_html_lines.extend([
            f"<h2 style='font-family: Arial, sans-serif; color: #333;'>{section_title.upper()} - Detailed View</h2>",
            f"<p style='font-family: Arial, sans-serif; font-size: 14px; line-height: 1.6;'><em>{section_description}</em></p>"
        ])

        if is_negative_pacing_week and remaining_full_months >= 0:
             month_str = "month" if remaining_full_months == 1 else "months"
             if remaining_full_months > 0: body_html_lines.append(f"<p style='font-family: Arial, sans-serif; font-size: 14px; line-height: 1.6;'><em>You have approximately <strong>{remaining_full_months} full {month_str}</strong> remaining in the year to reach these targets.</em></p>")
             elif today.month == 12: body_html_lines.append(f"<p style='font-family: Arial, sans-serif; font-size: 14px; line-height: 1.6;'><em>This is the final month to reach these targets.</em></p>")
        body_html_lines.append("<br/>")

        body_html_lines.append("<ul style='list-style-type: none; padding-left: 0; font-family: Arial, sans-serif; font-size: 14px;'>")
        display_limit = 20
        for i, acc_data in enumerate(email_accounts_for_week[:display_limit]):
            account_detail_link_html = f'<a href="{DASHBOARD_BASE_URL}/account/{acc_data["canonical_code"]}" style="color: #007bff; text-decoration: none;">Details</a>'
            
            item_html = f"<li style='margin-bottom: 20px; padding-bottom: 10px; border-bottom: 1px solid #eee;'>"
            item_html += f"<strong style='font-size: 1.1em;'>{i+1}. {acc_data['name']}</strong> ({account_detail_link_html})<br/>"
            item_html += f"    <span style='color: #555;'>CardCode:</span> {acc_data['base_card_code']}<br/>"
            item_html += f"    <span style='color: #555;'>Address:</span> {acc_data['full_address']}<br/>"
            item_html += f"    <span style='color: #555;'>Pacing:</span> <strong style='color: {'#dc3545' if acc_data['pace_numeric'] < 0 else '#28a745'};'>{acc_data['pace_display']}</strong> (YEP: {acc_data['yep_rev_short']} vs PY: {acc_data['py_rev_short']})<br/>"
            if is_negative_pacing_week:
                item_html += f"    <span style='color: #555;'>Needed to Reach +1% Target:</span> <strong style='color: #ffc107;'>{acc_data['amount_needed_str']}</strong> (Overall +1% Target: {acc_data['target_overall_display_str']}, Current YTD: {acc_data['cytd_rev_short']})"
            else: # Week 4 - Growth opportunities
                item_html += f"    <span style='color: #555;'>Goal:</span> Reach {acc_data['target_overall_display_str']} ({acc_data['target_percentage_achieved_str']}) | <span style='color: #555;'>Still Needed:</span> <strong style='color: #ffc107;'>{acc_data['amount_needed_str']}</strong>"


            if acc_data['suggested_next_order_size_str'] != "N/A" and acc_data['amount_needed_str'] != "Target Met!":
                suggestion_line = f"<br/>    <span style='color: #17a2b8;'>Consider Next Order of:</span> ~<strong style='color: #17a2b8;'>{acc_data['suggested_next_order_size_str']}</strong>"
                if acc_data['avg_order_cytd_str'] != "N/A":
                    suggestion_line += f" <span style='font-size:0.9em; color: #6c757d;'>(Typical Order: {acc_data['avg_order_cytd_str']})</span>"
                item_html += suggestion_line
            item_html += "</li>"
            body_html_lines.append(item_html)
        body_html_lines.append("</ul>")

        if len(email_accounts_for_week) > display_limit:
            view_all_link_html = f'<a href="{DASHBOARD_BASE_URL}/strategic?sales_rep={rep_id}&filter=pacing_w{week_num_for_title}" style="color: #007bff; text-decoration: none;">View All on Dashboard</a>'
            body_html_lines.append(f"<p style='font-family: Arial, sans-serif; font-size: 14px; line-height: 1.6;'><em>... and {len(email_accounts_for_week) - display_limit} more. {view_all_link_html}</em></p>")

        body_html_lines.append("<hr style='border: 0; border-top: 1px solid #eee;'/><p style='font-family: Arial, sans-serif; font-size: 14px; line-height: 1.6;'>Best regards,<br/>The Sales Intelligence Team</p>")
        html_body = "\n".join(body_html_lines)
        
        html_body_wrapper = f"<div style='font-family: Arial, sans-serif; font-size: 14px; color: #333; line-height: 1.6;'>{html_body}</div>"

        email_func = print_email_instead_of_sending if config.TEST_MODE else send_email
        log_prefix = "[TEST MODE] Would print HTML" if config.TEST_MODE else "Sent HTML"
        
        success = email_func(
            subject=subject, body=html_body_wrapper, recipient=rep_email, is_html=True, 
            from_email=config.FROM_EMAIL, smtp_server=config.SMTP_SERVER,
            smtp_port=config.SMTP_PORT, username=config.EMAIL_USERNAME,
            password=config.EMAIL_PASSWORD
        )
        if success:
            logger.info(f"{log_prefix} weekly pacing digest (Week {week_num_for_title}) to {rep_email} ({len(email_accounts_for_week)} accounts included in report).")
            return True
        else:
            logger.error(f"Email function returned False for HTML weekly pacing digest to {rep_email}.")
            return False

    except Exception as e:
        logger.error(f"Error generating HTML weekly pacing digest for rep {rep_name} ({rep_id}): {str(e)}", exc_info=True)
        try:
            if db.session.is_active: db.session.rollback(); logger.warning("Rolled back DB session.")
        except Exception as rb_err: logger.error(f"Error during DB rollback: {rb_err}")
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

'''
# --- Standalone Test Block (Updated for New Pacing Digest) ---
if __name__ == "__main__":
    print("--- Running Communication Engine Standalone Test (New Pacing Digest) ---")

    # --- Basic Logging Setup for Standalone Test ---
    log_level = logging.DEBUG # Or logging.INFO for less verbosity
    log_formatter = logging.Formatter('%(asctime)s - %(name)s [%(levelname)s] %(filename)s:%(lineno)d - %(message)s')
    
    # Configure console handler
    console_handler = logging.StreamHandler(sys.stdout) # Ensure sys is imported
    console_handler.setLevel(log_level)
    console_handler.setFormatter(log_formatter)

    # Configure root logger (if not already configured by Flask app context in other scenarios)
    root_logger = logging.getLogger()
    if not root_logger.hasHandlers(): # Add handler only if no handlers are configured
        root_logger.addHandler(console_handler)
    root_logger.setLevel(log_level) # Set root logger level

    # Configure module-level logger
    logger.setLevel(log_level) # Ensures the 'logger = logging.getLogger(__name__)' uses this level
    if not logger.hasHandlers(): # Add handler to module logger if it doesn't have one (e.g. if propagate is False)
        logger.addHandler(console_handler)
    # logger.propagate = False # Optional: uncomment if you want to prevent double logging from root

    logger.info("Configured loggers for standalone test.")
    # --- End Logging Setup ---

    try:
        from flask import Flask
        # It's good practice to also import 'os' here if not already at the top of the file
        import os # Make sure os is imported
    except ImportError as e:
        print(f"ERROR: Flask or os module not found: {e}.")
        # logger.critical(f"Flask or os module not found: {e}.") # Use logger if it's reliably set up by now
        exit(1)

    app = Flask(__name__)
    try:
        # --- Database URI Configuration: Prioritize Environment Variable ---
        db_uri_from_env = os.environ.get('SQLALCHEMY_DATABASE_URI')
        if db_uri_from_env:
            app.config['SQLALCHEMY_DATABASE_URI'] = db_uri_from_env
            logger.info(f"Using DB URI from ENVIRONMENT VARIABLE: {app.config['SQLALCHEMY_DATABASE_URI']}")
        elif hasattr(config, 'SQLALCHEMY_DATABASE_URI'): # Check if attribute exists in config object
            app.config['SQLALCHEMY_DATABASE_URI'] = getattr(config, 'SQLALCHEMY_DATABASE_URI')
            logger.info(f"Using DB URI from config.py: {app.config['SQLALCHEMY_DATABASE_URI']}")
        else:
            # This handles case where config object exists but is missing the URI, and no ENV var
            logger.error("SQLALCHEMY_DATABASE_URI not found in config.py and not set as ENV variable.")
            print("ERROR: SQLALCHEMY_DATABASE_URI not found in config.py and not set as ENV variable.")
            exit(1)
        # --- End Database URI Configuration ---

        app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
        # For debugging SQL queries generated by SQLAlchemy:
        # app.config['SQLALCHEMY_ECHO'] = True
        
    except AttributeError: # This specifically catches if 'config' itself or 'SQLALCHEMY_DATABASE_URI' on it is missing
        logger.error("AttributeError: SQLALCHEMY_DATABASE_URI not found in config.py (and not set as ENV). Ensure config.py is loaded and contains the URI.")
        print("ERROR: SQLALCHEMY_DATABASE_URI not found in config.py (and not set as ENV).")
        exit(1)
    except Exception as config_err:
        logger.error(f"Error loading Flask app configuration: {config_err}", exc_info=True)
        print(f"ERROR loading Flask app configuration: {config_err}")
        exit(1)

    try:
        db.init_app(app) # Ensure db is imported from models
        logger.info("Database initialized with Flask app for standalone test.")
        print("Database initialized with Flask app.")
    except Exception as db_err:
        logger.error(f"Error initializing database with Flask app: {db_err}", exc_info=True)
        print(f"ERROR initializing database with Flask app: {db_err}")
        exit(1)

    effective_test_mode = getattr(config, 'TEST_MODE', True)
    print(f"\n--- TEST MODE: {'ON (Printing Emails)' if effective_test_mode else 'OFF (Attempting SMTP Send)'} ---")

    # --- Test Execution ---
    # To test a specific rep, uncomment and set their name from your SALES_REP_MAPPING
    # TEST_REP_NAME = "Mariano Cruz" 
    TEST_REP_NAME = None # Set to None to test the multi-rep logic (send_all_weekly_digests)

    with app.app_context():
        logger.info("App context established for testing.")
        print("App context established for testing.")
        
        # Store the original datetime.datetime class to restore it later
        original_datetime_module_class = datetime.datetime 

        # --- Optional: Mocking datetime.datetime.now() for specific week testing ---
        # Uncomment and modify the MockDateTime class to test different weeks
        class MockDateTime(datetime.datetime): 
             @classmethod
             def now(cls, tz=None):
                 # Example: Test Week 1 (e.g., Jan 3rd of current year)
                 current_real_year = original_datetime_module_class.now().year
                 return original_datetime_module_class(current_real_year, 1, 3, tzinfo=tz)
                 #return original_datetime_module_class(current_real_year, 1, 10, tzinfo=tz) # Week 2
                 #return original_datetime_module_class(current_real_year, 1, 17, tzinfo=tz) # Week 3
                # return original_datetime_module_class(current_real_year, 1, 24, tzinfo=tz) # Week 4 (currently disabled in email logic)
        
        # # Apply the mock
        datetime.datetime = MockDateTime 
        logger.info(f"MOCKING datetime.datetime.now() to return date: {datetime.datetime.now().date()} for testing specific week logic.")
        # --- End Optional Date Mocking ---
        
        if TEST_REP_NAME:
            test_rep_id = None
            # Ensure SALES_REP_MAPPING is correctly loaded from config
            sales_rep_mapping = getattr(config, 'SALES_REP_MAPPING', {})
            test_rep_email = sales_rep_mapping.get(TEST_REP_NAME)

            if not test_rep_email:
                logger.error(f"Email not found for '{TEST_REP_NAME}' in config.SALES_REP_MAPPING.")
                print(f"ERROR: Email not found for '{TEST_REP_NAME}' in config.SALES_REP_MAPPING.")
            else:
                try:
                    # Ensure AccountPrediction model is correctly imported and db session is active
                    stmt = select(AccountPrediction.sales_rep).where(AccountPrediction.sales_rep_name == TEST_REP_NAME).limit(1)
                    result = db.session.execute(stmt).scalar_one_or_none()
                    if result:
                        test_rep_id = result
                        logger.info(f"Found Rep ID for {TEST_REP_NAME}: {test_rep_id}")
                        print(f"Found Rep ID for {TEST_REP_NAME}: {test_rep_id}")
                    else:
                        logger.warning(f"Could not find Rep ID for {TEST_REP_NAME} in AccountPrediction table.")
                        print(f"WARNING: Could not find Rep ID for {TEST_REP_NAME} in AccountPrediction table.")
                except Exception as db_err_rep_find: # More specific error name
                    logger.error(f"DB error finding Rep ID for {TEST_REP_NAME}: {db_err_rep_find}", exc_info=True)
                    print(f"ERROR: DB error finding Rep ID for {TEST_REP_NAME}: {db_err_rep_find}")

                if test_rep_id and test_rep_email:
                    logger.info(f"--- Testing SINGLE Rep Pacing Digest: {TEST_REP_NAME} (ID: {test_rep_id}) ---")
                    print(f"\n--- Testing SINGLE Rep Pacing Digest: {TEST_REP_NAME} (ID: {test_rep_id}) ---")
                    try:
                        success = send_weekly_digest_email_for_rep(test_rep_id, TEST_REP_NAME, test_rep_email)
                        if success:
                            logger.info(f"Test for {TEST_REP_NAME} indicated SUCCESS.")
                            print(f"\n--- Test for {TEST_REP_NAME} Indicated Success ---")
                        else:
                            logger.warning(f"Test for {TEST_REP_NAME} indicated FAILURE.")
                            print(f"\n--- Test for {TEST_REP_NAME} Indicated Failure ---")
                    except Exception as e_single_test: # More specific error name
                        logger.error(f"Exception during single rep pacing digest test for {TEST_REP_NAME}: {e_single_test}", exc_info=True)
                else:
                    logger.warning(f"Skipping single rep pacing digest test for {TEST_REP_NAME} due to missing ID or Email.")
                    print(f"\n--- Skipping single rep pacing digest test for {TEST_REP_NAME} due to missing ID or Email. ---")
        else:
            logger.info("--- Testing ALL Reps Pacing Digest ---")
            print(f"\n--- Testing ALL Reps Pacing Digest ---")
            send_all_weekly_digests() # Ensure this function is correctly defined in the file
        
        # Restore the original datetime.datetime class if it was mocked
        if datetime.datetime != original_datetime_module_class: # Check if it was actually changed
            datetime.datetime = original_datetime_module_class
            logger.info("Restored original datetime.datetime class.")
        # --- End Restore Date Mocking ---

    logger.info("--- Standalone Pacing Digest Test Finished ---")
    print("\n--- Standalone Pacing Digest Test Finished ---")
'''

'''
if __name__ == "__main__":
    print("--- Running Communication Engine Standalone Test (New Pacing Digest) ---")

    # --- Basic Logging Setup for Standalone Test ---
    # ... (logging setup as before, ensure it's comprehensive) ...
    log_level = logging.DEBUG 
    log_formatter = logging.Formatter('%(asctime)s - %(name)s [%(levelname)s] %(filename)s:%(lineno)d - %(message)s')
    console_handler = logging.StreamHandler(sys.stdout) 
    console_handler.setLevel(log_level)
    console_handler.setFormatter(log_formatter)
    root_logger = logging.getLogger()
    if not root_logger.hasHandlers(): 
        root_logger.addHandler(console_handler)
    root_logger.setLevel(log_level) 
    logger.setLevel(log_level) 
    if not logger.hasHandlers(): 
        logger.addHandler(console_handler)
    logger.info("Configured loggers for standalone test.")
    # --- End Logging Setup ---

    try:
        from flask import Flask
        import os 
    except ImportError as e:
        print(f"ERROR: Flask or os module not found: {e}.")
        exit(1)

    app = Flask(__name__)
    try:
        db_uri_from_env = os.environ.get('SQLALCHEMY_DATABASE_URI')
        if db_uri_from_env:
            app.config['SQLALCHEMY_DATABASE_URI'] = db_uri_from_env
            logger.info(f"Using DB URI from ENVIRONMENT VARIABLE: {app.config['SQLALCHEMY_DATABASE_URI']}")
        elif hasattr(config, 'SQLALCHEMY_DATABASE_URI') and getattr(config, 'SQLALCHEMY_DATABASE_URI'):
            app.config['SQLALCHEMY_DATABASE_URI'] = getattr(config, 'SQLALCHEMY_DATABASE_URI')
            logger.info(f"Using DB URI from config.py: {app.config['SQLALCHEMY_DATABASE_URI']}")
        else:
            logger.error("SQLALCHEMY_DATABASE_URI not found in config.py and not set as ENV variable.")
            print("ERROR: SQLALCHEMY_DATABASE_URI not found in config.py and not set as ENV variable.")
            exit(1)
        app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
    except Exception as config_err:
        logger.error(f"Error loading Flask app configuration: {config_err}", exc_info=True)
        print(f"ERROR loading Flask app configuration: {config_err}")
        exit(1)

    try:
        db.init_app(app) 
        logger.info("Database initialized with Flask app for standalone test.")
        print("Database initialized with Flask app.")
    except Exception as db_err:
        logger.error(f"Error initializing database with Flask app: {db_err}", exc_info=True)
        print(f"ERROR initializing database with Flask app: {db_err}")
        exit(1)

    # --- IMPORTANT: Configure for actual email sending for this test ---
    # Make sure your config.py or environment variables have correct email settings.
    # For this specific test, we will force TEST_MODE to False temporarily if needed.
    # However, it's better to control this via your actual config.TEST_MODE.
    
    # If you want to force sending emails regardless of config.TEST_MODE for this test run:
    # config.TEST_MODE = False 
    # logger.warning("FORCED config.TEST_MODE = False for this test run to send actual emails.")
    # print("WARNING: FORCED config.TEST_MODE = False for this test run to send actual emails.")
    # ELSE, it will respect your config.py setting.

    effective_test_mode = getattr(config, 'TEST_MODE', True) # Default to True if not set
    print(f"\n--- TEST MODE: {'ON (Printing Emails)' if effective_test_mode else 'OFF (Attempting SMTP Send)'} ---")
    if effective_test_mode:
        print("INFO: To send actual emails to Natasha, ensure config.TEST_MODE is False in your config.py or environment.")

    YOUR_TEST_EMAIL = "natasha@irwinnaturals.com"
    print(f"INFO: Emails for this test run will be directed to: {YOUR_TEST_EMAIL}")

    with app.app_context():
        logger.info("App context established for testing.")
        print("App context established for testing.")
        
        original_datetime_module_class = datetime.datetime 
        # --- Optional: Mocking datetime.datetime.now() for specific week testing ---
        class MockDateTime(datetime.datetime): 
              @classmethod
              def now(cls, tz=None):
                  current_real_year = original_datetime_module_class.now().year
        #          # return original_datetime_module_class(current_real_year, 1, 3, tzinfo=tz) # Week 1
        #          # return original_datetime_module_class(current_real_year, 1, 10, tzinfo=tz) # Week 2
        #          #return original_datetime_module_class(current_real_year, 1, 17, tzinfo=tz) # Week 3 Example
                  return original_datetime_module_class(current_real_year, 1, 24, tzinfo=tz) # Week 4 (22nd-31st)

        datetime.datetime = MockDateTime 
        logger.info(f"MOCKING datetime.datetime.now() to return date: {datetime.datetime.now().date()} for testing specific week logic.")
        # --- End Optional Date Mocking ---
        
        # --- Fetch ALL reps to test with, but send email to YOUR_TEST_EMAIL ---
        logger.info("--- Testing Digest Emails (will be sent to Natasha) ---")
        print(f"\n--- Testing Digest Emails (will be sent to {YOUR_TEST_EMAIL}) ---")
        
        reps_to_process_for_data = []
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
                # We still use their actual data, but the email recipient will be overridden
                reps_to_process_for_data.append({'id': rep_id, 'name': rep_name})
            
            logger.info(f"Found {len(reps_to_process_for_data)} distinct reps in the database to generate data for.")
            if not reps_to_process_for_data:
                logger.warning("No reps found in the database to generate test emails.")
                print("WARNING: No reps found to generate test emails.")

        except Exception as e_fetch_reps:
            logger.error(f"Error fetching reps for testing: {e_fetch_reps}", exc_info=True)
            print(f"ERROR fetching reps: {e_fetch_reps}")
        
        processed_count = 0
        failed_count = 0
        # You might want to limit how many reps' emails are generated and sent to you during testing
        # For example, just the first one or a specific one.
        TEST_SPECIFIC_REP_NAME_FOR_DATA = "Mariano Cruz" # Or None to try all found reps
        #TEST_SPECIFIC_REP_NAME_FOR_DATA = None 


        for rep_info in reps_to_process_for_data:
            if TEST_SPECIFIC_REP_NAME_FOR_DATA and rep_info['name'] != TEST_SPECIFIC_REP_NAME_FOR_DATA:
                continue # Skip if testing for a specific rep and this isn't them

            logger.info(f"--- Generating email for Rep: {rep_info['name']} (ID: {rep_info['id']}) ---")
            print(f"\n--- Generating email for Rep: {rep_info['name']} (ID: {rep_info['id']}) ---")
            try:
                # Call the function with the actual rep's data, but override the recipient
                success = send_weekly_digest_email_for_rep(
                    rep_info['id'], 
                    rep_info['name'], 
                    YOUR_TEST_EMAIL # <<< Email sent to Natasha
                )
                if success:
                    logger.info(f"Digest email for {rep_info['name']}'s data sent/printed for {YOUR_TEST_EMAIL} successfully.")
                    print(f"Digest email for {rep_info['name']}'s data sent/printed for {YOUR_TEST_EMAIL} successfully.")
                    processed_count +=1
                else:
                    logger.warning(f"Digest email for {rep_info['name']}'s data to {YOUR_TEST_EMAIL} FAILED to send/print.")
                    print(f"Digest email for {rep_info['name']}'s data to {YOUR_TEST_EMAIL} FAILED to send/print.")
                    failed_count += 1
                
                time.sleep(2) # Pause between emails if sending multiple

                if TEST_SPECIFIC_REP_NAME_FOR_DATA: # If testing only one, break after the first
                    break

            except Exception as e_single_test:
                logger.error(f"Exception during digest generation/send for {rep_info['name']}: {e_single_test}", exc_info=True)
                failed_count += 1
        
        logger.info(f"Test email generation complete. Processed: {processed_count}, Failed: {failed_count}.")
        print(f"Test email generation complete. Processed: {processed_count}, Failed: {failed_count}.")

        if datetime.datetime != original_datetime_module_class:
            datetime.datetime = original_datetime_module_class
            logger.info("Restored original datetime.datetime class.")

    logger.info("--- Standalone Pacing Digest Test Finished ---")
    print("\n--- Standalone Pacing Digest Test Finished ---")



if __name__ == "__main__":
    print("--- Running Communication Engine Standalone Test: Send all 4 weekly emails ---")

    # --- Basic Logging Setup for Standalone Test ---
    log_level = logging.DEBUG 
    log_formatter = logging.Formatter('%(asctime)s - %(name)s [%(levelname)s] %(filename)s:%(lineno)d - %(message)s')
    console_handler = logging.StreamHandler(sys.stdout) 
    console_handler.setLevel(log_level)
    console_handler.setFormatter(log_formatter)
    root_logger = logging.getLogger()
    if not root_logger.hasHandlers(): 
        root_logger.addHandler(console_handler)
    root_logger.setLevel(log_level) 
    logger.setLevel(log_level) 
    if not logger.hasHandlers(): 
        logger.addHandler(console_handler)
    logger.info("Configured loggers for standalone test.")
    # --- End Logging Setup ---

    try:
        from flask import Flask
        import os 
    except ImportError as e:
        print(f"ERROR: Flask or os module not found: {e}.")
        exit(1)

    app = Flask(__name__)
    try:
        db_uri_from_env = os.environ.get('SQLALCHEMY_DATABASE_URI')
        if db_uri_from_env:
            app.config['SQLALCHEMY_DATABASE_URI'] = db_uri_from_env
            logger.info(f"Using DB URI from ENVIRONMENT VARIABLE.")
        elif hasattr(config, 'SQLALCHEMY_DATABASE_URI') and getattr(config, 'SQLALCHEMY_DATABASE_URI'):
            app.config['SQLALCHEMY_DATABASE_URI'] = getattr(config, 'SQLALCHEMY_DATABASE_URI')
            logger.info(f"Using DB URI from config.py.")
        else:
            logger.error("SQLALCHEMY_DATABASE_URI not found in config.py and not set as ENV variable.")
            print("ERROR: SQLALCHEMY_DATABASE_URI not found.")
            exit(1)
        app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
    except Exception as config_err:
        logger.error(f"Error loading Flask app configuration: {config_err}", exc_info=True)
        print(f"ERROR loading Flask app configuration: {config_err}")
        exit(1)

    try:
        db.init_app(app) 
        logger.info("Database initialized with Flask app for standalone test.")
        print("Database initialized with Flask app.")
    except Exception as db_err:
        logger.error(f"Error initializing database with Flask app: {db_err}", exc_info=True)
        print(f"ERROR initializing database with Flask app: {db_err}")
        exit(1)

    # --- TEST CONFIGURATION ---
    TEST_RECIPIENT_EMAIL = "natasha@irwinnaturals.com"
    TEST_REP_NAME_FOR_DATA = "Mariano Cruz"
    WEEK_TEST_DAYS = {1: 3, 2: 10, 3: 17, 4: 24} # Day of the month to simulate for each week

    effective_test_mode = getattr(config, 'TEST_MODE', True)
    print("\n" + "="*60)
    print(f"TEST MODE: {'ON (Printing Emails to Console)' if effective_test_mode else 'OFF (Attempting Real SMTP Send)'}")
    if effective_test_mode:
        print("!!! IMPORTANT: To send actual emails, set TEST_MODE = False in config.py")
    print(f"Test emails will be sent to: {TEST_RECIPIENT_EMAIL}")
    print(f"Email content will be generated using data for: {TEST_REP_NAME_FOR_DATA}")
    print("="*60 + "\n")
    
    with app.app_context():
        logger.info("App context established for testing.")
        
        # --- Find the Rep ID for the test data ---
        test_rep_id = None
        try:
            stmt = select(AccountPrediction.sales_rep).where(AccountPrediction.sales_rep_name == TEST_REP_NAME_FOR_DATA).limit(1)
            result = db.session.execute(stmt).scalar_one_or_none()
            if result:
                test_rep_id = result
                logger.info(f"Found Rep ID for {TEST_REP_NAME_FOR_DATA}: {test_rep_id}")
            else:
                logger.error(f"Could not find Rep ID for '{TEST_REP_NAME_FOR_DATA}'. Cannot proceed.")
                print(f"ERROR: Could not find Rep ID for '{TEST_REP_NAME_FOR_DATA}'. Please check the name and database.")
                exit(1)
        except Exception as e_fetch_rep:
            logger.error(f"DB error finding Rep ID for {TEST_REP_NAME_FOR_DATA}: {e_fetch_rep}", exc_info=True)
            exit(1)

        original_datetime_class = datetime.datetime
        
        for week_num, day_to_simulate in WEEK_TEST_DAYS.items():
            print(f"\n--- PROCESSING WEEK {week_num} (simulating day {day_to_simulate}) ---")
            
            # --- Mock datetime.datetime.now() for this specific week ---
            class MockDateTime(datetime.datetime):
                @classmethod
                def now(cls, tz=None):
                    current_real_year = original_datetime_class.now().year
                    # Simulate a date in the current month to make the email text relevant
                    current_real_month = original_datetime_class.now().month
                    return original_datetime_class(current_real_year, current_real_month, day_to_simulate, tzinfo=tz)
            
            datetime.datetime = MockDateTime
            logger.info(f"MOCKING datetime.now() to: {datetime.datetime.now().date()} for Week {week_num} test.")

            try:
                success = send_weekly_digest_email_for_rep(
                    test_rep_id, 
                    TEST_REP_NAME_FOR_DATA, 
                    TEST_RECIPIENT_EMAIL
                )
                if success:
                    print(f"✅ SUCCESS: Week {week_num} email for {TEST_REP_NAME_FOR_DATA} was generated and sent/printed.")
                else:
                    print(f"❌ FAILED: Week {week_num} email generation failed. Check logs for details.")
            except Exception as e_test_run:
                logger.error(f"Exception during Week {week_num} test for {TEST_REP_NAME_FOR_DATA}: {e_test_run}", exc_info=True)
                print(f"❌ EXCEPTION on Week {week_num}. See logs for details.")

            # Pause between sending emails
            if not effective_test_mode:
                print("Pausing for 5 seconds before next email...")
                time.sleep(5)
            
        # --- Restore the original datetime class ---
        datetime.datetime = original_datetime_class
        logger.info("Restored original datetime.datetime class.")
        print("\n--- All weekly tests complete. ---")

'''

if __name__ == "__main__":
    # --- This test sends Week 2 digest emails for a specific list of reps to Natasha ---
    try:
        from dotenv import load_dotenv
        load_dotenv()
        print("--- .env file loaded successfully ---")
    except ImportError:
        print("--- WARNING: python-dotenv not installed. Run 'pip install python-dotenv'. ---")

    print("--- Running Communication Engine Standalone Test: Send Week 2 Digest Emails ---")

    # --- Basic Logging Setup ---
    log_level = logging.INFO 
    log_formatter = logging.Formatter('%(asctime)s - %(name)s [%(levelname)s] - %(message)s')
    console_handler = logging.StreamHandler(sys.stdout) 
    console_handler.setLevel(log_level)
    console_handler.setFormatter(log_formatter)
    root_logger = logging.getLogger()
    if not root_logger.hasHandlers(): 
        root_logger.addHandler(console_handler)
    root_logger.setLevel(log_level) 
    logger.setLevel(log_level) 
    
    # --- Flask App and DB Setup ---
    try:
        from flask import Flask
        import os 
        import time
    except ImportError as e:
        print(f"ERROR: Required modules not found: {e}.")
        exit(1)

    app = Flask(__name__)
    try:
        db_uri = os.environ.get('SQLALCHEMY_DATABASE_URI')
        if not db_uri:
             raise ValueError("SQLALCHEMY_DATABASE_URI not found in environment or .env file.")
        app.config['SQLALCHEMY_DATABASE_URI'] = db_uri
        app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
        logger.info("Database URI loaded successfully.")
    except Exception as config_err:
        logger.error(f"Error loading Flask app configuration: {config_err}", exc_info=True)
        exit(1)

    try:
        db.init_app(app) 
        logger.info("Database initialized with Flask app for standalone test.")
    except Exception as db_err:
        logger.error(f"Error initializing database with Flask app: {db_err}", exc_info=True)
        exit(1)

    # --- TEST CONFIGURATION ---
    TEST_RECIPIENT_EMAIL = "natasha@irwinnaturals.com"
    REPS_TO_TEST = [
        "Ashley Bolanos",
        "Christina Antrim",
        "Lisa Clarke",
        "Liz Paz",
        "Mariano Cruz",
        "Trina Hilley",
        "Donald Corgill"
    ]
    DAY_TO_SIMULATE_FOR_WEEK_2 = 10 # Any day between 8 and 14

    effective_test_mode = getattr(config, 'TEST_MODE', True)
    print("\n" + "="*70)
    print(f"TEST MODE: {'ON (Printing Emails to Console)' if effective_test_mode else 'OFF (Attempting Real SMTP Send)'}")
    if effective_test_mode:
        print("!!! IMPORTANT: To send actual emails, ensure TEST_MODE = False in your .env file")
    print(f"Test emails will be sent to: {TEST_RECIPIENT_EMAIL}")
    print(f"Generating reports for {len(REPS_TO_TEST)} specific sales reps.")
    print("="*70 + "\n")
    
    with app.app_context():
        logger.info("App context established for testing.")
        
        original_datetime_class = datetime.datetime
        
        # --- Mock datetime.datetime.now() to simulate Week 2 ---
        class MockDateTime(datetime.datetime):
            @classmethod
            def now(cls, tz=None):
                current_real_year = original_datetime_class.now().year
                current_real_month = original_datetime_class.now().month
                return original_datetime_class(current_real_year, current_real_month, DAY_TO_SIMULATE_FOR_WEEK_2, tzinfo=tz)
        
        datetime.datetime = MockDateTime
        logger.info(f"MOCKING datetime.now() to: {datetime.datetime.now().date()} to simulate Week 2.")

        # --- Loop through the specified reps and send emails ---
        for rep_name in REPS_TO_TEST:
            print(f"\n--- PROCESSING: {rep_name} ---")
            test_rep_id = None
            try:
                stmt = select(AccountPrediction.sales_rep).where(AccountPrediction.sales_rep_name == rep_name).limit(1)
                result = db.session.execute(stmt).scalar_one_or_none()
                if result:
                    test_rep_id = result
                    logger.info(f"Found Rep ID for {rep_name}: {test_rep_id}")
                else:
                    logger.warning(f"Could not find Rep ID for '{rep_name}' in the database. Skipping.")
                    print(f"⚠️  WARNING: Could not find Rep ID for '{rep_name}'. Skipping this rep.")
                    continue # Move to the next rep
            except Exception as e_fetch_rep:
                logger.error(f"DB error finding Rep ID for {rep_name}: {e_fetch_rep}", exc_info=True)
                print(f"❌ ERROR: Database error while looking for '{rep_name}'. Skipping.")
                continue

            try:
                success = send_weekly_digest_email_for_rep(
                    test_rep_id, 
                    rep_name, 
                    TEST_RECIPIENT_EMAIL  # Always send to Natasha
                )
                if success:
                    print(f"✅ SUCCESS: Week 2 email for {rep_name} was generated and sent/printed.")
                else:
                    print(f"❌ FAILED: Week 2 email generation failed for {rep_name}. Check logs for details.")
            except Exception as e_test_run:
                logger.error(f"Exception during email generation for {rep_name}: {e_test_run}", exc_info=True)
                print(f"❌ EXCEPTION on {rep_name}. See logs for details.")

            # Pause between sending emails to be safe
            if not effective_test_mode:
                print("Pausing for 3 seconds...")
                time.sleep(3)
            
        # --- Restore the original datetime class ---
        datetime.datetime = original_datetime_class
        logger.info("Restored original datetime.datetime class.")
        print("\n--- All specified reps have been processed. Test finished. ---")