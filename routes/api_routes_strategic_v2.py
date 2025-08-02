# routes/api_routes_strategic_v2.py

from flask import Blueprint, jsonify, request
from sqlalchemy import func, desc, asc, or_, and_, select, case # Added and_, case
from models import db, AccountPrediction, AccountHistoricalRevenue, Transaction # Added AccountHistoricalRevenue
import logging
import json
from datetime import datetime, timedelta, date
import pandas as pd # Add pandas for easier date handling
import numpy as np  # Add numpy for mean calculation
from sklearn.linear_model import LinearRegression
from collections import defaultdict
from collections import defaultdict
import math

# --- Import Config safely for Thresholds ---
# (This assumes config.py is accessible)
try:
    import config
    logger_cfg = logging.getLogger(__name__ + '.config') # Dedicated logger for config part
    logger_cfg.info("Importing config for API thresholds.")
    HEALTH_POOR_THRESHOLD = getattr(config, 'HEALTH_POOR_THRESHOLD', 40)
    PRIORITY_PACE_DECLINE_PCT_THRESHOLD = getattr(config, 'PRIORITY_PACE_DECLINE_PCT_THRESHOLD', -10)
    GROWTH_PACE_INCREASE_PCT_THRESHOLD = getattr(config, 'GROWTH_PACE_INCREASE_PCT_THRESHOLD', 10)
    GROWTH_HEALTH_THRESHOLD = getattr(config, 'GROWTH_HEALTH_THRESHOLD', 60)
    GROWTH_MISSING_PRODUCTS_THRESHOLD = getattr(config, 'GROWTH_MISSING_PRODUCTS_THRESHOLD', 3)
    # Add Priority thresholds used in summary counts (adjust values as needed)
    HIGH_PRIORITY_THRESHOLD = getattr(config, 'HIGH_PRIORITY_THRESHOLD', 75)
    MED_PRIORITY_THRESHOLD = getattr(config, 'MED_PRIORITY_THRESHOLD', 50)

except ImportError:
    logger_cfg = logging.getLogger(__name__ + '.config')
    logger_cfg.warning("Could not import config.py. Using default API thresholds.")
    HEALTH_POOR_THRESHOLD = 40
    PRIORITY_PACE_DECLINE_PCT_THRESHOLD = -10
    GROWTH_PACE_INCREASE_PCT_THRESHOLD = 10
    GROWTH_HEALTH_THRESHOLD = 60
    GROWTH_MISSING_PRODUCTS_THRESHOLD = 3
    HIGH_PRIORITY_THRESHOLD = 75
    MED_PRIORITY_THRESHOLD = 50
# --- End Config Import ---


def _get_purchase_timeline_data(canonical_code: str, year: int, session) -> list:
    """
    Gets transaction dates and associated products/revenue for timeline chart.
    """
    logger_timeline = logging.getLogger(__name__ + '._get_purchase_timeline_data')
    logger_timeline.debug(f"Getting purchase timeline data for {canonical_code}, year {year}")
    timeline_data = []
    try:
        stmt = select(
                    Transaction.posting_date,
                    Transaction.description,
                    Transaction.revenue
                )\
               .where(and_(
                   Transaction.canonical_code == canonical_code,
                   Transaction.year == year
               ))\
               .order_by(Transaction.posting_date.asc())
        results = session.execute(stmt).all()
        if not results: return []

        data_by_date = defaultdict(lambda: {"products": set(), "total_revenue": 0.0})
        for row in results:
            posting_dt = row.posting_date
            description = row.description
            revenue = row.revenue or 0.0
            if isinstance(posting_dt, datetime):
                 date_key = posting_dt.date()
                 if description: data_by_date[date_key]["products"].add(description)
                 data_by_date[date_key]["total_revenue"] += revenue

        for purchase_date, data in sorted(data_by_date.items()):
            chart_datetime = datetime.combine(purchase_date, datetime.min.time())
            daily_revenue = round(data["total_revenue"], 2)
            products_list = sorted(list(data["products"]))
            timeline_data.append({
                "x": chart_datetime.isoformat(),
                "y": 1,
                "products": products_list,
                "daily_revenue": daily_revenue
            })
        return timeline_data
    except Exception as e:
        logger_timeline.error(f"Error getting purchase timeline data for {canonical_code}, {year}: {e}", exc_info=True)
        return []



# --- Helper function to get PY Revenue (Copy from communication_engine or import) ---
# For simplicity, copied here. Ensure consistency if changed elsewhere.
def get_previous_year_revenue(account_canonical_codes, prev_year, session):
    """Fetches total_revenue for specific accounts for the specified previous year."""
    logger_helper = logging.getLogger(__name__ + '.get_py_revenue')
    if not account_canonical_codes or not prev_year: return {}
    logger_helper.debug(f"DB Query: Fetching PY revenue for {len(account_canonical_codes)} accounts, year {prev_year}")
    revenue_map = {}
    try:
        # Ensure codes are unique and not empty/None before querying
        valid_codes = list(filter(None, set(account_canonical_codes)))
        if not valid_codes: return {}

        stmt = select(
            AccountHistoricalRevenue.canonical_code,
            AccountHistoricalRevenue.total_revenue
        ).where(
            AccountHistoricalRevenue.canonical_code.in_(valid_codes),
            AccountHistoricalRevenue.year == prev_year
        )
        results = session.execute(stmt).all() # Use passed session
        for row in results:
            revenue_map[row.canonical_code] = row.total_revenue or 0.0
        logger_helper.debug(f"DB Query: Fetched PY revenue for {len(revenue_map)} accounts.")
    except Exception as e:
        logger_helper.error(f"DB Query Error in get_previous_year_revenue: {e}", exc_info=True)
    return revenue_map
# --- End Helper function ---


# --- Helper function to check Growth Opportunity ---
def is_growth_opportunity(acc, py_rev, today_date):
    """Checks if an account meets growth opportunity criteria."""
    logger_helper = logging.getLogger(__name__ + '.is_growth_opp')
    try:
        # Rule 1: Must have sufficient health
        if (acc.health_score or 0) < GROWTH_HEALTH_THRESHOLD:
            return False

        meets_criteria = False

        # Rule 2: Check Pace % Increase
        pace_vs_ly = acc.pace_vs_ly
        if pace_vs_ly is not None and py_rev is not None and py_rev > 0:
            pace_pct = (pace_vs_ly / py_rev) * 100.0
            if pace_pct >= GROWTH_PACE_INCREASE_PCT_THRESHOLD:
                meets_criteria = True
                # logger_helper.debug(f"{acc.canonical_code}: Meets growth via Pace ({pace_pct:.1f}%)")


        # Rule 3: Check Missing Products (if not already met)
        if not meets_criteria:
            missing_products = []
            try:
                # Use property if available and it returns a list
                if hasattr(acc, 'missing_top_products') and isinstance(acc.missing_top_products, list):
                     missing_products = acc.missing_top_products
                elif acc.missing_top_products_json: # Fallback to parsing JSON
                    loaded = json.loads(acc.missing_top_products_json)
                    if isinstance(loaded, list): missing_products = loaded
            except (json.JSONDecodeError, TypeError):
                 logger_helper.warning(f"Could not parse missing products for {acc.canonical_code} in growth check.")
                 pass # Ignore errors here
            if len(missing_products) >= GROWTH_MISSING_PRODUCTS_THRESHOLD:
                meets_criteria = True
                # logger_helper.debug(f"{acc.canonical_code}: Meets growth via Missing Products ({len(missing_products)})")

        # Rule 4: Check High Value Segment Due Soon (if not already met)
        if not meets_criteria and acc.rfm_segment in ["Champions", "Loyal Customers"]:
            due_date = acc.next_expected_purchase_date
            if due_date:
                due_date_only = due_date.date() if isinstance(due_date, datetime) else due_date
                if isinstance(due_date_only, date): # Check type just in case
                    next_14_days_end = today_date + timedelta(days=14)
                    if due_date_only <= next_14_days_end:
                        meets_criteria = True
                        # logger_helper.debug(f"{acc.canonical_code}: Meets growth via Segment/Due Date")

        return meets_criteria
    except Exception as e:
        logger_helper.error(f"Error in is_growth_opportunity for {getattr(acc, 'canonical_code', 'N/A')}: {e}", exc_info=True)
        return False
# --- End Helper function ---


# Create Blueprint
api_strategic_v2_bp = Blueprint('api_strategic_v2', __name__, url_prefix='/api/strategic') # Use _v2 in BP name
logger = logging.getLogger(__name__) # Define logger for the blueprint


@api_strategic_v2_bp.route('/accounts_v2', methods=['GET'])
def get_strategic_accounts_data_v2():
    """
    V2 Endpoint: Fetches detailed account prediction data sorted by priority score,
    calculates comprehensive summary stats, and supports filtering.
    """
    logger.info("Received request for strategic accounts data (V2 Endpoint)")

    # --- Get and Validate Filters (Unchanged) ---
    sales_rep_id = request.args.get('sales_rep')
    distributor = request.args.get('distributor')
    log_msg = (f"Filtering V2 - Rep ID: {sales_rep_id or 'All (Inc. Unassigned)'}, "
               f"Dist: {distributor or 'All'}")
    logger.info(log_msg)

    try:
        # --- Base Query and Filtering (Unchanged) ---
        stmt = select(AccountPrediction)
        conditions = []
        if distributor:
            conditions.append(AccountPrediction.distributor == distributor)
        if sales_rep_id == "__UNASSIGNED__":
            conditions.append(or_(AccountPrediction.sales_rep == None, AccountPrediction.sales_rep == ''))
        elif sales_rep_id:
            conditions.append(AccountPrediction.sales_rep == sales_rep_id)

        if conditions:
            stmt = stmt.where(*conditions)

        stmt = stmt.order_by(AccountPrediction.enhanced_priority_score.desc().nullslast())
        
        accounts = db.session.execute(stmt).scalars().all()
        logger.info(f"Found {len(accounts)} accounts matching filters for V2 endpoint.")

        # --- Calculate Comprehensive Summary Statistics (Unchanged) ---
        summary_stats = {
            'total_accounts': 0, 'total_yep': 0.0, 'avg_priority_score': None, 'avg_health_score': None,
            'count_priority1': 0, 'count_priority2': 0, 'count_due_this_week': 0,
            'count_overdue': 0, 'count_low_health': 0, 'count_low_pace': 0,
            'count_high_pace': 0, 'count_growth_opps': 0
        }

        if accounts:
            summary_stats['total_accounts'] = len(accounts)
            valid_priority_scores = []
            valid_health_scores = []
            account_codes = [acc.canonical_code for acc in accounts]
            today = datetime.now().date()
            current_year = today.year
            end_of_this_week = today + timedelta(days=6)

            py_revenue_map = get_previous_year_revenue(account_codes, current_year - 1, db.session)

            low_pace_count = 0 # Use a separate counter to avoid double counting from the debug log
            for acc in accounts:
                summary_stats['total_yep'] += acc.yep_revenue or 0.0
                if acc.enhanced_priority_score is not None: valid_priority_scores.append(acc.enhanced_priority_score)
                if acc.health_score is not None: valid_health_scores.append(acc.health_score)
                priority_score = acc.enhanced_priority_score or -1
                if priority_score >= HIGH_PRIORITY_THRESHOLD: summary_stats['count_priority1'] += 1
                if MED_PRIORITY_THRESHOLD <= priority_score < HIGH_PRIORITY_THRESHOLD: summary_stats['count_priority2'] += 1
                due_date = acc.next_expected_purchase_date
                if due_date:
                    due_date_only = due_date.date() if isinstance(due_date, datetime) else due_date
                    if isinstance(due_date_only, date) and today <= due_date_only <= end_of_this_week:
                         summary_stats['count_due_this_week'] += 1
                if (acc.days_overdue or 0) > 0: summary_stats['count_overdue'] += 1
                if (acc.health_score or 101) < HEALTH_POOR_THRESHOLD: summary_stats['count_low_health'] += 1
                py_rev = py_revenue_map.get(acc.canonical_code, 0.0)
                pace_val = acc.pace_vs_ly
                if pace_val is not None and py_rev is not None and py_rev > 0:
                    pace_pct = (pace_val / py_rev) * 100.0
                    if pace_pct <= PRIORITY_PACE_DECLINE_PCT_THRESHOLD: low_pace_count += 1
                    if pace_pct >= GROWTH_PACE_INCREASE_PCT_THRESHOLD: summary_stats['count_high_pace'] += 1
                if is_growth_opportunity(acc, py_rev, today):
                    summary_stats['count_growth_opps'] += 1
            
            summary_stats['count_low_pace'] = low_pace_count # Assign the final count
            if valid_priority_scores: summary_stats['avg_priority_score'] = round(sum(valid_priority_scores) / len(valid_priority_scores), 1)
            if valid_health_scores: summary_stats['avg_health_score'] = round(sum(valid_health_scores) / len(valid_health_scores), 1)
            summary_stats['total_yep'] = round(summary_stats['total_yep'], 2)

        # --- Format Response List (With NaN cleaning) ---
        output_list = []
        for acc in accounts:
            py_rev_for_acc = py_revenue_map.get(acc.canonical_code, 0.0)
            carried_list = acc.carried_top_products if hasattr(acc, 'carried_top_products') else []
            missing_list = acc.missing_top_products if hasattr(acc, 'missing_top_products') else []

            # Create the dictionary from the SQLAlchemy object
            acc_data = {
                "id": acc.id,
                "canonical_code": acc.canonical_code,
                "base_card_code": acc.base_card_code,
                "name": acc.name,
                "distributor": acc.distributor,
                "full_address": acc.full_address,
                "enhanced_priority_score": acc.enhanced_priority_score,
                "health_score": acc.health_score,
                "health_category": acc.health_category,
                "rfm_segment": acc.rfm_segment,
                "recency_score": acc.recency_score,
                "frequency_score": acc.frequency_score,
                "next_expected_purchase_date": acc.next_expected_purchase_date.isoformat() if acc.next_expected_purchase_date else None,
                "days_overdue": acc.days_overdue,
                "last_purchase_date": acc.last_purchase_date.isoformat() if acc.last_purchase_date else None,
                "last_purchase_amount": acc.last_purchase_amount,
                "days_since_last_purchase": acc.days_since_last_purchase,
                "account_total": acc.account_total,
                "cytd_revenue": acc.cytd_revenue,
                "yep_revenue": acc.yep_revenue,
                "pace_vs_ly": acc.pace_vs_ly,
                "py_total_revenue": py_rev_for_acc,
                "yoy_revenue_growth": acc.yoy_revenue_growth,
                "avg_order_amount_cytd": acc.avg_order_amount_cytd,
                "median_interval_days": acc.median_interval_days,
                "avg_interval_cytd": acc.avg_interval_cytd,
                "avg_interval_py": acc.avg_interval_py,
                "product_coverage_percentage": acc.product_coverage_percentage,
                "carried_top_products": carried_list if isinstance(carried_list, list) else [],
                "missing_top_products": missing_list if isinstance(missing_list, list) else [],
                "sales_rep": acc.sales_rep,
                "sales_rep_name": acc.sales_rep_name,
            }

            # --- THIS IS THE FIX ---
            # Create a new, clean dictionary, replacing any NaN values with None.
            cleaned_acc_data = {}
            for key, value in acc_data.items():
                # Check if the value is a float and if it is NaN
                if isinstance(value, float) and math.isnan(value):
                    cleaned_acc_data[key] = None # Replace NaN with None for valid JSON
                else:
                    cleaned_acc_data[key] = value
            # --- END FIX ---
            
            output_list.append(cleaned_acc_data)

        # --- Return Combined Data ---
        return jsonify({
            "accounts": output_list,
            "summary_stats": summary_stats
        })

    except Exception as e:
        logger.error(f"Error fetching V2 strategic accounts data: {str(e)}", exc_info=True)
        return jsonify({"error": "An internal server error occurred."}), 500
    

# --- NEW Account Detail API Endpoint ---
@api_strategic_v2_bp.route('/accounts/<string:canonical_code>/details', methods=['GET'])
def get_account_details_and_chart_data(canonical_code):
    """
    API: Get detailed info, chart data, and basic analysis (revenue trend).
    """
    logger.info(f"API request for account details/charts/analysis: {canonical_code}")
    # print("--- RUNNING UPDATED get_account_details_and_chart_data ---") # Optional: Keep if needed
    if not canonical_code:
        return jsonify({"error": "canonical_code parameter is required."}), 400

    try:
        # === 1. Fetch Current Prediction Data ===
        stmt_pred = select(AccountPrediction).where(AccountPrediction.canonical_code == canonical_code)
        account = db.session.execute(stmt_pred).scalar_one_or_none()
        if not account: return jsonify({"error": "Account prediction data not found."}), 404

        # === 2. Fetch Historical Aggregates ===
        current_year = datetime.now().year
        prev_year = current_year - 1
        stmt_hist = select(
            AccountHistoricalRevenue.year,
            AccountHistoricalRevenue.total_revenue,
            AccountHistoricalRevenue.transaction_count
        ).where(
            AccountHistoricalRevenue.canonical_code == canonical_code
        ).order_by(AccountHistoricalRevenue.year.asc())
        historical_results = db.session.execute(stmt_hist).all()
        historical_summary = [
            {"year": r.year, "revenue": r.total_revenue or 0.0, "transactions": r.transaction_count or 0}
            for r in historical_results
        ]
        py_summary = next((item for item in historical_summary if item["year"] == prev_year), None)
        py_total_revenue = py_summary['revenue'] if py_summary else 0.0

        # === 3. Fetch/Calculate Data SPECIFICALLY for Charts ===
        cy_timeline_data = _get_purchase_timeline_data(canonical_code, current_year, db.session)
        py_timeline_data = _get_purchase_timeline_data(canonical_code, prev_year, db.session)
        revenue_history_data = {
            "years": [h['year'] for h in historical_summary],
            "revenues": [h['revenue'] for h in historical_summary]
        }

        # === 4. Calculate Analysis ===
        revenue_trend = _calculate_linear_trend(
            revenue_history_data["years"],
            revenue_history_data["revenues"]
        )
        # Add calls to other analysis helpers here later if needed

        # === 5. Format API Response ===
        carried_list = account.carried_top_products if hasattr(account, 'carried_top_products') else []
        missing_list = account.missing_top_products if hasattr(account, 'missing_top_products') else []

        response_data = {
            "prediction": {
                # Include all relevant fields from AccountPrediction model
                "canonical_code": account.canonical_code,
                "base_card_code": account.base_card_code,
                "name": account.name,
                "sales_rep": account.sales_rep,
                "sales_rep_name": account.sales_rep_name,
                "distributor": account.distributor,
                "full_address": account.full_address,
                "health_score": account.health_score,
                "health_category": account.health_category,
                "rfm_segment": account.rfm_segment,
                "days_overdue": account.days_overdue,
                "last_purchase_date": account.last_purchase_date.isoformat() if account.last_purchase_date else None,
                "next_expected_purchase_date": account.next_expected_purchase_date.isoformat() if account.next_expected_purchase_date else None,
                "account_total": account.account_total,
                "pace_vs_ly": account.pace_vs_ly,
                "yep_revenue": account.yep_revenue,
                "cytd_revenue": account.cytd_revenue,
                "avg_interval_cytd": account.avg_interval_cytd,
                "avg_interval_py": account.avg_interval_py,
                "py_total_revenue": py_total_revenue, # Include calculated PY total
                "product_coverage_percentage": account.product_coverage_percentage,
                "missing_top_products": missing_list if isinstance(missing_list, list) else [],
                "carried_top_products": carried_list if isinstance(carried_list, list) else []
                # Add others like 'median_interval_days' if needed by frontend
            },
            "historical_summary": historical_summary,
            "chart_data": {
                "cy_purchase_timeline": cy_timeline_data,
                "py_purchase_timeline": py_timeline_data,
                "revenue_history": revenue_history_data
            },
            "analysis": { # <<< ADDED ANALYSIS SECTION
                "revenue_trend": revenue_trend
            }
        }
        return jsonify(response_data)

    except Exception as e:
        logger.error(f"Error fetching details API for account {canonical_code}: {e}", exc_info=True)
        return jsonify({"error": "An internal server error occurred."}), 500
    


def _calculate_linear_trend(years: list, values: list) -> dict:
    """
    Calculates slope, intercept, R-squared for simple linear regression.
    Handles cases with insufficient data or non-numeric values.
    """
    logger_trend = logging.getLogger(__name__ + '._calculate_linear_trend')
    trend_results = {"slope": None, "intercept": None, "r_squared": None, "forecast_next": None}
    try:
        # Ensure we have lists of the same length
        if not isinstance(years, list) or not isinstance(values, list) or len(years) != len(values):
             logger_trend.warning("Input years/values are not lists or have different lengths.")
             return trend_results

        # Prepare data: remove entries where value is null/NaN and keep corresponding year
        valid_data = [(year, value) for year, value in zip(years, values) if pd.notna(value) and isinstance(value, (int, float))]

        # Need at least 2 valid data points for a trend
        if len(valid_data) < 2:
            logger_trend.debug(f"Insufficient valid data points ({len(valid_data)}) for trend calculation.")
            return trend_results

        # Separate into numpy arrays for sklearn
        valid_years_np = np.array([item[0] for item in valid_data]).reshape(-1, 1)
        valid_values_np = np.array([item[1] for item in valid_data])

        # Perform Linear Regression
        model = LinearRegression()
        model.fit(valid_years_np, valid_values_np)

        # Extract results
        slope = model.coef_[0]
        intercept = model.intercept_
        r_squared = model.score(valid_years_np, valid_values_np)

        trend_results["slope"] = round(float(slope), 2)
        trend_results["intercept"] = round(float(intercept), 2)
        trend_results["r_squared"] = round(float(r_squared), 2)

        # Forecast for the next year
        if years: # Check if years list was not empty initially
             next_year = max(years) + 1
             forecast = model.predict(np.array([[next_year]]))
             trend_results["forecast_next"] = round(float(forecast[0]), 2)

        logger_trend.debug(f"Trend calculated: Slope={trend_results['slope']}, R²={trend_results['r_squared']}, Forecast={trend_results['forecast_next']}")

    except Exception as e:
        logger_trend.error(f"Error calculating linear trend: {e}", exc_info=True) # Log full traceback
        # Return dict with None values on any error
    return trend_results



# --- Remember to register this blueprint in your main app factory ---
# from routes.api_routes_strategic_v2 import api_strategic_v2_bp
# app.register_blueprint(api_strategic_v2_bp)