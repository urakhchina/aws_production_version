# api_routes.py

from flask import Blueprint, jsonify, request # Added request for potential future use
import logging
from sqlalchemy import select, func, distinct, or_ # Added select, func, distinct, or_
from models import db, AccountPrediction
import random # Keep for fallback in get_all_top_accounts if needed

# Create blueprint
api_bp = Blueprint('api', __name__, url_prefix='/api')

# Set up basic logging
logger = logging.getLogger(__name__)


@api_bp.route('/rep/<rep_name>/strategic-accounts', methods=['GET'])
def get_rep_strategic_accounts(rep_name):
    """Returns strategic account information for a specific sales rep using SQLAlchemy 2.x"""
    try:
        # --- Build queries using SQLAlchemy 2.x ---
        base_stmt = select(AccountPrediction).where(AccountPrediction.sales_rep_name == rep_name)

        stmt_champions = base_stmt.where(AccountPrediction.rfm_segment == 'Champions')
        champions = db.session.execute(stmt_champions).scalars().all()

        stmt_at_risk = base_stmt.where(AccountPrediction.rfm_segment.in_(["At Risk", "Can't Lose"]))
        at_risk = db.session.execute(stmt_at_risk).scalars().all()

        stmt_critical_health = base_stmt.where(AccountPrediction.health_category == 'Critical')
        critical_health = db.session.execute(stmt_critical_health).scalars().all()
        # --- End Queries ---

        # --- Format the account data (No change needed in formatting logic) ---
        result = {
            'champions': [{
                'canonical_code': acct.canonical_code, # Use canonical code
                'name': acct.name,
                'address': acct.full_address,
                'account_total': acct.account_total,
                'last_purchase_date': acct.last_purchase_date.strftime('%Y-%m-%d') if acct.last_purchase_date else None,
                'purchase_frequency': acct.purchase_frequency,
                'rfm_segment': acct.rfm_segment if hasattr(acct, 'rfm_segment') else None,
                'health_category': acct.health_category if hasattr(acct, 'health_category') else None,
                'health_score': acct.health_score if hasattr(acct, 'health_score') else None,
                'days_overdue': acct.days_overdue,
                'recommendations': [
                    "Schedule a quarterly business review",
                    "Introduce new premium product lines",
                    "Consider case study or testimonial opportunity"
                ]
            } for acct in champions],

            'at_risk': [{
                'canonical_code': acct.canonical_code, # Use canonical code
                'name': acct.name,
                'address': acct.full_address,
                'account_total': acct.account_total,
                'last_purchase_date': acct.last_purchase_date.strftime('%Y-%m-%d') if acct.last_purchase_date else None,
                'days_overdue': acct.days_overdue,
                'rfm_segment': acct.rfm_segment if hasattr(acct, 'rfm_segment') else None,
                'health_category': acct.health_category if hasattr(acct, 'health_category') else None,
                'health_score': acct.health_score if hasattr(acct, 'health_score') else None,
                'purchase_frequency': acct.purchase_frequency,
                'recommendations': [
                    "Personal call from management",
                    "Customer satisfaction survey",
                    "Analyze recent order patterns for changes"
                ] if acct.rfm_segment != "Can't Lose" else [
                    "Executive outreach",
                    "Schedule account review meeting",
                    "Prepare personalized retention offer"
                ]
            } for acct in at_risk],

            'critical_health': [{
                'canonical_code': acct.canonical_code, # Use canonical code
                'name': acct.name,
                'address': acct.full_address,
                'account_total': acct.account_total,
                'health_score': acct.health_score if hasattr(acct, 'health_score') else None,
                'health_category': acct.health_category if hasattr(acct, 'health_category') else None,
                'days_overdue': acct.days_overdue,
                'rfm_segment': acct.rfm_segment if hasattr(acct, 'rfm_segment') else None,
                'last_purchase_date': acct.last_purchase_date.strftime('%Y-%m-%d') if acct.last_purchase_date else None,
                'purchase_frequency': acct.purchase_frequency,
                'recommendations': [
                    "Immediate intervention required",
                    "Account health assessment",
                    "Develop recovery plan"
                ]
            } for acct in critical_health]
        }
        # --- End Formatting ---

        return jsonify(result)

    except Exception as e:
        logger.error(f"Error in get_rep_strategic_accounts: {str(e)}", exc_info=True) # Added exc_info
        return jsonify({'error': f"An internal server error occurred: {str(e)}"}), 500


@api_bp.route('/sales-reps', methods=['GET'])
def get_sales_reps():
    """Returns a list of all sales reps using SQLAlchemy 2.x"""
    try:
        # --- Use select() and scalars() for distinct column ---
        stmt = select(AccountPrediction.sales_rep_name).distinct()
        # .scalars().all() directly gives the list of names
        rep_list = db.session.execute(stmt).scalars().all()
        # Filter out None or empty strings if necessary (though distinct might handle None)
        rep_list = [rep for rep in rep_list if rep and rep.strip()]
        # --- End Query ---
        return jsonify(sorted(rep_list)) # Sort for consistency
    except Exception as e:
        logger.error(f"Error fetching sales reps: {str(e)}", exc_info=True)
        return jsonify({'error': f"An internal server error occurred: {str(e)}"}), 500


@api_bp.route('/sales-manager/overview', methods=['GET'])
def get_sales_manager_overview():
    """
    Returns overview data for the sales manager dashboard using SQLAlchemy 2.x.

    This endpoint provides:
    1. Summary metrics for all reps
    2. List of all sales reps with their key performance metrics
    """
    try:
        # --- Get all sales reps using SQLAlchemy 2.x ---
        rep_stmt = select(AccountPrediction.sales_rep_name).distinct()
        sales_reps = db.session.execute(rep_stmt).scalars().all()
        rep_names = sorted([rep for rep in sales_reps if rep and rep.strip()]) # Sort and filter
        # --- End Get Reps ---

        # Initialize response data
        overview_data = {
            'summary': {
                'total_accounts': 0,
                'total_revenue': 0.0,
            },
            'rep_performance': {}
        }

        # --- Get all account data once (more efficient) ---
        all_accounts_stmt = select(AccountPrediction)
        all_accounts = db.session.execute(all_accounts_stmt).scalars().all()
        # --- End Get All Accounts ---

        # --- Process data in Python ---
        temp_rep_data = {}
        for account in all_accounts:
            rep_name = account.sales_rep_name.strip() if account.sales_rep_name else "Unassigned"
            # Ensure rep_name exists in our distinct list if we want to exclude calculation for non-listed reps
            # if rep_name not in rep_names and rep_name != "Unassigned": continue # Optional: skip reps not in distinct list

            if rep_name not in temp_rep_data:
                temp_rep_data[rep_name] = {
                    'account_count': 0,
                    'total_revenue': 0.0,
                }

            # Increment counts and sums
            temp_rep_data[rep_name]['account_count'] += 1
            current_total = account.account_total or 0.0
            temp_rep_data[rep_name]['total_revenue'] += current_total

        # Populate final structure and summary
        for rep_name, data in temp_rep_data.items():
            overview_data['rep_performance'][rep_name] = data
            # Add to summary totals
            overview_data['summary']['total_accounts'] += data['account_count']
            overview_data['summary']['total_revenue'] += data['total_revenue']
        # --- End Processing ---

        return jsonify(overview_data)

    except Exception as e:
        logger.error(f"Error in get_sales_manager_overview: {str(e)}", exc_info=True) # Added exc_info
        return jsonify({'error': f"An internal server error occurred: {str(e)}"}), 500


@api_bp.route('/sales-manager/rep-top-accounts/<rep_name>', methods=['GET'])
def get_rep_top_accounts(rep_name):
    """
    Returns the top 20 accounts by revenue for a specific sales rep using SQLAlchemy 2.x,
    and attempts to fetch historical metrics.
    """
    try:
        # --- Get top 20 accounts for this rep using SQLAlchemy 2.x ---
        stmt = (select(AccountPrediction)
                .where(AccountPrediction.sales_rep_name == rep_name)
                .order_by(AccountPrediction.account_total.desc().nullslast())
                .limit(20))
        top_accounts = db.session.execute(stmt).scalars().all()
        # --- End Query ---

        if not top_accounts:
            # Use canonical code if linking becomes important
            return jsonify({"message": f"No accounts found for sales rep: {rep_name}"}), 404

        # --- Import snapshot function (Keep as is, assumes snapshot_manager is updated/compatible) ---
        try:
             from snapshot_manager import get_historical_metrics
        except ImportError:
             logger.warning("snapshot_manager or get_historical_metrics not found. Trend data will use fallbacks.")
             def get_historical_metrics(account_id, period): return None # Define dummy function
        # --- End Import ---


        # --- Format account data (No changes needed in formatting logic itself) ---
        result = []
        for acct in top_accounts:
            # Get real snapshot data if available (using canonical_code now potentially)
            # IMPORTANT: get_historical_metrics needs to accept canonical_code or map id internally
            wow_metrics = get_historical_metrics(acct.canonical_code, 'weekly') # Pass canonical_code
            mom_metrics = get_historical_metrics(acct.canonical_code, 'monthly')# Pass canonical_code
            qoq_metrics = get_historical_metrics(acct.canonical_code, 'quarterly')# Pass canonical_code

            # Fallback logic remains the same conceptually
            wow_change = wow_metrics['account_total_change'] if wow_metrics else 0.0
            mom_change = mom_metrics['account_total_change'] if mom_metrics else 0.0
            qoq_change = qoq_metrics['account_total_change'] if qoq_metrics else 0.0

            # Example Fallback/Proxy calculation if no snapshot data (kept for reference)
            if not wow_metrics:
                from datetime import datetime, timedelta
                today = datetime.now().date()
                days_since_purchase = (today - acct.last_purchase_date.date()).days if acct.last_purchase_date else 30
                expected_interval = acct.median_interval_days if acct.median_interval_days and acct.median_interval_days > 0 else 30
                interval_factor = (expected_interval - days_since_purchase) / expected_interval if expected_interval else 0
                wow_change = interval_factor * 10 # Simplified proxy
            if not mom_metrics:
                if hasattr(acct, 'health_score') and acct.health_score is not None: mom_change = (acct.health_score - 50) / 5
                else: mom_change = wow_change / 2
            if not qoq_metrics:
                if hasattr(acct, 'rfm_segment') and acct.rfm_segment:
                    segment_trends = {'Champions': 15, 'Loyal Customers': 8, 'At Risk': -10, "Can't Lose": -20, 'New Customers': 5, 'Potential Loyalists': 3}
                    base_trend = segment_trends.get(acct.rfm_segment, 0)
                    freq_adj = min(10, max(-10, (acct.purchase_frequency - 5) * 2)) if acct.purchase_frequency else 0
                    qoq_change = base_trend + freq_adj
                else: qoq_change = mom_change * 1.5


            # Ensure values are within reasonable ranges
            wow_change = round(max(-100, min(100, wow_change)), 1) # Wider range perhaps?
            mom_change = round(max(-100, min(100, mom_change)), 1)
            qoq_change = round(max(-100, min(100, qoq_change)), 1)


            result.append({
                'id': acct.id, # Keep internal ID if useful for frontend
                'canonical_code': acct.canonical_code, # Use canonical code
                'name': acct.name,
                'address': acct.full_address,
                'account_total': acct.account_total,
                'last_purchase_date': acct.last_purchase_date.strftime('%Y-%m-%d') if acct.last_purchase_date else None,
                'purchase_frequency': acct.purchase_frequency,
                'health_score': acct.health_score if hasattr(acct, 'health_score') else None,
                'days_overdue': acct.days_overdue,
                'rfm_segment': acct.rfm_segment if hasattr(acct, 'rfm_segment') else None,
                'changes': {
                    'week_over_week': wow_change,
                    'month_over_month': mom_change,
                    'quarter_over_quarter': qoq_change
                }
            })
        # --- End Formatting ---

        return jsonify(result)

    except Exception as e:
        logger.error(f"Error in get_rep_top_accounts: {str(e)}", exc_info=True) # Added exc_info
        return jsonify({'error': f"An internal server error occurred: {str(e)}"}), 500


@api_bp.route('/sales-manager/all-top-accounts', methods=['GET'])
def get_all_top_accounts():
    """
    Returns the top 20 accounts by revenue for each sales rep in a consolidated view
    using SQLAlchemy 2.x.
    """
    try:
        # --- Get all sales reps using SQLAlchemy 2.x ---
        rep_stmt = select(AccountPrediction.sales_rep_name).distinct()
        sales_reps = db.session.execute(rep_stmt).scalars().all()
        rep_names = sorted([rep for rep in sales_reps if rep and rep.strip()]) # Sort and filter
        # --- End Get Reps ---

        # Initialize response data
        result = {}

        # Get top accounts for each rep
        for rep_name in rep_names:
            # --- Query using SQLAlchemy 2.x ---
            stmt = (select(AccountPrediction)
                   .where(AccountPrediction.sales_rep_name == rep_name)
                   .order_by(AccountPrediction.account_total.desc().nullslast())
                   .limit(20))
            accounts = db.session.execute(stmt).scalars().all()
            # --- End Query ---

            if not accounts:
                continue

            # --- Format account data (No change needed in formatting logic itself) ---
            rep_data = []
            for acct in accounts:
                # Calculate trends (simplified version or call get_historical_metrics if performance allows)
                # Using random as placeholder like original:
                wow_change = random.uniform(-15, 15)
                mom_change = random.uniform(-20, 20)
                qoq_change = random.uniform(-30, 30)

                rep_data.append({
                    'id': acct.id,
                    'canonical_code': acct.canonical_code, # Use canonical code
                    'name': acct.name,
                    'address': acct.full_address,
                    'account_total': acct.account_total,
                    'last_purchase_date': acct.last_purchase_date.strftime('%Y-%m-%d') if acct.last_purchase_date else None,
                    'purchase_frequency': acct.purchase_frequency,
                    'health_score': acct.health_score if hasattr(acct, 'health_score') else None,
                    'days_overdue': acct.days_overdue,
                    'rfm_segment': acct.rfm_segment if hasattr(acct, 'rfm_segment') else None,
                    'changes': {
                        'week_over_week': round(wow_change, 1),
                        'month_over_month': round(mom_change, 1),
                        'quarter_over_quarter': round(qoq_change, 1)
                    }
                })
            # --- End Formatting ---

            # Add rep's data to result
            result[rep_name] = rep_data

        return jsonify(result)

    except Exception as e:
        logger.error(f"Error in get_all_top_accounts: {str(e)}", exc_info=True) # Added exc_info
        return jsonify({'error': f"An internal server error occurred: {str(e)}"}), 500


@api_bp.route('/debug/db-check')
def check_db():
    """ Checks database connectivity using SQLAlchemy 2.x """
    try:
        # Import needed modules inside the function
        from sqlalchemy import func
        # No need to import AccountHistoricalRevenue if only using it here
        from models import AccountHistoricalRevenue

        # --- Use select() and scalar() for count ---
        stmt = select(func.count(AccountHistoricalRevenue.id))
        count = db.session.scalar(stmt)
        # --- End Query ---

        return jsonify({"db_connected": True, "record_count": count})
    except Exception as e:
        logger.error(f"Database check failed: {str(e)}", exc_info=True) # Added exc_info
        return jsonify({"db_connected": False, "error": str(e)})