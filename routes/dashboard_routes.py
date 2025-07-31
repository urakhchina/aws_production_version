from flask import Blueprint, render_template
import os
import logging
from models import db, AccountPrediction # Import model to check if rep exists

# Create blueprint
dashboard_bp = Blueprint('dashboard', __name__, url_prefix='/dashboard')

# Set up basic logging
logger = logging.getLogger(__name__)

@dashboard_bp.route('/', methods=['GET'])
def main_dashboard():
    """Serve the churn risk dashboard with added navigation to strategic dashboard"""
    try:
        return render_template('churn_dashboard.html')
    except Exception as e:
        logger.error(f"Error rendering churn dashboard: {str(e)}")
        return f"Error rendering dashboard: {str(e)}", 500

@dashboard_bp.route('/strategic', methods=['GET'])
def strategic_dashboard():
    """Serve the strategic accounts dashboard"""
    try:
        return render_template('strategic_dashboard.html')
    except Exception as e:
        logger.error(f"Error rendering strategic dashboard: {str(e)}")
        return f"Error rendering strategic dashboard: {str(e)}", 500
    

@dashboard_bp.route('/sales-manager', methods=['GET'])
def sales_manager_dashboard():
    """Serve the sales manager dashboard"""
    try:
        return render_template('sales_manager_dashboard.html')
    except Exception as e:
        logger.error(f"Error rendering sales manager dashboard: {str(e)}")
        return f"Error rendering sales manager dashboard: {str(e)}", 500
    

# NEW: Route for Strategic Dashboard WITH a specific rep ID in the path
@dashboard_bp.route('/strategic/<string:rep_id>', methods=['GET'])
def strategic_dashboard_rep(rep_id):
    """Serve the strategic accounts dashboard pre-filtered for a specific rep ID."""
    logger.info(f"Request for strategic dashboard for specific Rep ID: {rep_id}")
    try:
        # Optional: Validate if the rep_id actually exists in your system
        # This prevents loading the page for invalid IDs passed in the URL
        stmt = db.select(AccountPrediction.id).where(AccountPrediction.sales_rep == rep_id).limit(1)
        rep_exists = db.session.execute(stmt).scalar_one_or_none() is not None
        if not rep_exists:
            logger.warning(f"Attempted to load strategic dashboard for non-existent Rep ID: {rep_id}")
            abort(404, description=f"Sales Rep ID '{rep_id}' not found.") # Return 404 Not Found

        # Pass the validated rep_id to the template context
        return render_template('strategic_dashboard.html', initial_sales_rep_id=rep_id)
    except Exception as e:
        logger.error(f"Error rendering strategic dashboard for rep {rep_id}: {str(e)}")
        return f"Error rendering dashboard for rep {rep_id}: {str(e)}", 500
    

# --- NEW ROUTE for Account Detail Page ---
@dashboard_bp.route('/account/<string:canonical_code>', methods=['GET'])
def account_detail_page(canonical_code):
    """Serve the dedicated detail page for a specific account."""
    logger.info(f"Request for account detail page: {canonical_code}")
    if not canonical_code:
        abort(400, description="Missing account identifier.")

    try:
        # **Optional but recommended: Basic validation**
        # Check if an account with this canonical_code actually exists in the Prediction table
        stmt = db.select(AccountPrediction.id).where(AccountPrediction.canonical_code == canonical_code).limit(1)
        account_exists = db.session.execute(stmt).scalar_one_or_none() is not None

        if not account_exists:
             logger.warning(f"Account detail page requested for non-existent code: {canonical_code}")
             abort(404, description=f"Account '{canonical_code}' not found.") # Return 404 Not Found

        # Pass the canonical_code to the template.
        # The template's JS will fetch all other details via API.
        return render_template('account_detail.html', account_code=canonical_code) # Use 'account_code' for clarity in template

    except Exception as e:
        logger.error(f"Error rendering account detail page for {canonical_code}: {str(e)}")
        # Consider a proper error template
        return f"Error loading account detail page: {str(e)}", 500