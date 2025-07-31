from flask import Blueprint, redirect, url_for
import logging

# Set up logging
logger = logging.getLogger(__name__)

# Create blueprint
compatibility_bp = Blueprint('compatibility', __name__, url_prefix='/api/historical')

@compatibility_bp.route('/top-accounts-by-rep', methods=['GET'])
def top_accounts_redirect():
    """Compatibility route that redirects to the new endpoint"""
    logger.info("Redirecting from /api/historical/top-accounts-by-rep to /api/sales-manager/all-top-accounts")
    return redirect('/api/sales-manager/all-top-accounts')