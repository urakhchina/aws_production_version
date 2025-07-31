# routes/api_routes_historical.py

from logging import config
from flask import Blueprint, jsonify, request
import pandas as pd
import os
import glob
import json
import logging
from sqlalchemy import select, func, desc, asc, distinct, and_, or_, outerjoin # Ensure all needed are imported
from models import db, AccountHistoricalRevenue, AccountPrediction, AccountSnapshot # Added AccountSnapshot

# Set up logging
logger = logging.getLogger(__name__)

# Create blueprint
api_historical_bp = Blueprint('api_historical', __name__, url_prefix='/api/sales-manager')


def get_base_code_from_canonical(canonical_code):
    """Extracts base code, handling potential None input."""
    if not canonical_code or not isinstance(canonical_code, str):
        return None
    # Simple split assuming format BASE_SUFFIX
    parts = canonical_code.split('_', 1)
    return parts[0] if parts else None


@api_historical_bp.route('/accounts/<path:canonical_code>/history', methods=['GET']) # Renamed card_code to canonical_code
def get_account_history(canonical_code): # Renamed card_code to canonical_code
    """
    Get YEARLY history (revenue, transactions, AND categorized products)
    for a specific account using its canonical_code (v2.x).
    """
    if not canonical_code:
        return jsonify({"error": "canonical_code parameter is required."}), 400

    logger.info(f"Fetching yearly history and categorized products for account canonical_code='{canonical_code}' from DB (v2.x)")

    response_data = {
        "canonical_code": canonical_code, # Use canonical_code in response
        "yearly_history": {"years": [], "revenue": [], "transactions": []},
        "products_by_year": {}
    }
    found_history_or_account = False

    try:
        # --- Query using SQLAlchemy 2.x ---
        stmt = select(
            AccountHistoricalRevenue.year,
            AccountHistoricalRevenue.total_revenue,
            AccountHistoricalRevenue.transaction_count,
            AccountHistoricalRevenue.yearly_products_json # Fetch the JSON string
        ).where(
            # Use canonical_code for filtering
            AccountHistoricalRevenue.canonical_code == canonical_code
        ).order_by(
            AccountHistoricalRevenue.year.asc()
        )
        # Execute and get results as Row objects
        yearly_data_db_rows = db.session.execute(stmt).all()
        # --- End Query ---

        if yearly_data_db_rows:
            found_history_or_account = True
            # Process Row objects (access by attribute name or index)
            response_data["yearly_history"]["years"] = [row.year for row in yearly_data_db_rows]
            response_data["yearly_history"]["revenue"] = [float(row.total_revenue) if row.total_revenue else 0.0 for row in yearly_data_db_rows]
            response_data["yearly_history"]["transactions"] = [int(row.transaction_count) if row.transaction_count else 0 for row in yearly_data_db_rows]

            # --- Categorize Products By Year (No change needed in Python logic) ---
            products_by_year_categorized = {}
            seen_products_so_far = set()

            for row in yearly_data_db_rows: # Iterate through Row objects
                year = row.year
                year_str = str(year)
                current_year_products_set = set()
                products_json_string = row.yearly_products_json # Access JSON string

                # Robust JSON Parsing Logic (remains the same)
                if products_json_string:
                    try:
                        if isinstance(products_json_string, str):
                            loaded_list = json.loads(products_json_string)
                            if isinstance(loaded_list, list):
                                current_year_products_set = set(p for p in loaded_list if isinstance(p, str) and p.strip())
                            else: logger.warning(f"Parsed products JSON for {canonical_code} year {year}, but got type {type(loaded_list)} instead of list.")
                        else: logger.warning(f"products_json for {canonical_code} year {year} is not a string (type: {type(products_json_string)}). Skipping parse.")
                    except json.JSONDecodeError: logger.warning(f"Could not decode products_json for {canonical_code} year {year}. Invalid JSON: '{str(products_json_string)[:100]}...'")
                    except Exception as e: logger.error(f"Unexpected error parsing products_json for {canonical_code} year {year}: {e}", exc_info=False)

                # Calculate categories
                new_this_year = list(current_year_products_set - seen_products_so_far)
                reordered_this_year = list(current_year_products_set.intersection(seen_products_so_far))

                # Store categorized data
                products_by_year_categorized[year_str] = {
                    "all": sorted(list(current_year_products_set)),
                    "new": sorted(new_this_year),
                    "reordered": sorted(reordered_this_year)
                }
                seen_products_so_far.update(current_year_products_set)
            # --- End Categorization ---

            response_data["products_by_year"] = products_by_year_categorized
            logger.info(f"Processed {len(yearly_data_db_rows)} yearly records (incl. categorized products) from DB for {canonical_code}.")

        else:
            logger.warning(f"No yearly records found in DB for account {canonical_code}")
            # --- Check if account exists in Prediction table using canonical_code (v2.x) ---
            exists_stmt = select(AccountPrediction.id).where(AccountPrediction.canonical_code == canonical_code).limit(1)
            account_exists = db.session.execute(exists_stmt).scalar_one_or_none() is not None
            # --- End Check ---
            if account_exists:
                found_history_or_account = True
                logger.info(f"Account {canonical_code} exists but has no historical data.")
            else:
                 logger.error(f"Account {canonical_code} not found anywhere.")
                 return jsonify({"error": f"Account with canonical_code {canonical_code} not found."}), 404

        # Return 200 OK with potentially empty history/products if account exists
        return jsonify(response_data)

    except Exception as db_err:
        logger.error(f"Database error fetching history for {canonical_code}: {str(db_err)}", exc_info=True)
        return jsonify({"error": "An internal server error occurred while fetching account history."}), 500


@api_historical_bp.route('/top_accounts_by_rep', methods=['GET'])
def get_top_accounts_by_rep():
    """
    Get the top accounts by YEARLY revenue for each sales rep using SQLAlchemy 2.x
    and canonical_code for joining.

    Query parameters:
    - year: int - Filter by year (defaults to most recent year)
    - distributor: str - Filter by distributor
    - sales_rep: str - Filter by specific sales rep (ID or __UNASSIGNED__)
    - limit: int - Number of accounts to return per rep (default 20)
    """
    try:
        # Parse query parameters
        year = request.args.get('year', type=int)
        distributor = request.args.get('distributor') # Allow empty string or None
        sales_rep_id = request.args.get('sales_rep')     # Allow empty string, None, or __UNASSIGNED__
        limit = request.args.get('limit', 20, type=int)

        # Determine the year to query (SQLAlchemy 2.x)
        if not year:
            # Use select() and scalar() for max year
            max_year_stmt = select(func.max(AccountHistoricalRevenue.year))
            max_year_result = db.session.scalar(max_year_stmt)
            # --- End Query ---
            if not max_year_result:
                logger.warning("No historical data found to determine max year.")
                return jsonify({"error": "No historical data available"}), 404
            year = int(max_year_result)
            logger.info(f"No year provided, defaulting to most recent year: {year}")

        logger.info(f"Fetching top accounts for year={year}, distributor='{distributor}', sales_rep='{sales_rep_id}', limit={limit} (v2.x)")

        # --- Base Query using SQLAlchemy 2.x select() and join() ---
        stmt = select(
            AccountHistoricalRevenue.canonical_code, # Use canonical_code
            AccountHistoricalRevenue.name.label('historical_name'),
            AccountHistoricalRevenue.sales_rep,
            AccountPrediction.sales_rep_name,
            AccountHistoricalRevenue.total_revenue.label('yearly_revenue'),
            AccountHistoricalRevenue.transaction_count.label('yearly_transaction_count'),
            AccountPrediction.distributor,
            AccountPrediction.health_score,
            AccountPrediction.health_category,
            AccountPrediction.yoy_revenue_growth,
            AccountPrediction.name.label('prediction_name')
        ).select_from(AccountHistoricalRevenue).join(
            AccountPrediction,
            # Join on canonical_code
            AccountHistoricalRevenue.canonical_code == AccountPrediction.canonical_code
        ).where(
            AccountHistoricalRevenue.year == year
        )
        # --- End Base Query ---

        # --- Apply Optional Filters ---
        conditions = [] # Collect conditions
        if distributor:
            conditions.append(AccountPrediction.distributor == distributor)

        # --- Apply Sales Rep Filter LOGIC (on Historical table) ---
        if sales_rep_id == "__UNASSIGNED__":
            logger.info("Applying filter for Unassigned Sales Reps (NULL or Empty)...")
            conditions.append(or_(
                AccountHistoricalRevenue.sales_rep == None,
                AccountHistoricalRevenue.sales_rep == ''
            ))
        elif sales_rep_id: # If it has a value AND is not "__UNASSIGNED__"
            logger.info(f"Applying filter for specific Sales Rep ID: {sales_rep_id}")
            conditions.append(AccountHistoricalRevenue.sales_rep == sales_rep_id)
        else: # sales_rep_id is "" or None (All Reps including unassigned)
            logger.info("No specific Sales Rep filter applied (showing all).")
            pass # No *additional* rep filter needed

        if conditions:
             stmt = stmt.where(*conditions) # Apply collected conditions
        # --- End Optional Filters ---

        # Order for ranking within reps and overall sorting
        stmt = stmt.order_by(
            AccountHistoricalRevenue.sales_rep.asc().nullsfirst(), # Keep unassigned together
            desc('yearly_revenue').nullslast() # NULL revenue last
        )

        # Execute the query to get all relevant accounts as Row objects
        all_results_rows = db.session.execute(stmt).all()
        logger.info(f"Query returned {len(all_results_rows)} total rows before applying limit per rep.")
        partner_codes_set = getattr(config, 'CURRENT_YEAR_PARTNER_CODES', set()) # Get set from config


        # --- Process results (No change needed in Python logic) ---
        final_accounts_by_rep = {}
        if not sales_rep_id: # Group by rep and apply limit
            for row in all_results_rows: # Iterate Row objects
                rep_key = row.sales_rep if row.sales_rep else "__UNASSIGNED__"
                if rep_key not in final_accounts_by_rep:
                    final_accounts_by_rep[rep_key] = []

                if len(final_accounts_by_rep[rep_key]) < limit:
                     account_data = {
                        # Access data by attribute name from Row object
                        'canonical_code': row.canonical_code,
                        'name': row.prediction_name or row.historical_name,
                        'sales_rep': row.sales_rep,
                        'sales_rep_name': row.sales_rep_name,
                        'yearly_revenue': float(row.yearly_revenue) if row.yearly_revenue else 0.0,
                        'transaction_count': int(row.yearly_transaction_count) if row.yearly_transaction_count else 0,
                        'distributor': row.distributor,
                        'health_score': float(row.health_score) if row.health_score else None,
                        'health_category': row.health_category,
                        'yoy_growth': float(row.yoy_revenue_growth) if row.yoy_revenue_growth else None,
                        'is_partner': bool(
                            get_base_code_from_canonical(row.canonical_code) and \
                            get_base_code_from_canonical(row.canonical_code) in partner_codes_set
                        )
                     }
                     final_accounts_by_rep[rep_key].append(account_data)

            # Format response for multiple reps
            response = {
                'year': year,
                'distributor_filter': distributor or 'All',
                'sales_rep_filter': 'All',
                'limit_per_rep': limit,
                'total_reps_found': len(final_accounts_by_rep),
                'reps': [
                    {
                        'sales_rep': rep,
                        'sales_rep_name': (accounts[0]['sales_rep_name'] if accounts and accounts[0]['sales_rep'] is not None else ('Unassigned Accounts' if rep == '__UNASSIGNED__' else 'Unknown')),
                        'accounts': accounts
                    }
                    for rep, accounts in final_accounts_by_rep.items()
                ]
            }

        else: # Specific sales rep requested, just take top N results overall
            single_rep_accounts = []
            for row in all_results_rows[:limit]: # Already sorted by revenue descending
                account_data = {
                    'canonical_code': row.canonical_code,
                    'name': row.prediction_name or row.historical_name,
                    'sales_rep': row.sales_rep,
                    'sales_rep_name': row.sales_rep_name,
                    'yearly_revenue': float(row.yearly_revenue) if row.yearly_revenue else 0.0,
                    'transaction_count': int(row.yearly_transaction_count) if row.yearly_transaction_count else 0,
                    'distributor': row.distributor,
                    'health_score': float(row.health_score) if row.health_score else None,
                    'health_category': row.health_category,
                    'yoy_growth': float(row.yoy_revenue_growth) if row.yoy_revenue_growth else None,
                    'is_partner': bool(
                        get_base_code_from_canonical(row.canonical_code) and \
                        get_base_code_from_canonical(row.canonical_code) in partner_codes_set
                    )
                 }
                single_rep_accounts.append(account_data)

            # Format response for a single rep/group
            rep_name_display = 'Unassigned Accounts' if sales_rep_id == '__UNASSIGNED__' else (single_rep_accounts[0]['sales_rep_name'] if single_rep_accounts else 'Unknown')
            response = {
                'year': year,
                'distributor_filter': distributor or 'All',
                'sales_rep_filter': sales_rep_id,
                'sales_rep_name': rep_name_display,
                'limit': limit,
                'total_accounts_returned': len(single_rep_accounts),
                'accounts': single_rep_accounts
            }

        return jsonify(response)

    except Exception as e:
        logger.error(f"Error retrieving top accounts by rep: {str(e)}", exc_info=True)
        return jsonify({"error": "An internal error occurred while retrieving top accounts."}), 500


@api_historical_bp.route('/sales_rep_performance', methods=['GET'])
def get_sales_rep_performance():
    """
    Get performance metrics for sales reps based on YEARLY historical data
    using SQLAlchemy 2.x and canonical_code for joining.

    Query parameters:
    - year: int - Filter by year (defaults to most recent year)
    - distributor: str - Filter by distributor
    """
    try:
        # Parse query parameters
        year = request.args.get('year', type=int)
        distributor = request.args.get('distributor')

        # Determine the year to query (SQLAlchemy 2.x)
        if not year:
            max_year_stmt = select(func.max(AccountHistoricalRevenue.year))
            max_year_result = db.session.scalar(max_year_stmt)
            if not max_year_result:
                return jsonify({"error": "No historical data available"}), 404
            year = int(max_year_result)
        prev_year = year - 1
        logger.info(f"Fetching sales rep performance for year={year}, prev_year={prev_year}, distributor='{distributor}' (v2.x)")

        # --- Query for Current Year Performance (SQLAlchemy 2.x) ---
        current_stmt = select(
            AccountHistoricalRevenue.sales_rep,
            AccountPrediction.sales_rep_name,
            func.sum(AccountHistoricalRevenue.total_revenue).label('total_revenue'),
            # Count distinct canonical codes for accurate account count
            func.count(func.distinct(AccountHistoricalRevenue.canonical_code)).label('account_count'),
            func.avg(AccountPrediction.health_score).label('avg_health_score')
        ).select_from(AccountHistoricalRevenue).join(
            AccountPrediction,
            # Join on canonical_code
            AccountHistoricalRevenue.canonical_code == AccountPrediction.canonical_code
        ).where(
            AccountHistoricalRevenue.year == year
        )
        if distributor:
            current_stmt = current_stmt.where(AccountPrediction.distributor == distributor)

        current_stmt = current_stmt.group_by(
            AccountHistoricalRevenue.sales_rep,
            AccountPrediction.sales_rep_name
        )
        current_results = db.session.execute(current_stmt).all() # Get Row objects
        current_perf_dict = {}
        for res in current_results:
            rep_key = res.sales_rep if res.sales_rep else "__UNASSIGNED__"
            current_perf_dict[rep_key] = {
                'sales_rep_name': res.sales_rep_name if res.sales_rep else 'Unassigned Accounts',
                'total_revenue': res.total_revenue,
                'account_count': res.account_count,
                'avg_health_score': res.avg_health_score
            }

        # --- Query for Previous Year Performance (SQLAlchemy 2.x) ---
        prev_stmt = select(
            AccountHistoricalRevenue.sales_rep,
            func.sum(AccountHistoricalRevenue.total_revenue).label('prev_revenue'),
            # Count distinct canonical codes for accurate account count
            func.count(func.distinct(AccountHistoricalRevenue.canonical_code)).label('prev_account_count')
        ).select_from(AccountHistoricalRevenue)

        # Conditional join for distributor filter
        prev_conditions = [AccountHistoricalRevenue.year == prev_year]
        if distributor:
            prev_stmt = prev_stmt.join(
                AccountPrediction, AccountHistoricalRevenue.canonical_code == AccountPrediction.canonical_code
            )
            prev_conditions.append(AccountPrediction.distributor == distributor)

        prev_stmt = prev_stmt.where(*prev_conditions).group_by(
            AccountHistoricalRevenue.sales_rep
        )
        prev_results = db.session.execute(prev_stmt).all() # Get Row objects
        prev_perf_dict = { (row.sales_rep if row.sales_rep else "__UNASSIGNED__"): row for row in prev_results }
        # --- End Queries ---

        # --- Combine and Calculate YoY (No change needed in Python logic) ---
        combined_performance = []
        for rep_key, current_data in current_perf_dict.items():
            prev_data = prev_perf_dict.get(rep_key)
            prev_revenue = float(prev_data.prev_revenue) if prev_data and prev_data.prev_revenue else 0.0
            prev_accounts = int(prev_data.prev_account_count) if prev_data and prev_data.prev_account_count else 0
            current_revenue = float(current_data.get('total_revenue', 0.0))
            current_accounts = int(current_data.get('account_count', 0))

            # Calculate YoY Revenue Growth
            if prev_revenue > 0: revenue_growth = ((current_revenue - prev_revenue) / prev_revenue) * 100
            elif current_revenue > 0: revenue_growth = 100.0
            else: revenue_growth = 0.0

            # Calculate YoY Account Growth
            if prev_accounts > 0: account_growth = ((current_accounts - prev_accounts) / prev_accounts) * 100
            elif current_accounts > 0: account_growth = 100.0
            else: account_growth = 0.0

            combined_performance.append({
                'sales_rep': rep_key if rep_key != "__UNASSIGNED__" else None,
                'sales_rep_name': current_data.get('sales_rep_name'),
                'total_revenue': current_revenue,
                'account_count': current_accounts,
                'avg_health_score': float(current_data['avg_health_score']) if current_data.get('avg_health_score') is not None else None,
                'yoy_revenue_growth': revenue_growth,
                'yoy_account_growth': account_growth,
                'prev_year_revenue': prev_revenue if prev_data else None,
                'prev_year_accounts': prev_accounts if prev_data else None
            })

        combined_performance.sort(key=lambda x: (x.get('sales_rep_name', '') == 'Unassigned Accounts', -x['total_revenue']))

        response = {
            'year': year,
            'prev_year': prev_year,
            'distributor_filter': distributor or 'All',
            'total_reps': len(combined_performance),
            'performance': combined_performance
        }

        return jsonify(response)

    except Exception as e:
        logger.error(f"Error retrieving sales rep performance: {str(e)}", exc_info=True)
        return jsonify({"error": "An internal error occurred while retrieving sales rep performance."}), 500


@api_historical_bp.route('/years', methods=['GET'])
def get_available_years():
    """
    Get all available distinct years present in the AccountHistoricalRevenue table
    using SQLAlchemy 2.x.
    """
    try:
        # --- Use select() and scalars() for distinct column ---
        stmt = select(distinct(AccountHistoricalRevenue.year))\
               .order_by(AccountHistoricalRevenue.year.asc())
        # .scalars().all() directly gives the list of years
        year_list_results = db.session.execute(stmt).scalars().all()
        # Filter out None just in case
        year_list = sorted([year for year in year_list_results if year is not None])
        # --- End Query ---
        logger.info(f"Found available years: {year_list}")
        return jsonify({"years": year_list})
    except Exception as e:
        logger.error(f"Error retrieving available years: {str(e)}", exc_info=True)
        return jsonify({"error": "An internal server error occurred while retrieving available years."}), 500
    finally:
    # Add this block to log regardless of success/failure within try
        logger.debug(f"Final year_list being returned by /api/sales-manager/years: {year_list if 'year_list' in locals() else 'Not Defined'}")


@api_historical_bp.route('/yoy_growth', methods=['GET'])
def get_yoy_growth():
    """
    Get year-over-year growth metrics for accounts using SQLAlchemy 2.x,
    primarily from AccountPrediction, supplemented with current year revenue
    from AccountHistoricalRevenue. Joins use canonical_code.

    Query parameters:
    - year: int - Filter by year (defaults to most recent year for revenue context)
    - distributor: str - Filter by distributor
    - sales_rep: str - Filter by specific sales rep (ID or __UNASSIGNED__)
    - limit: int - Number of accounts to return (default 100)
    - sort: str - Sort field (growth, revenue) (default growth)
    - direction: str - Sort direction (asc, desc) (default desc)
    """
    try:
        # Parse query parameters
        year = request.args.get('year', type=int)
        distributor = request.args.get('distributor')
        sales_rep_id = request.args.get('sales_rep')
        limit = request.args.get('limit', 100, type=int)
        sort_field = request.args.get('sort', 'growth', type=str).lower()
        sort_direction = request.args.get('direction', 'desc', type=str).lower()

        # Determine the year for revenue context (SQLAlchemy 2.x)
        if not year:
            max_year_stmt = select(func.max(AccountHistoricalRevenue.year))
            max_year_result = db.session.scalar(max_year_stmt)
            if not max_year_result:
                return jsonify({"error": "No historical data available"}), 404
            year = int(max_year_result)
        prev_year = year - 1
        logger.info(f"Fetching YoY growth, context year={year}, distributor='{distributor}', sales_rep='{sales_rep_id}', sort={sort_field}, dir={sort_direction}, limit={limit} (v2.x)")


        # --- Base Query using SQLAlchemy 2.x select() and outerjoin() ---
        # Use outerjoin to include prediction accounts even if they have no revenue in 'year'
        stmt = select(
            AccountPrediction.canonical_code, # Use canonical_code
            AccountPrediction.name,
            AccountPrediction.sales_rep,
            AccountPrediction.sales_rep_name, # Added rep name
            AccountPrediction.distributor,
            AccountPrediction.yoy_revenue_growth,
            # Label the historical revenue to avoid ambiguity
            AccountHistoricalRevenue.total_revenue.label('current_revenue'),
            AccountPrediction.health_score,
            AccountPrediction.health_category
        ).select_from(AccountPrediction).outerjoin( # Start select from Prediction
            AccountHistoricalRevenue,
            # Join condition using canonical_code and specific year
            and_(
                AccountPrediction.canonical_code == AccountHistoricalRevenue.canonical_code,
                AccountHistoricalRevenue.year == year
            )
        )
        # --- End Base Query ---

        # --- Apply Optional Filters (on Prediction table) ---
        conditions = []
        if distributor:
            conditions.append(AccountPrediction.distributor == distributor)

        # --- Apply Sales Rep Filter LOGIC (on Prediction table) ---
        if sales_rep_id == "__UNASSIGNED__":
            logger.info("Applying filter for Unassigned Sales Reps (NULL or Empty)...")
            conditions.append(or_(
                AccountPrediction.sales_rep == None,
                AccountPrediction.sales_rep == ''
            ))
        elif sales_rep_id: # If it has a value AND is not "__UNASSIGNED__"
            logger.info(f"Applying filter for specific Sales Rep ID: {sales_rep_id}")
            conditions.append(AccountPrediction.sales_rep == sales_rep_id)
        else: # sales_rep_id is "" or None (All Reps including unassigned)
            logger.info("No specific Sales Rep filter applied (showing all).")
            pass # No *additional* rep filter needed

        if conditions:
             stmt = stmt.where(*conditions) # Apply collected conditions
        # --- End Optional Filters ---

        # --- Apply Sorting ---
        if sort_field == 'revenue':
            # Sort by the labeled 'current_revenue' from the joined table
            sort_column = func.coalesce(AccountHistoricalRevenue.total_revenue, 0) # Use coalesce on original column before labeling if needed
            order_expression = sort_column.asc() if sort_direction == 'asc' else sort_column.desc()
        else: # Default to sorting by growth
            sort_column = func.coalesce(AccountPrediction.yoy_revenue_growth, -float('inf'))
            order_expression = sort_column.asc() if sort_direction == 'asc' else sort_column.desc()

        stmt = stmt.order_by(order_expression.nullslast()) # Add nullslast
        # --- End Sorting ---

        # Apply limit
        stmt = stmt.limit(limit)

        # Execute the query and get Row objects
        results_rows = db.session.execute(stmt).all()
        logger.info(f"YoY growth query returned {len(results_rows)} accounts.")


        # Get the partner set ONCE before the comprehension
        partner_codes_set = getattr(config, 'CURRENT_YEAR_PARTNER_CODES', set())

        # --- Format the response (Accessing data from Row objects) ---
        accounts = [
            {
                'canonical_code': row.canonical_code, # Use canonical_code
                'name': row.name,
                'sales_rep': row.sales_rep,
                'sales_rep_name': row.sales_rep_name, # Added rep name
                'distributor': row.distributor,
                'current_revenue': float(row.current_revenue) if row.current_revenue is not None else 0.0,
                'yoy_growth': float(row.yoy_revenue_growth) if row.yoy_revenue_growth is not None else None,
                'health_score': float(row.health_score) if row.health_score is not None else None,
                'health_category': row.health_category,
                'is_partner': bool(
                                get_base_code_from_canonical(row.canonical_code) and \
                                get_base_code_from_canonical(row.canonical_code) in partner_codes_set
                                )
            }
            for row in results_rows # Iterate through Row objects
        ]
        # --- End Formatting ---

        response = {
            'context_year': year,
            'prev_year': prev_year,
            'distributor_filter': distributor or 'All',
            'sales_rep_filter': sales_rep_id or 'All',
            'sort_by': sort_field,
            'sort_direction': sort_direction,
            'limit': limit,
            'total_accounts_returned': len(accounts),
            'accounts': accounts
        }

        return jsonify(response)

    except Exception as e:
        logger.error(f"Error retrieving YoY growth data: {str(e)}", exc_info=True)
        return jsonify({"error": "An internal error occurred while retrieving YoY growth data."}), 500