# routes/api_routes_strategic.py

from flask import Blueprint, jsonify, request
from sqlalchemy import func, desc, asc, or_, select, extract 
from models import db, AccountPrediction, Transaction, AccountHistoricalRevenue 
from config import TOP_30_SET 
from datetime import datetime, timedelta, date
import logging
import json
from collections import defaultdict, OrderedDict
import math

logger = logging.getLogger(__name__) 

api_strategic_bp = Blueprint('api_strategic', __name__, url_prefix='/api/strategic')


def _clean_distributor(raw: str | None) -> str | None:
    """Normalise distributor names once so the UI gets stable keys."""
    if not raw:
        return None
    return raw.strip().upper()


# --- get_sku_description_map (remains the same) ---
def get_sku_description_map(sku_list: list) -> dict:
    if not sku_list:
        return {}
    
    # Ensure all SKUs are clean strings for the query
    valid_skus_for_query = list(set(str(s).strip() for s in sku_list if s and str(s).strip()))
    if not valid_skus_for_query:
        return {str(s).strip(): "Description N/A" for s in sku_list if s and str(s).strip()}

    fetched_sku_to_desc = {}
    try:
        # --- SIMPLIFIED AND MORE ROBUST QUERY ---
        # This subquery finds the most recent transaction for each SKU that has a valid description.
        subq = select(
            Transaction.item_code,
            Transaction.description,
            func.row_number().over(
                partition_by=Transaction.item_code,
                order_by=Transaction.posting_date.desc()  # Order by the most recent date
            ).label('rn')
        ).where(
            Transaction.item_code.in_(valid_skus_for_query),
            Transaction.description.isnot(None),
            Transaction.description != ''
        ).subquery()

        # Final query selects only the top row for each SKU (the most recent one)
        stmt = select(subq.c.item_code, subq.c.description).where(subq.c.rn == 1)
        
        results = db.session.execute(stmt).fetchall()
        for sku, description in results:
            fetched_sku_to_desc[str(sku).strip()] = description
        # --- END SIMPLIFIED QUERY ---

    except Exception as e:
        logger.error(f"Error fetching SKU descriptions map with new query: {e}", exc_info=True)
        # Fallback to returning N/A for all on error
        return {sku: "Description N/A" for sku in valid_skus_for_query}

    # Final mapping to ensure all requested SKUs get a value
    final_map = {}
    for clean_sku in valid_skus_for_query:
        final_map[clean_sku] = fetched_sku_to_desc.get(clean_sku, "Description N/A")
        
    return final_map

# --- get_detailed_product_history_by_quarter (remains the same) ---
def get_detailed_product_history_by_quarter( monthly_aggregate_results: list, master_sku_desc_map: dict, top_30_skus_set: set, canonical_code_for_logging: str ):
    logger.info(f"Processing detailed product history for {canonical_code_for_logging} using pre-fetched monthly aggregates and master SKU descriptions.")
    quarterly_sku_aggregates = defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: { "total_quantity": 0, "total_revenue": 0.0 })))
    for row in monthly_aggregate_results:
        year_str = str(row.year)
        try:
            month_int = int(row.month_val) 
            if 1 <= month_int <= 3: quarter_key = "Q1"
            elif 4 <= month_int <= 6: quarter_key = "Q2"
            elif 7 <= month_int <= 9: quarter_key = "Q3"
            elif 10 <= month_int <= 12: quarter_key = "Q4"
            else: continue
        except (ValueError, TypeError): continue
        sku = str(row.item_code); agg_data = quarterly_sku_aggregates[year_str][quarter_key][sku]
        agg_data["total_quantity"] += int(row.sum_quantity or 0); agg_data["total_revenue"] += float(row.sum_revenue or 0.0)
    
    final_response_structure = OrderedDict(); sorted_years = sorted(quarterly_sku_aggregates.keys(), key=int)
    data_from_absolute_previous_quarter_for_qoq = {}
    
    for year_str in sorted_years:
        final_response_structure[year_str] = OrderedDict(); year_data_for_quarters = quarterly_sku_aggregates[year_str]
        for q_key in ["Q1", "Q2", "Q3", "Q4"]:
            current_quarter_sku_details_map = year_data_for_quarters.get(q_key, {}); current_quarter_product_list_frontend = []
            current_quarter_skus_set_for_metrics = set(current_quarter_sku_details_map.keys()); qty_top_30_this_qtr = 0; rev_total_this_qtr = 0.0; carried_top_30_details_this_qtr = []
            
            for sku, details in current_quarter_sku_details_map.items():
                chosen_description = master_sku_desc_map.get(sku, "Description N/A"); is_top_30_val = bool(sku in top_30_skus_set)
                current_qty = details["total_quantity"]; current_rev = details["total_revenue"]
                status_in_qtr_vs_prev_q = "Repurchased"; 
                if sku not in data_from_absolute_previous_quarter_for_qoq: status_in_qtr_vs_prev_q = "Newly Added this Qtr (vs Prev Qtr)"
                
                qoq_qty_pct_change, qoq_rev_pct_change = None, None; prev_q_sku_data = data_from_absolute_previous_quarter_for_qoq.get(sku)
                if prev_q_sku_data:
                    prev_qty = prev_q_sku_data["total_quantity"]; prev_rev = prev_q_sku_data["total_revenue"]
                    if prev_qty > 0: qoq_qty_pct_change = round(((current_qty - prev_qty) / prev_qty) * 100, 1)
                    elif current_qty > 0 : qoq_qty_pct_change = None 
                    if prev_rev > 0: qoq_rev_pct_change = round(((current_rev - prev_rev) / prev_rev) * 100, 1)
                    elif current_rev > 0 : qoq_rev_pct_change = None 
                elif current_qty > 0 or current_rev > 0 : qoq_qty_pct_change = None; qoq_rev_pct_change = None 
                
                current_quarter_product_list_frontend.append({ "sku": sku, "description": chosen_description, "quantity": current_qty, "revenue": round(current_rev, 2), "is_top_30": is_top_30_val, "status_in_qtr": status_in_qtr_vs_prev_q, "qoq_qty_pct_change": qoq_qty_pct_change, "qoq_rev_pct_change": qoq_rev_pct_change })
                rev_total_this_qtr += current_rev
                if is_top_30_val: 
                    qty_top_30_this_qtr += current_qty
                    carried_top_30_details_this_qtr.append({ "sku": sku, "description": chosen_description, "quantity": current_qty, "revenue": round(current_rev, 2) })
            
            current_quarter_product_list_frontend.sort(key=lambda x: x["revenue"], reverse=True)
            
            previous_quarter_skus_set = set(data_from_absolute_previous_quarter_for_qoq.keys())
            
            added_skus_from_set_diff = list(current_quarter_skus_set_for_metrics - previous_quarter_skus_set)
            added_skus_details = []
            for s_added in added_skus_from_set_diff:
                details = current_quarter_sku_details_map.get(s_added)
                if details:
                    added_skus_details.append({
                        "sku": s_added,
                        "description": master_sku_desc_map.get(s_added, "Description N/A"),
                        "quantity": details.get("total_quantity", 0),
                        "revenue": round(details.get("total_revenue", 0.0), 2)
                    })
            added_skus_details.sort(key=lambda x: x["revenue"], reverse=True) # Optional sort

            dropped_skus_from_set_diff = list(previous_quarter_skus_set - current_quarter_skus_set_for_metrics)
            dropped_skus_details = [ {"sku": s, "description": master_sku_desc_map.get(s, "Description N/A")} for s in dropped_skus_from_set_diff ]
            dropped_skus_details.sort(key=lambda x: x["description"]) # Optional sort by description

            repurchased_skus_set = current_quarter_skus_set_for_metrics.intersection(previous_quarter_skus_set)
            qty_repurchased_this_qtr = sum(current_quarter_sku_details_map.get(sku, {}).get("total_quantity", 0) for sku in repurchased_skus_set)
            
            repurchased_skus_details_list = []
            for sku_repurchased in repurchased_skus_set:
                details = current_quarter_sku_details_map.get(sku_repurchased)
                if details:
                    repurchased_skus_details_list.append({
                        "sku": sku_repurchased,
                        "description": master_sku_desc_map.get(sku_repurchased, "Description N/A"),
                        "quantity": details.get("total_quantity", 0),
                        "revenue": round(details.get("total_revenue", 0.0), 2)
                    })
            repurchased_skus_details_list.sort(key=lambda x: x["revenue"], reverse=True)

            # Sort carried_top_30_details_this_qtr (already populated with SKU, desc, qty, rev)
            carried_top_30_details_this_qtr.sort(key=lambda x: x["revenue"], reverse=True)

            final_response_structure[year_str][q_key] = { 
                "products": current_quarter_product_list_frontend, 
                "metrics": { 
                    "total_items_in_quarter": len(current_quarter_product_list_frontend), 
                    "total_revenue_in_quarter": round(rev_total_this_qtr, 2), 
                    "items_added_details": added_skus_details, # Now includes qty/rev
                    "items_dropped_details": dropped_skus_details, 
                    "items_repurchased_count": len(repurchased_skus_set), 
                    "quantity_repurchased": qty_repurchased_this_qtr, 
                    "repurchased_skus_details": repurchased_skus_details_list, # ADDED
                    "top_30_skus_carried_details": carried_top_30_details_this_qtr, # Already includes qty/rev
                    "count_top_30_skus_carried": len(carried_top_30_details_this_qtr), 
                    "quantity_top_30_carried": qty_top_30_this_qtr 
                } 
            }
            data_from_absolute_previous_quarter_for_qoq = current_quarter_sku_details_map.copy() if current_quarter_sku_details_map else {}
    return final_response_structure

# --- get_strategic_accounts_data (remains the same) ---
@api_strategic_bp.route('/accounts', methods=['GET'])
def get_strategic_accounts_data():
    """Get strategic accounts data with filtering and summary statistics."""
    logger.info("Received request for strategic accounts data")
    from config import HEALTH_POOR_THRESHOLD, PRIORITY_PACE_DECLINE_PCT_THRESHOLD, GROWTH_PACE_INCREASE_PCT_THRESHOLD 
    
    sales_rep_id = request.args.get('sales_rep')
    distributor = request.args.get('distributor')
    health_category = request.args.get('health_category')
    rfm_segment = request.args.get('rfm_segment')
    
    try:
        stmt = select(AccountPrediction)
        conditions = []
        
        if distributor: 
            conditions.append(AccountPrediction.distributor == distributor.strip().upper())
        if health_category: 
            conditions.append(AccountPrediction.health_category == health_category)
        if rfm_segment: 
            conditions.append(AccountPrediction.rfm_segment == rfm_segment)
        if sales_rep_id == "__UNASSIGNED__": 
            conditions.append(or_(AccountPrediction.sales_rep == None, AccountPrediction.sales_rep == ''))
        elif sales_rep_id: 
            conditions.append(AccountPrediction.sales_rep == sales_rep_id)
            
        if conditions: 
            stmt = stmt.where(*conditions)
            
        accounts = db.session.execute(stmt).scalars().all()
        
        # --- CORRECTED LOGIC from your original file ---
        all_skus_needed = set()
        for acc in accounts:
            all_skus_needed.update(str(s).strip() for s in (acc.carried_top_products or []) if str(s).strip())
            # This is the corrected block for missing_top_products
            for item in (acc.missing_top_products or []):
                if isinstance(item, dict) and item.get('sku'):
                    all_skus_needed.add(str(item['sku']).strip())
        # --- END CORRECTION ---

        master_sku_desc_map = get_sku_description_map(list(all_skus_needed))
        
        # (The rest of the function from your original file is correct and can be used as-is)
        total_yep = sum(acc.yep_revenue for acc in accounts if acc.yep_revenue)
        overdue_count = sum(1 for acc in accounts if acc.days_overdue and acc.days_overdue > 0)
        avg_coverage = None
        distribution = {'low': 0, 'medium': 0, 'high': 0, 'unknown': 0}
        coverages = [acc.product_coverage_percentage for acc in accounts if acc.product_coverage_percentage is not None]
        
        if coverages:
            avg_coverage = round(sum(coverages) / len(coverages), 1)
            distribution['low'] = sum(1 for c in coverages if c < 20)
            distribution['medium'] = sum(1 for c in coverages if 20 <= c < 50)
            distribution['high'] = sum(1 for c in coverages if c >= 50)
        distribution['unknown'] = len(accounts) - len(coverages)
        
        active_enhanced_priority_sum = sum(acc.enhanced_priority_score for acc in accounts if acc.enhanced_priority_score is not None)
        active_enhanced_priority_count = sum(1 for acc in accounts if acc.enhanced_priority_score is not None)
        avg_priority_score_summary = round(active_enhanced_priority_sum / active_enhanced_priority_count, 1) if active_enhanced_priority_count > 0 else None
        
        health_scores = [acc.health_score for acc in accounts if acc.health_score is not None]
        avg_health_score_summary = round(sum(health_scores) / len(health_scores), 1) if health_scores else None
        
        output_list = []
        for acc in accounts:
            described_carried = [
                {"sku": str(s).strip(), "description": master_sku_desc_map.get(str(s).strip(), "Description N/A")} 
                for s in (acc.carried_top_products or [])
            ]
            
            described_missing = []
            for item in (acc.missing_top_products or []):
                if isinstance(item, dict) and item.get('sku'):
                    sku = str(item['sku']).strip()
                    described_missing.append({
                        "sku": sku, 
                        "description": master_sku_desc_map.get(sku, "Description N/A"), 
                        "reason": item.get('placeholder_insight', 'Missing Top 30 Product')
                    })
            
            acc_data = {col.name: getattr(acc, col.name) for col in acc.__table__.columns}
            acc_data['carried_top_products'] = described_carried
            acc_data['missing_top_products'] = described_missing
            
            for field in ['last_purchase_date', 'next_expected_purchase_date']:
                if acc_data.get(field): 
                    acc_data[field] = acc_data[field].isoformat()
                    
            output_list.append(acc_data)
        
        return jsonify({
            "accounts": output_list, 
            "summary_stats": {
                "total_accounts": len(accounts),
                "total_yep": total_yep,
                "overdue_count": overdue_count,
                "average_coverage": avg_coverage,
                "coverage_distribution": distribution,
                "avg_priority_score": avg_priority_score_summary,
                "avg_health_score": avg_health_score_summary,
                "count_priority1": sum(1 for acc in accounts if acc.enhanced_priority_score and acc.enhanced_priority_score >= 75),
                "count_priority2": sum(1 for acc in accounts if acc.enhanced_priority_score and 50 <= acc.enhanced_priority_score < 75),
                "count_due_this_week": sum(1 for acc in accounts if acc.next_expected_purchase_date and 0 <= (acc.next_expected_purchase_date.date() - datetime.utcnow().date()).days <= 7),
                "count_overdue": overdue_count,
                "count_low_health": sum(1 for acc in accounts if acc.health_score and acc.health_score < HEALTH_POOR_THRESHOLD),
                "count_low_pace": sum(1 for acc in accounts if acc.pace_vs_ly is not None and acc.py_total_revenue is not None and acc.py_total_revenue > 0 and (acc.pace_vs_ly / acc.py_total_revenue * 100) < PRIORITY_PACE_DECLINE_PCT_THRESHOLD),
                "count_high_pace": sum(1 for acc in accounts if acc.pace_vs_ly is not None and acc.py_total_revenue is not None and acc.py_total_revenue > 0 and (acc.pace_vs_ly / acc.py_total_revenue * 100) > GROWTH_PACE_INCREASE_PCT_THRESHOLD),
                "count_growth_opps": sum(1 for acc in accounts if is_growth_opportunity_api(acc)),
            }
        })
        
    except Exception as e:
        logger.error(f"Error fetching strategic accounts data: {str(e)}", exc_info=True)
        return jsonify({"error": "An internal server error occurred."}), 500

# --- is_growth_opportunity_api (remains the same) ---
def is_growth_opportunity_api(account_prediction_obj):
    from config import GROWTH_HEALTH_THRESHOLD, GROWTH_PACE_INCREASE_PCT_THRESHOLD, GROWTH_MISSING_PRODUCTS_THRESHOLD
    acc = account_prediction_obj 
    if (acc.health_score or 0) < GROWTH_HEALTH_THRESHOLD: return False
    if acc.pace_vs_ly is not None and acc.py_total_revenue is not None and acc.py_total_revenue > 0:
        pace_percent = (acc.pace_vs_ly / acc.py_total_revenue) * 100
        if pace_percent >= GROWTH_PACE_INCREASE_PCT_THRESHOLD: return True
    missing_products_list = acc.missing_top_products 
    if isinstance(missing_products_list, list) and len(missing_products_list) >= GROWTH_MISSING_PRODUCTS_THRESHOLD: return True
    if acc.rfm_segment in ["Champions", "Loyal Customers"] and acc.next_expected_purchase_date:
        days_until_due = (acc.next_expected_purchase_date.date() - datetime.utcnow().date()).days
        if 0 <= days_until_due <= 14: return True
    return False


# --- UPDATED get_account_details function ---
@api_strategic_bp.route('/accounts/<path:canonical_code>/details', methods=['GET'])
def get_account_details(canonical_code):
    logger.info(f"Fetching details for account: {canonical_code} (with enhanced timeline and data freshness)")
    if not canonical_code:
        return jsonify({"error": "Canonical code is required."}), 400

    try:
        prediction_stmt = select(AccountPrediction).where(AccountPrediction.canonical_code == canonical_code)
        prediction = db.session.execute(prediction_stmt).scalar_one_or_none()

        if not prediction:
            return jsonify({"error": f"Account with canonical code '{canonical_code}' not found."}), 404

        # +++ Parse Rolling SKU Analysis +++
        rolling_sku_analysis_list = []
        if prediction.rolling_sku_analysis_json:
            try:
                json_string_safe = prediction.rolling_sku_analysis_json.replace(': NaN', ': null')
                rolling_sku_analysis_list = json.loads(json_string_safe)
            except json.JSONDecodeError:
                logger.warning(f"Could not decode rolling_sku_analysis_json for {canonical_code}")
        # +++ END NEW +++

        # --- Collect SKUs for Description Fetching ---
        all_skus_for_descriptions = set()
        if prediction.carried_top_products:
            all_skus_for_descriptions.update(str(s).strip() for s in prediction.carried_top_products if str(s).strip())
        if prediction.missing_top_products:
             # missing_top_products is a list of dicts, so we extract the 'sku' value
            for item in (prediction.missing_top_products or []):
                if isinstance(item, dict) and item.get('sku'):
                    all_skus_for_descriptions.add(str(item['sku']).strip())

        products_purchased_sku_list_cleaned = []
        if prediction.products_purchased:
            try:
                parsed_list = json.loads(prediction.products_purchased)
                if isinstance(parsed_list, list):
                    products_purchased_sku_list_cleaned = [str(s).strip() for s in parsed_list if str(s).strip()]
            except json.JSONDecodeError:
                logger.warning(f"Could not decode products_purchased for {canonical_code}")
        all_skus_for_descriptions.update(products_purchased_sku_list_cleaned)

        if prediction.recommended_products_next_purchase_json:
            try:
                recs_list_raw = json.loads(prediction.recommended_products_next_purchase_json)
                if isinstance(recs_list_raw, list):
                    for item_raw in recs_list_raw:
                        if isinstance(item_raw, dict) and item_raw.get("sku"):
                            all_skus_for_descriptions.add(str(item_raw.get("sku")).strip())
            except json.JSONDecodeError:
                logger.warning(f"Could not decode recommended_products_next_purchase_json for SKU collection for {canonical_code}")
        
        stmt_monthly_aggregates = select(
            Transaction.year, extract('month', Transaction.posting_date).label('month_val'),
            Transaction.item_code, func.sum(Transaction.quantity).label('sum_quantity'),
            func.sum(Transaction.revenue).label('sum_revenue')
        ).where(
            Transaction.canonical_code == canonical_code, Transaction.item_code.isnot(None), Transaction.item_code != ''
        ).group_by(
            Transaction.year, extract('month', Transaction.posting_date), Transaction.item_code
        ).order_by(
            Transaction.year.asc(), extract('month', Transaction.posting_date).asc(), Transaction.item_code
        )
        monthly_aggregate_results = db.session.execute(stmt_monthly_aggregates).fetchall()
        for row in monthly_aggregate_results:
            if row.item_code and str(row.item_code).strip():
                all_skus_for_descriptions.add(str(row.item_code).strip())

        master_sku_desc_map = {}
        if all_skus_for_descriptions:
            master_sku_desc_map = get_sku_description_map(list(all_skus_for_descriptions))

        # --- Prepare prediction_data_dict (with fix for missing_top_products) ---
        prediction_data_dict = {col.name: getattr(prediction, col.name) for col in prediction.__table__.columns}
        date_fields = ['last_purchase_date', 'next_expected_purchase_date', 'rep_last_order_date', 'reminder_sent_at', 'notified_last_purchase_date']
        for field in date_fields:
            if prediction_data_dict.get(field) and isinstance(prediction_data_dict[field], (datetime, date)):
                prediction_data_dict[field] = prediction_data_dict[field].isoformat()
            elif prediction_data_dict.get(field) is not None:
                prediction_data_dict[field] = None
        
        prediction_data_dict['carried_top_products'] = [ {"sku": str(s).strip(), "description": master_sku_desc_map.get(str(s).strip(), "Description N/A")} for s in (prediction.carried_top_products or []) ]
        
        described_missing_list = []
        raw_missing_list_of_dicts = prediction.missing_top_products or [] 
        if isinstance(raw_missing_list_of_dicts, list):
            for item_dict in raw_missing_list_of_dicts:
                if isinstance(item_dict, dict) and 'sku' in item_dict:
                    sku = str(item_dict.get('sku', '')).strip()
                    if sku:
                        described_missing_list.append({
                            'sku': sku,
                            'description': master_sku_desc_map.get(sku, "Description N/A"),
                            'reason': item_dict.get('placeholder_insight', 'Missing Top 30 Product')
                        })
        prediction_data_dict['missing_top_products'] = described_missing_list
        
        described_products_purchased = []
        if prediction.products_purchased:
            try:
                parsed_list = json.loads(prediction.products_purchased)
                if isinstance(parsed_list, list):
                    described_products_purchased = [ {"sku": str(s).strip(), "description": master_sku_desc_map.get(str(s).strip(), "Description N/A")} for s in parsed_list if str(s).strip() ]
            except json.JSONDecodeError: pass
        prediction_data_dict['products_purchased'] = described_products_purchased

        described_recommended_products_for_growth_engine = []
        raw_recommended_json = prediction_data_dict.get('recommended_products_next_purchase_json')
        if raw_recommended_json:
            try:
                recs_list_raw = json.loads(raw_recommended_json)
                if isinstance(recs_list_raw, list):
                    for item_raw in recs_list_raw:
                        if isinstance(item_raw, dict) and item_raw.get("sku"):
                            sku_val = str(item_raw.get("sku")).strip()
                            described_recommended_products_for_growth_engine.append({ "sku": sku_val, "description": master_sku_desc_map.get(sku_val, "Description N/A"), "reason": item_raw.get("reason", "Recommended") })
            except json.JSONDecodeError:
                logger.warning(f"Could not decode recommended_products_next_purchase_json for description: {canonical_code}")

        # --- Historical Summary & Analysis Data ---
        hist_summary_stmt = select( AccountHistoricalRevenue.year, AccountHistoricalRevenue.total_revenue.label('revenue'), AccountHistoricalRevenue.transaction_count.label('transactions') ).where(AccountHistoricalRevenue.canonical_code == canonical_code).order_by(AccountHistoricalRevenue.year.asc())
        historical_summary_list = [dict(row) for row in db.session.execute(hist_summary_stmt).mappings().all()]
        analysis_data_dict = { "revenue_trend": { "slope": prediction_data_dict.get('revenue_trend_slope'), "r_squared": prediction_data_dict.get('revenue_trend_r_squared'), "intercept": prediction_data_dict.get('revenue_trend_intercept'), "forecast_next": None, "forecast_year": None, "forecast_method": None, "model_type": "Linear Regression" if prediction_data_dict.get('revenue_trend_slope') is not None else "N/A" } }
        if historical_summary_list and len(historical_summary_list) >= 2:
            next_year_to_forecast = datetime.utcnow().year + 1
            slope_from_pred = prediction_data_dict.get('revenue_trend_slope')
            intercept_from_pred = prediction_data_dict.get('revenue_trend_intercept')
            if slope_from_pred is not None and intercept_from_pred is not None:
                try:
                    analysis_data_dict["revenue_trend"]["forecast_next"] = float(slope_from_pred * next_year_to_forecast + intercept_from_pred)
                    analysis_data_dict["revenue_trend"]["forecast_method"] = "linear_regression"
                except Exception:
                    analysis_data_dict["revenue_trend"]["forecast_next"] = None
                    analysis_data_dict["revenue_trend"]["forecast_method"] = "calculation_error"
            analysis_data_dict["revenue_trend"]["forecast_year"] = next_year_to_forecast

        growth_engine_data = { "target_yep_plus_1_pct": prediction_data_dict.get('target_yep_plus_1_pct'), "additional_revenue_needed_eoy": prediction_data_dict.get('additional_revenue_needed_eoy'), "suggested_next_purchase_amount": prediction_data_dict.get('suggested_next_purchase_amount'), "recommended_products": described_recommended_products_for_growth_engine, "message": prediction_data_dict.get('growth_engine_message'), "already_on_track": (prediction_data_dict.get('additional_revenue_needed_eoy') is not None and prediction_data_dict.get('additional_revenue_needed_eoy') <= 0) }
        

        # --- THIS IS THE FIX ---
        # Also clean the growth_engine_data dictionary for any NaN values
        cleaned_growth_engine_data = {}
        for key, value in growth_engine_data.items():
            if isinstance(value, float) and math.isnan(value):
                cleaned_growth_engine_data[key] = None
            else:
                cleaned_growth_engine_data[key] = value
        # --- END FIX ---


        revenue_history_data_dict = {"years": [str(h['year']) for h in historical_summary_list], "revenues": [h['revenue'] for h in historical_summary_list]}
        current_year = datetime.utcnow().year; previous_year = current_year - 1
        
        def get_daily_timeline(target_year):
            """
            Enhanced timeline with order details and distributor information.
            Returns timeline points with drill-down details for tooltips.
            """
            rows = db.session.execute(
                select(
                    func.date(Transaction.posting_date).label("purchase_date"),
                    Transaction.distributor,
                    Transaction.item_code,
                    func.sum(Transaction.quantity).label("total_qty"),
                    func.sum(Transaction.revenue).label("total_rev"),
                )
                .where(
                    Transaction.canonical_code == canonical_code,
                    extract('year', Transaction.posting_date) == target_year,
                    Transaction.distributor.isnot(None), 
                    Transaction.distributor != "",
                )
                .group_by(
                    func.date(Transaction.posting_date), 
                    Transaction.distributor, 
                    Transaction.item_code
                )
                .order_by(func.date(Transaction.posting_date))
            ).mappings().all()

            timeline_points = {}
            for row in rows:
                date_key = row["purchase_date"]
                distributor_clean = _clean_distributor(row["distributor"])
                point_key = (date_key, distributor_clean)
                
                if point_key not in timeline_points:
                    timeline_points[point_key] = {
                        "x": date_key.isoformat(), 
                        "daily_revenue": 0.0,
                        "distributor": distributor_clean or "UNKNOWN", 
                        "details": []
                    }
                
                point = timeline_points[point_key]
                point["daily_revenue"] += float(row["total_rev"] or 0.0)
                point["details"].append({
                    "sku": row["item_code"],
                    "description": master_sku_desc_map.get(str(row["item_code"]).strip(), "Description N/A"),
                    "quantity": int(row["total_qty"] or 0),
                    "revenue": float(row["total_rev"] or 0.0)
                })
            
            return list(timeline_points.values())

        cy_timeline_data_list = get_daily_timeline(current_year)
        py_timeline_data_list = get_daily_timeline(previous_year)
        
        # --- Data Freshness Indicator (Global Distributor Upload Tracking) ---
        distributor_uploads_query = select(
            Transaction.distributor,
            func.max(Transaction.posting_date).label("last_upload_date"),
        ).where(
            Transaction.distributor.isnot(None),
            Transaction.distributor != ""
        ).group_by(Transaction.distributor)
        
        distributor_upload_rows = db.session.execute(distributor_uploads_query).all()
        
        # --- NEW: Add the distributor cadence map ---
        DISTRIBUTOR_CADENCE_MAP = {
            'DIRECT': 'daily',
            'KEHE': 'weekly',
            'PALKO': 'monthly',
            'SUPERNATURAL': 'monthly',
            'THRESHOLD': 'weekly',
            'UNFI': 'weekly'
        }
        
        distributor_uploads = []
        for dist, last_upload in distributor_upload_rows:
            if not dist:
                continue
            
            clean_dist = _clean_distributor(dist)
            
            last_upload_date = last_upload.date() if isinstance(last_upload, datetime) else last_upload
            next_expected_date = last_upload_date + timedelta(days=30)
            
            distributor_uploads.append({
                "distributor": clean_dist,
                "last_upload": last_upload_date.isoformat(),
                "next_expected_upload": next_expected_date.isoformat(),
                "days_since_last_upload": (datetime.utcnow().date() - last_upload_date).days,
                "is_overdue": (datetime.utcnow().date() - last_upload_date).days > 35,
                "cadence": DISTRIBUTOR_CADENCE_MAP.get(clean_dist, 'unknown') # <-- ADD THIS LINE
            })
        
        detailed_product_history = get_detailed_product_history_by_quarter( monthly_aggregate_results, master_sku_desc_map, TOP_30_SET, canonical_code )

        # +++ MODIFICATION: Add is_top_30 flag to yearly product summary +++
        yearly_product_aggregates_raw = defaultdict(lambda: defaultdict(lambda: {"total_quantity_year": 0, "total_revenue_year": 0.0}))
        for row_monthly_agg in monthly_aggregate_results:
            year_str = str(row_monthly_agg.year)
            sku = str(row_monthly_agg.item_code).strip()
            if sku:
                yearly_product_aggregates_raw[year_str][sku]["total_quantity_year"] += int(row_monthly_agg.sum_quantity or 0)
                yearly_product_aggregates_raw[year_str][sku]["total_revenue_year"] += float(row_monthly_agg.sum_revenue or 0.0)

        yearly_product_summary_final = OrderedDict()
        sorted_years_for_summary = sorted(yearly_product_aggregates_raw.keys(), key=int) 

        for year_str in sorted_years_for_summary:
            skus_data_for_year = yearly_product_aggregates_raw[year_str]
            product_list_for_year_table = []
            for sku, data in skus_data_for_year.items():
                product_list_for_year_table.append({
                    "sku": sku,
                    "description": master_sku_desc_map.get(sku, "Description N/A"),
                    "total_quantity_year": data["total_quantity_year"],
                    "total_revenue_year": round(data["total_revenue_year"], 2),
                    "is_top_30": sku in TOP_30_SET  # <-- THE NEW LINE
                })
            product_list_for_year_table.sort(key=lambda x: x["total_revenue_year"], reverse=True)
            yearly_product_summary_final[year_str] = product_list_for_year_table
        # +++ END MODIFICATION +++

        # +++ Add is_top_30 flag to rolling SKU analysis +++
        for sku_item in rolling_sku_analysis_list:
            if isinstance(sku_item, dict) and 'item_code' in sku_item:
                sku_item['is_top_30'] = sku_item['item_code'] in TOP_30_SET
        # +++ END ADDITION +++

        # --- DEBUG: Print distributor uploads data to terminal ---
        #print("--- DEBUG: Data being sent to frontend ---")
        #print("distributor_uploads:")
        #for upload in distributor_uploads:
        #    print(f"  {upload}")
        #print("------------------------------------------")


        cleaned_prediction_data = {}
        for key, value in prediction_data_dict.items():
            if isinstance(value, float) and math.isnan(value):
                cleaned_prediction_data[key] = None # Replace NaN with None
            else:
                cleaned_prediction_data[key] = value
        # --- END FIX ---

        # --- Final JSON Response ---
        return jsonify({
            "prediction": cleaned_prediction_data,
            "historical_summary": historical_summary_list, 
            "analysis": analysis_data_dict,
            "growth_engine": cleaned_growth_engine_data,
            "chart_data": {
                "revenue_history": revenue_history_data_dict,
                "cy_purchase_timeline": cy_timeline_data_list,
                "py_purchase_timeline": py_timeline_data_list,
                "detailed_product_history_by_quarter": detailed_product_history,
                "yearly_product_summary_table_data": yearly_product_summary_final
            },
            "distributor_uploads": distributor_uploads,  # <-- NEW KEY FOR DATA FRESHNESS
            "rolling_sku_analysis": rolling_sku_analysis_list
        })

    except Exception as e:
        logger.error(f"Error fetching account details for {canonical_code}: {str(e)}", exc_info=True)
        return jsonify({"error": "An internal server error occurred."}), 500