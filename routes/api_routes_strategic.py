# routes/api_routes_strategic.py

from flask import Blueprint, jsonify, request
from sqlalchemy import func, desc, asc, or_, select, extract 
from models import db, AccountPrediction, Transaction, AccountHistoricalRevenue 
from config import TOP_30_SET 
from datetime import datetime, timedelta, date
import logging
import json
from collections import defaultdict, OrderedDict

logger = logging.getLogger(__name__) 

api_strategic_bp = Blueprint('api_strategic', __name__, url_prefix='/api/strategic')

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
    # ... (implementation remains the same as your last provided version) ...
    logger.info("Received request for strategic accounts data (optimized with SKU description map)")
    from config import HEALTH_POOR_THRESHOLD, PRIORITY_PACE_DECLINE_PCT_THRESHOLD, GROWTH_PACE_INCREASE_PCT_THRESHOLD 
    sales_rep_id = request.args.get('sales_rep'); distributor = request.args.get('distributor'); health_category = request.args.get('health_category'); rfm_segment = request.args.get('rfm_segment')
    logger.info(f"Filtering strategic accounts: sales_rep='{sales_rep_id}', distributor='{distributor}', health_category='{health_category}', rfm_segment='{rfm_segment}'")
    try:
        stmt = select(AccountPrediction); conditions = []
        if distributor: conditions.append(AccountPrediction.distributor == distributor)
        if health_category: conditions.append(AccountPrediction.health_category == health_category)
        if rfm_segment: conditions.append(AccountPrediction.rfm_segment == rfm_segment)
        if sales_rep_id == "__UNASSIGNED__": conditions.append(or_(AccountPrediction.sales_rep == None, AccountPrediction.sales_rep == ''))
        elif sales_rep_id: conditions.append(AccountPrediction.sales_rep == sales_rep_id)
        if conditions: stmt = stmt.where(*conditions)
        accounts = db.session.execute(stmt).scalars().all(); logger.info(f"Found {len(accounts)} accounts matching filters.")
        all_skus_needed = set()
        if accounts:
            for acc_data in accounts:
                if acc_data.carried_top_products: all_skus_needed.update(str(s).strip() for s in acc_data.carried_top_products if str(s).strip())
                if acc_data.missing_top_products: all_skus_needed.update(str(s).strip() for s in acc_data.missing_top_products if str(s).strip())
        master_sku_desc_map = {}; 
        if all_skus_needed: master_sku_desc_map = get_sku_description_map(list(all_skus_needed))
        avg_coverage = None; distribution = {'low': 0, 'medium': 0, 'high': 0, 'unknown': 0}; coverages = []; overdue_count = 0; total_yep_for_summary = 0.0; active_enhanced_priority_sum = 0; active_enhanced_priority_count = 0
        if accounts:
            for acc_summary in accounts: 
                cov_percent = acc_summary.product_coverage_percentage
                if cov_percent is not None: coverages.append(cov_percent)
                if acc_summary.days_overdue is not None and acc_summary.days_overdue > 0: overdue_count += 1
                if acc_summary.yep_revenue is not None: total_yep_for_summary += acc_summary.yep_revenue
                if acc_summary.enhanced_priority_score is not None: active_enhanced_priority_sum += acc_summary.enhanced_priority_score; active_enhanced_priority_count +=1
            if coverages: avg_coverage = round(sum(coverages) / len(coverages), 1); distribution['low'] = sum(1 for c in coverages if c < 20); distribution['medium'] = sum(1 for c in coverages if 20 <= c < 50); distribution['high'] = sum(1 for c in coverages if c >= 50)
            distribution['unknown'] = len(accounts) - len(coverages)
        avg_priority_score_summary = round(active_enhanced_priority_sum / active_enhanced_priority_count, 1) if active_enhanced_priority_count > 0 else None
        avg_health_score_summary = round(sum(acc.health_score for acc in accounts if acc.health_score is not None) / len([acc for acc in accounts if acc.health_score is not None]), 1) if any(acc.health_score is not None for acc in accounts) else None
        output_list = []
        for acc in accounts:
            described_carried_list = []; 
            if acc.carried_top_products: described_carried_list = [ {"sku": str(s).strip(), "description": master_sku_desc_map.get(str(s).strip(), "Description N/A")} for s in acc.carried_top_products if str(s).strip() ]
            described_missing_list = []
            if acc.missing_top_products: described_missing_list = [ {"sku": str(s).strip(), "description": master_sku_desc_map.get(str(s).strip(), "Description N/A")} for s in acc.missing_top_products if str(s).strip() ]
            output_list.append({ "id": acc.id, "card_code": acc.canonical_code, "name": acc.name, "distributor": acc.distributor, "full_address": acc.full_address, "recency_score": acc.recency_score, "frequency_score": acc.frequency_score, "account_total": acc.account_total, "health_score": acc.health_score, "health_category": acc.health_category, "rfm_segment": acc.rfm_segment, "enhanced_priority_score": acc.enhanced_priority_score, "yoy_revenue_growth": acc.yoy_revenue_growth, "days_overdue": acc.days_overdue, "last_purchase_date": acc.last_purchase_date.isoformat() if acc.last_purchase_date else None, "next_expected_purchase_date": acc.next_expected_purchase_date.isoformat() if acc.next_expected_purchase_date else None, "product_coverage_percentage": acc.product_coverage_percentage, "carried_top_products": described_carried_list, "missing_top_products": described_missing_list,  "sales_rep": acc.sales_rep, "sales_rep_name": acc.sales_rep_name, "py_total_revenue": getattr(acc, 'py_total_revenue', None),  "yep_revenue": acc.yep_revenue, "pace_vs_ly": acc.pace_vs_ly })
        return jsonify({ "accounts": output_list, "summary_stats": { "average_coverage": avg_coverage, "coverage_distribution": distribution, "overdue_count": overdue_count, "total_accounts": len(accounts), "total_yep": total_yep_for_summary, "avg_priority_score": avg_priority_score_summary, "avg_health_score": avg_health_score_summary, "count_priority1": sum(1 for acc_s in accounts if acc_s.enhanced_priority_score is not None and acc_s.enhanced_priority_score >= 75), "count_priority2": sum(1 for acc_s in accounts if acc_s.enhanced_priority_score is not None and acc_s.enhanced_priority_score >= 50 and acc_s.enhanced_priority_score < 75), "count_due_this_week": sum(1 for acc_s in accounts if acc_s.next_expected_purchase_date and (acc_s.next_expected_purchase_date.date() - datetime.utcnow().date()).days <= 7 and (acc_s.next_expected_purchase_date.date() - datetime.utcnow().date()).days >= 0), "count_overdue": overdue_count,  "count_low_health": sum(1 for acc_s in accounts if acc_s.health_score is not None and acc_s.health_score < HEALTH_POOR_THRESHOLD), "count_low_pace": sum(1 for acc_s in accounts if acc_s.pace_vs_ly is not None and acc_s.py_total_revenue is not None and acc_s.py_total_revenue > 0 and (acc_s.pace_vs_ly / acc_s.py_total_revenue * 100) < PRIORITY_PACE_DECLINE_PCT_THRESHOLD), "count_high_pace": sum(1 for acc_s in accounts if acc_s.pace_vs_ly is not None and acc_s.py_total_revenue is not None and acc_s.py_total_revenue > 0 and (acc_s.pace_vs_ly / acc_s.py_total_revenue * 100) > GROWTH_PACE_INCREASE_PCT_THRESHOLD), "count_growth_opps": sum(1 for acc_s in accounts if is_growth_opportunity_api(acc_s)),  } })
    except Exception as e: logger.error(f"Error fetching strategic accounts data: {str(e)}", exc_info=True); return jsonify({"error": "An internal server error occurred."}), 500

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
    logger.info(f"Fetching details for account: {canonical_code} (with yearly product summary and Top 30 flag)")
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

        # --- Collect SKUs for Description Fetching (as before) ---
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

        # --- Historical Summary & Analysis Data (no changes) ---
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
        
        revenue_history_data_dict = {"years": [str(h['year']) for h in historical_summary_list], "revenues": [h['revenue'] for h in historical_summary_list]}
        current_year = datetime.utcnow().year; previous_year = current_year - 1
        
        def get_daily_timeline(target_year):
            # ... (no change in this inner function) ...
            daily_stmt = select( func.date(Transaction.posting_date).label('purchase_date'), func.sum(Transaction.revenue).label('daily_revenue') ).where( Transaction.canonical_code == canonical_code, Transaction.year == target_year ).group_by( func.date(Transaction.posting_date) ).order_by( func.date(Transaction.posting_date) )
            daily_results_rowmappings = db.session.execute(daily_stmt).mappings().all()
            timeline_data = []
            for r_map in daily_results_rowmappings:
                purchase_date_val = r_map.get('purchase_date'); x_val = None
                if purchase_date_val:
                    if hasattr(purchase_date_val, 'isoformat'): x_val = purchase_date_val.isoformat()
                    elif isinstance(purchase_date_val, str): x_val = purchase_date_val 
                timeline_data.append({ "x": x_val, "daily_revenue": float(r_map.get('daily_revenue') or 0.0) })
            return timeline_data
        cy_timeline_data_list = get_daily_timeline(current_year)
        py_timeline_data_list = get_daily_timeline(previous_year)
        
        detailed_product_history = get_detailed_product_history_by_quarter( monthly_aggregate_results, master_sku_desc_map, TOP_30_SET, canonical_code )

        # +++ MODIFICATION: Add is_top_30 flag here +++
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

        # --- Final JSON Response ---
        return jsonify({
            "prediction": prediction_data_dict,
            "historical_summary": historical_summary_list, 
            "analysis": analysis_data_dict,
            "growth_engine": growth_engine_data,
            "chart_data": {
                "revenue_history": revenue_history_data_dict,
                "cy_purchase_timeline": cy_timeline_data_list,
                "py_purchase_timeline": py_timeline_data_list,
                "detailed_product_history_by_quarter": detailed_product_history,
                "yearly_product_summary_table_data": yearly_product_summary_final
            },
            "rolling_sku_analysis": rolling_sku_analysis_list
        })

    except Exception as e:
        logger.error(f"Error fetching account details for {canonical_code}: {str(e)}", exc_info=True)
        return jsonify({"error": "An internal server error occurred."}), 500