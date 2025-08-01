# routes/api_routes_strategic.py

from flask import Blueprint, jsonify, request
from sqlalchemy import func, desc, or_, select, extract 
from models import db, AccountPrediction, Transaction, AccountHistoricalRevenue 
from config import TOP_30_SET 
from datetime import datetime, date
import logging
import json
from collections import defaultdict, OrderedDict

logger = logging.getLogger(__name__) 

api_strategic_bp = Blueprint('api_strategic', __name__, url_prefix='/api/strategic')


def _clean_distributor(raw: str | None) -> str | None:
    """Normalise distributor names once so the UI gets stable keys."""
    if not raw:
        return None
    return raw.strip().upper()


def get_sku_description_map(sku_list: list) -> dict:
    """Get SKU descriptions from the most recent transaction for each SKU."""
    if not sku_list:
        return {}
    
    valid_skus_for_query = list(set(str(s).strip() for s in sku_list if s and str(s).strip()))
    if not valid_skus_for_query:
        return {str(s).strip(): "Description N/A" for s in sku_list if s and str(s).strip()}

    fetched_sku_to_desc = {}
    try:
        subq = select(
            Transaction.item_code,
            Transaction.description,
            func.row_number().over(
                partition_by=Transaction.item_code,
                order_by=Transaction.posting_date.desc()
            ).label('rn')
        ).where(
            Transaction.item_code.in_(valid_skus_for_query),
            Transaction.description.isnot(None),
            Transaction.description != ''
        ).subquery()
        
        stmt = select(subq.c.item_code, subq.c.description).where(subq.c.rn == 1)
        results = db.session.execute(stmt).fetchall()
        for sku, description in results:
            fetched_sku_to_desc[str(sku).strip()] = description
    except Exception as e:
        logger.error(f"Error fetching SKU descriptions map: {e}", exc_info=True)
        return {sku: "Description N/A" for sku in valid_skus_for_query}

    return {
        clean_sku: fetched_sku_to_desc.get(clean_sku, "Description N/A")
        for clean_sku in valid_skus_for_query
    }


def get_detailed_product_history_by_quarter(
    monthly_aggregate_results: list, 
    master_sku_desc_map: dict, 
    top_30_skus_set: set, 
    canonical_code_for_logging: str
):
    """Process monthly aggregates into quarterly product history with detailed metrics."""
    logger.info(f"Processing detailed product history for {canonical_code_for_logging}")
    
    quarterly_sku_aggregates = defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: {
        "total_quantity": 0, "total_revenue": 0.0
    })))
    
    for row in monthly_aggregate_results:
        year_str = str(row.year)
        try:
            month_int = int(row.month_val) 
            if 1 <= month_int <= 3: 
                quarter_key = "Q1"
            elif 4 <= month_int <= 6: 
                quarter_key = "Q2"
            elif 7 <= month_int <= 9: 
                quarter_key = "Q3"
            elif 10 <= month_int <= 12: 
                quarter_key = "Q4"
            else: 
                continue
        except (ValueError, TypeError): 
            continue
            
        sku = str(row.item_code)
        agg_data = quarterly_sku_aggregates[year_str][quarter_key][sku]
        agg_data["total_quantity"] += int(row.sum_quantity or 0)
        agg_data["total_revenue"] += float(row.sum_revenue or 0.0)
    
    final_response_structure = OrderedDict()
    sorted_years = sorted(quarterly_sku_aggregates.keys(), key=int)
    data_from_absolute_previous_quarter_for_qoq = {}
    
    for year_str in sorted_years:
        final_response_structure[year_str] = OrderedDict()
        year_data_for_quarters = quarterly_sku_aggregates[year_str]
        
        for q_key in ["Q1", "Q2", "Q3", "Q4"]:
            current_quarter_sku_details_map = year_data_for_quarters.get(q_key, {})
            current_quarter_product_list_frontend = []
            current_quarter_skus_set_for_metrics = set(current_quarter_sku_details_map.keys())
            qty_top_30_this_qtr, rev_total_this_qtr, carried_top_30_details_this_qtr = 0, 0.0, []
            
            for sku, details in current_quarter_sku_details_map.items():
                is_top_30_val = sku in top_30_skus_set
                current_qty, current_rev = details["total_quantity"], details["total_revenue"]
                status_in_qtr_vs_prev_q = "Newly Added this Qtr (vs Prev Qtr)" if sku not in data_from_absolute_previous_quarter_for_qoq else "Repurchased"
                qoq_qty_pct_change, qoq_rev_pct_change = None, None
                
                prev_q_sku_data = data_from_absolute_previous_quarter_for_qoq.get(sku)
                if prev_q_sku_data:
                    prev_qty, prev_rev = prev_q_sku_data["total_quantity"], prev_q_sku_data["total_revenue"]
                    if prev_qty > 0: 
                        qoq_qty_pct_change = round(((current_qty - prev_qty) / prev_qty) * 100, 1)
                    if prev_rev > 0: 
                        qoq_rev_pct_change = round(((current_rev - prev_rev) / prev_rev) * 100, 1)
                
                current_quarter_product_list_frontend.append({
                    "sku": sku, 
                    "description": master_sku_desc_map.get(sku, "Description N/A"), 
                    "quantity": current_qty, 
                    "revenue": round(current_rev, 2), 
                    "is_top_30": is_top_30_val, 
                    "status_in_qtr": status_in_qtr_vs_prev_q, 
                    "qoq_qty_pct_change": qoq_qty_pct_change, 
                    "qoq_rev_pct_change": qoq_rev_pct_change
                })
                
                rev_total_this_qtr += current_rev
                if is_top_30_val:
                    qty_top_30_this_qtr += current_qty
                    carried_top_30_details_this_qtr.append({
                        "sku": sku, 
                        "description": master_sku_desc_map.get(sku, "Description N/A"), 
                        "quantity": current_qty, 
                        "revenue": round(current_rev, 2)
                    })
            
            current_quarter_product_list_frontend.sort(key=lambda x: x["revenue"], reverse=True)
            previous_quarter_skus_set = set(data_from_absolute_previous_quarter_for_qoq.keys())
            
            # Added SKUs with proper data structure
            added_skus_details = []
            for s in (current_quarter_skus_set_for_metrics - previous_quarter_skus_set):
                sku_data = current_quarter_sku_details_map.get(s, {})
                added_skus_details.append({
                    "sku": s, 
                    "description": master_sku_desc_map.get(s, "Description N/A"), 
                    "quantity": sku_data.get("total_quantity", 0),
                    "revenue": round(sku_data.get("total_revenue", 0.0), 2)
                })
            
            # Dropped SKUs
            dropped_skus_details = [
                {"sku": s, "description": master_sku_desc_map.get(s, "Description N/A")} 
                for s in (previous_quarter_skus_set - current_quarter_skus_set_for_metrics)
            ]
            
            # Repurchased SKUs
            repurchased_skus_set = current_quarter_skus_set_for_metrics.intersection(previous_quarter_skus_set)
            repurchased_skus_details = []
            for s in repurchased_skus_set:
                sku_data = current_quarter_sku_details_map.get(s, {})
                repurchased_skus_details.append({
                    "sku": s, 
                    "description": master_sku_desc_map.get(s, "Description N/A"), 
                    "quantity": sku_data.get("total_quantity", 0),
                    "revenue": round(sku_data.get("total_revenue", 0.0), 2)
                })
            
            final_response_structure[year_str][q_key] = {
                "products": current_quarter_product_list_frontend, 
                "metrics": {
                    "total_items_in_quarter": len(current_quarter_product_list_frontend), 
                    "total_revenue_in_quarter": round(rev_total_this_qtr, 2), 
                    "items_added_details": sorted(added_skus_details, key=lambda x: x.get("revenue", 0), reverse=True), 
                    "items_dropped_details": sorted(dropped_skus_details, key=lambda x: x["description"]), 
                    "items_repurchased_count": len(repurchased_skus_set), 
                    "quantity_repurchased": sum(d.get("quantity", 0) for d in repurchased_skus_details), 
                    "repurchased_skus_details": sorted(repurchased_skus_details, key=lambda x: x.get("revenue", 0), reverse=True), 
                    "top_30_skus_carried_details": sorted(carried_top_30_details_this_qtr, key=lambda x: x["revenue"], reverse=True), 
                    "count_top_30_skus_carried": len(carried_top_30_details_this_qtr), 
                    "quantity_top_30_carried": qty_top_30_this_qtr
                }
            }
            
            data_from_absolute_previous_quarter_for_qoq = current_quarter_sku_details_map.copy() if current_quarter_sku_details_map else {}
    
    return final_response_structure


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
            # Normalize incoming distributor parameter to match stored format
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
        
        # Collect all SKUs needed for descriptions - BUG FIX HERE
        all_skus_needed = set()
        for acc in accounts:
            all_skus_needed.update(str(s).strip() for s in (acc.carried_top_products or []) if str(s).strip())
            # FIXED: Handle missing_top_products as list of dictionaries
            for item in (acc.missing_top_products or []):
                if isinstance(item, dict) and item.get('sku'):
                    all_skus_needed.add(str(item['sku']).strip())

        master_sku_desc_map = get_sku_description_map(list(all_skus_needed))
        
        # Calculate comprehensive summary statistics
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
        
        # Additional summary calculations
        active_enhanced_priority_sum = sum(acc.enhanced_priority_score for acc in accounts if acc.enhanced_priority_score is not None)
        active_enhanced_priority_count = sum(1 for acc in accounts if acc.enhanced_priority_score is not None)
        avg_priority_score_summary = round(active_enhanced_priority_sum / active_enhanced_priority_count, 1) if active_enhanced_priority_count > 0 else None
        
        health_scores = [acc.health_score for acc in accounts if acc.health_score is not None]
        avg_health_score_summary = round(sum(health_scores) / len(health_scores), 1) if health_scores else None
        
        # Build output list
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
            
            # Handle date fields
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


def is_growth_opportunity_api(account_prediction_obj):
    """Determine if an account represents a growth opportunity."""
    from config import GROWTH_HEALTH_THRESHOLD, GROWTH_PACE_INCREASE_PCT_THRESHOLD, GROWTH_MISSING_PRODUCTS_THRESHOLD
    
    acc = account_prediction_obj 
    if (acc.health_score or 0) < GROWTH_HEALTH_THRESHOLD: 
        return False
        
    if acc.pace_vs_ly is not None and acc.py_total_revenue is not None and acc.py_total_revenue > 0 and ((acc.pace_vs_ly / acc.py_total_revenue) * 100) >= GROWTH_PACE_INCREASE_PCT_THRESHOLD: 
        return True
        
    if isinstance(acc.missing_top_products, list) and len(acc.missing_top_products) >= GROWTH_MISSING_PRODUCTS_THRESHOLD: 
        return True
        
    if acc.rfm_segment in ["Champions", "Loyal Customers"] and acc.next_expected_purchase_date and (0 <= (acc.next_expected_purchase_date.date() - datetime.utcnow().date()).days <= 14): 
        return True
        
    return False


@api_strategic_bp.route("/accounts/<path:canonical_code>/details", methods=["GET"])
def get_account_details(canonical_code: str):
    """Get detailed account information including predictions, history, and analysis."""
    logger.info(f"Fetching details for account {canonical_code} with new distributor timeline logic")
    if not canonical_code:
        return jsonify({"error": "Canonical code is required."}), 400

    try:
        prediction = db.session.scalar(select(AccountPrediction).where(AccountPrediction.canonical_code == canonical_code))
        if not prediction:
            return jsonify({"error": f"Account with canonical code '{canonical_code}' not found."}), 404

        # 1. Gather all SKUs needed for descriptions from all data sources first
        all_skus_for_descriptions = set()
        all_skus_for_descriptions.update(str(s).strip() for s in (prediction.carried_top_products or []))
        
        # Handle missing_top_products as list of dictionaries
        for item in (prediction.missing_top_products or []):
            if isinstance(item, dict) and item.get('sku'): 
                all_skus_for_descriptions.add(str(item['sku']).strip())
                
        if prediction.products_purchased:
            try: 
                all_skus_for_descriptions.update(str(s).strip() for s in json.loads(prediction.products_purchased))
            except json.JSONDecodeError: 
                pass
        
        if prediction.recommended_products_next_purchase_json:
            try:
                recs_list_raw = json.loads(prediction.recommended_products_next_purchase_json)
                if isinstance(recs_list_raw, list):
                    for item_raw in recs_list_raw:
                        if isinstance(item_raw, dict) and item_raw.get("sku"):
                            all_skus_for_descriptions.add(str(item_raw.get("sku")).strip())
            except json.JSONDecodeError:
                pass
        
        # Get monthly aggregates for quarterly analysis and yearly summary
        monthly_aggregate_results = db.session.execute(select(
            Transaction.year, 
            extract('month', Transaction.posting_date).label('month_val'),
            Transaction.item_code, 
            func.sum(Transaction.quantity).label('sum_quantity'),
            func.sum(Transaction.revenue).label('sum_revenue')
        ).where(
            Transaction.canonical_code == canonical_code, 
            Transaction.item_code.isnot(None), 
            Transaction.item_code != ''
        ).group_by(
            Transaction.year, 
            extract('month', Transaction.posting_date), 
            Transaction.item_code
        ).order_by(
            Transaction.year.asc(), 
            extract('month', Transaction.posting_date).asc()
        )).fetchall()
        
        for row in monthly_aggregate_results:
            if row.item_code: 
                all_skus_for_descriptions.add(str(row.item_code).strip())

        # 2. Get the master description map once
        master_sku_desc_map = get_sku_description_map(list(all_skus_for_descriptions))

        # 3. Build all data structures using the master map
        prediction_data_dict = {col.name: getattr(prediction, col.name) for col in prediction.__table__.columns}
        
        # Handle date fields
        for field in ['last_purchase_date', 'next_expected_purchase_date', 'rep_last_order_date', 'reminder_sent_at', 'notified_last_purchase_date']:
            if prediction_data_dict.get(field): 
                prediction_data_dict[field] = prediction_data_dict[field].isoformat()
        
        # Add descriptions to carried top products
        prediction_data_dict['carried_top_products'] = [
            {"sku": s, "description": master_sku_desc_map.get(s, "Description N/A")} 
            for s in (prediction.carried_top_products or [])
        ]
        
        # Handle missing_top_products with descriptions
        prediction_data_dict['missing_top_products'] = [
            {
                "sku": item['sku'], 
                "description": master_sku_desc_map.get(item['sku'], "Description N/A"), 
                "reason": item.get('placeholder_insight', 'Missing')
            } 
            for item in (prediction.missing_top_products or []) 
            if isinstance(item, dict) and item.get('sku')
        ]
        
        # Add descriptions to products purchased
        described_products_purchased = []
        if prediction.products_purchased:
            try:
                parsed_list = json.loads(prediction.products_purchased)
                if isinstance(parsed_list, list):
                    described_products_purchased = [
                        {"sku": str(s).strip(), "description": master_sku_desc_map.get(str(s).strip(), "Description N/A")} 
                        for s in parsed_list if str(s).strip()
                    ]
            except json.JSONDecodeError:
                pass
        prediction_data_dict['products_purchased'] = described_products_purchased

        # Historical summary
        hist_summary_list = [
            dict(r) for r in db.session.execute(select(
                AccountHistoricalRevenue.year, 
                AccountHistoricalRevenue.total_revenue.label('revenue')
            ).where(
                AccountHistoricalRevenue.canonical_code == canonical_code
            ).order_by(AccountHistoricalRevenue.year.asc())).mappings().all()
        ]
        
        current_year, previous_year = datetime.utcnow().year, datetime.utcnow().year - 1

        def get_daily_timeline(target_year: int) -> list[dict]:
            """Returns daily timeline with distributor information."""
            daily_stmt = select(
                func.date(Transaction.posting_date).label("purchase_date"),
                Transaction.distributor,
                func.sum(Transaction.revenue).label("daily_revenue"),
            ).where(
                Transaction.canonical_code == canonical_code, 
                extract('year', Transaction.posting_date) == target_year,
                Transaction.distributor.isnot(None), 
                Transaction.distributor != "",
            ).group_by(
                func.date(Transaction.posting_date), 
                Transaction.distributor
            ).order_by(func.date(Transaction.posting_date))
            
            return [{
                "x": r["purchase_date"].isoformat() if r["purchase_date"] else None,
                "daily_revenue": float(r["daily_revenue"] or 0.0),
                "distributor": _clean_distributor(r["distributor"]) or "UNKNOWN"
            } for r in db.session.execute(daily_stmt).mappings().all()]

        cy_timeline = get_daily_timeline(current_year)
        py_timeline = get_daily_timeline(previous_year)

        # Detailed product history by quarter
        detailed_product_history = get_detailed_product_history_by_quarter(
            monthly_aggregate_results, 
            master_sku_desc_map, 
            TOP_30_SET, 
            canonical_code
        )

        # Yearly product summary - FIX: Properly aggregate by year AND sku
        yearly_aggregates = defaultdict(lambda: defaultdict(lambda: {"qty": 0, "rev": 0.0}))
        
        for row in monthly_aggregate_results:
            year_str = str(row.year)
            sku = str(row.item_code).strip()
            yearly_aggregates[year_str][sku]["qty"] += int(row.sum_quantity or 0)
            yearly_aggregates[year_str][sku]["rev"] += float(row.sum_revenue or 0.0)

        yearly_product_summary_final = OrderedDict()
        for year_str, sku_dict in sorted(yearly_aggregates.items(), key=lambda t: int(t[0])):
            yearly_product_summary_final[year_str] = sorted([
                {
                    "sku": sku, 
                    "description": master_sku_desc_map.get(sku, "Description N/A"),
                    "total_quantity_year": data["qty"], 
                    "total_revenue_year": round(data["rev"], 2),
                    "is_top_30": sku in TOP_30_SET
                } for sku, data in sku_dict.items()
            ], key=lambda x: x["total_revenue_year"], reverse=True)

        # Rolling SKU Analysis - Enhanced error logging
        rolling_sku_analysis_list = []
        if prediction.rolling_sku_analysis_json:
            try:
                safe_json = prediction.rolling_sku_analysis_json.replace(': NaN', ': null')
                rolling_sku_analysis_list = json.loads(safe_json)
            except json.JSONDecodeError as e:
                logger.warning(f"Could not decode rolling_sku_analysis_json for {canonical_code}: {e}")
                logger.warning(f"Problematic JSON substring: {prediction.rolling_sku_analysis_json[:200]}...")

        # Growth engine data
        described_recommended_products_for_growth_engine = []
        if prediction.recommended_products_next_purchase_json:
            try:
                recs_list_raw = json.loads(prediction.recommended_products_next_purchase_json)
                if isinstance(recs_list_raw, list):
                    for item_raw in recs_list_raw:
                        if isinstance(item_raw, dict) and item_raw.get("sku"):
                            sku_val = str(item_raw.get("sku")).strip()
                            described_recommended_products_for_growth_engine.append({
                                "sku": sku_val,
                                "description": master_sku_desc_map.get(sku_val, "Description N/A"),
                                "reason": item_raw.get("reason", "Recommended")
                            })
            except json.JSONDecodeError:
                pass

        return jsonify({
            "prediction": prediction_data_dict, 
            "historical_summary": hist_summary_list,
            "analysis": {
                "revenue_trend": {
                    "slope": prediction.revenue_trend_slope, 
                    "r_squared": prediction.revenue_trend_r_squared,
                    "intercept": prediction.revenue_trend_intercept,
                    "model_type": "Linear Regression" if prediction.revenue_trend_slope is not None else "N/A"
                }
            },
            "growth_engine": {
                "target_yep_plus_1_pct": prediction.target_yep_plus_1_pct,
                "additional_revenue_needed_eoy": prediction.additional_revenue_needed_eoy,
                "suggested_next_purchase_amount": prediction.suggested_next_purchase_amount,
                "recommended_products": described_recommended_products_for_growth_engine,
                "message": prediction.growth_engine_message,
                "already_on_track": (prediction.additional_revenue_needed_eoy or 0) <= 0
            },
            "chart_data": {
                "revenue_history": {
                    "years": [str(h['year']) for h in hist_summary_list], 
                    "revenues": [h['revenue'] for h in hist_summary_list]
                },
                "cy_purchase_timeline": cy_timeline, 
                "py_purchase_timeline": py_timeline,
                "detailed_product_history_by_quarter": detailed_product_history,
                "yearly_product_summary_table_data": yearly_product_summary_final,
            },
            "rolling_sku_analysis": rolling_sku_analysis_list
        })
        
    except Exception:
        logger.exception(f"Error fetching account details for {canonical_code}")
        return jsonify({"error": "Internal server error"}), 500