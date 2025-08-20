import os
import io
import hashlib
import hmac
import time
import threading
from functools import wraps
from flask import Blueprint, request, jsonify, current_app, abort, Flask
import logging
import pandas as pd
import numpy as np
from werkzeug.utils import secure_filename
from datetime import datetime, timedelta
import json 
import re
from sqlalchemy import select 
# This is the specific SQLAlchemy command for PostgreSQL's "UPSERT" feature
from sqlalchemy.dialects.postgresql import insert as pg_insert


try:
    from pipeline import (recalculate_predictions_and_metrics, clean_data, 
                          aggregate_item_codes, safe_json_dumps,
                          generate_canonical_code, get_base_card_code, _normalize_upc)
except ImportError as e:
    logging.error(f"CRITICAL: Could not import required pipeline/mapper functions: {e}", exc_info=True)
    def recalculate_predictions_and_metrics(*args, **kwargs): logging.error("Fallback: recalc not imported!"); return pd.DataFrame()
    def clean_data(df, *args, **kwargs): logging.error("Fallback: clean_data not imported!"); return df
    def aggregate_item_codes(series, *args, **kwargs): logging.warning("Fallback: aggregate_item_codes not imported!"); return []
    def safe_json_dumps(data, *args, **kwargs): logging.warning("Fallback: safe_json_dumps not imported!"); return None
    def generate_canonical_code(*args, **kwargs): logging.error("Fallback: generate_canonical_code not imported!"); return None
    def get_base_card_code(*args, **kwargs): logging.error("Fallback: get_base_card_code not imported!"); return None

from models import db, AccountPrediction, AccountHistoricalRevenue, Transaction 

logger = logging.getLogger(__name__)
webhook_bp = Blueprint('webhook', __name__, url_prefix='/webhook')

def safe_float(value, default=0.0):
    try:
        if pd.isna(value): return default
        return float(value)
    except (ValueError, TypeError): return default

def safe_int(value, default=0):
    try:
        if pd.isna(value): return default
        i = int(float(value))
        return i
    except (ValueError, TypeError): return default

def require_hmac_signature(f):
    """
    Verifies HMAC for both multipart (file upload) and JSON bodies, matching the
    behavior of your existing distributor_upload_secure.py client **without** any
    client changes.

    Supported client pattern (multipart/form-data):
      signature over: "{timestamp}.{sha256(file_bytes)}"
      headers:        X-Request-Timestamp, X-Signature
    (Also tolerated: X-Timestamp, Signature query param "sig" and "ts")

    Supported client patterns (JSON / non-file bodies):
      A) "{timestamp}." + raw_body_bytes
      B) "{timestamp}.{sha256(raw_body_bytes)}"
    """
    @wraps(f)
    def _wrap(*args, **kwargs):
        # 1) Pull secret from config or env; if missing, reject to be safe in prod
        secret = (current_app.config.get('WEBHOOK_HMAC_SECRET')
                  or os.getenv('HMAC_SECRET_KEY'))
        if not secret:
            abort(500, description="HMAC secret not configured on server.")

        # 2) Collect signature + timestamp from headers or query
        sig = (request.headers.get('X-Signature')
               or request.headers.get('Signature')
               or request.args.get('sig')
               or '')
        ts  = (request.headers.get('X-Request-Timestamp')
               or request.headers.get('X-Timestamp')
               or request.args.get('ts'))
        if not sig or not ts:
            abort(400, description="Missing signature headers.")

        # 3) Freshness check (default 5 minutes, configurable)
        try:
            ts_int = int(ts)
        except ValueError:
            abort(400, description="Invalid timestamp format.")
        now = int(time.time())
        ttl = int(current_app.config.get('WEBHOOK_HMAC_TTL_SECONDS', 300))
        if abs(now - ts_int) > ttl:
            abort(401, description="Request timestamp is too old or invalid.")

        secret_bytes = secret.encode('utf-8')

        # 4) MULTIPART path (file upload) — matches distributor_upload_secure.py
        uploaded = request.files.get('file')
        if uploaded is not None:
            try:
                # Read the file stream, hash it, then rewind so later code can save()
                blob = uploaded.stream.read()
                file_hash_hex = hashlib.sha256(blob).hexdigest()
                try:
                    uploaded.stream.seek(0)
                except Exception:
                    uploaded.stream = io.BytesIO(blob)
            except Exception:
                abort(400, description="Unable to read uploaded file for signature verification.")

            # Primary: "{ts}.{sha256(file_bytes)}"
            msg1 = f"{ts}.{file_hash_hex}".encode('utf-8')
            c1 = hmac.new(secret_bytes, msg1, hashlib.sha256).hexdigest()

            # Lenient fallback (older schemes some clients use):
            #   a) "{ts}." + raw bytes (rare, but harmless to support)
            #   b) "{ts}.{filename}"
            msg2 = ts.encode('utf-8') + b"." + blob
            c2 = hmac.new(secret_bytes, msg2, hashlib.sha256).hexdigest()

            fname = uploaded.filename or ''
            msg3 = f"{ts}.{fname}".encode('utf-8')
            c3 = hmac.new(secret_bytes, msg3, hashlib.sha256).hexdigest()

            if hmac.compare_digest(sig, c1) or hmac.compare_digest(sig, c2) or hmac.compare_digest(sig, c3):
                return f(*args, **kwargs)

            abort(401, description="Invalid signature.")

        # 5) JSON / non-file path — accept two common patterns
        body = request.get_data(cache=True) or b''

        # A) "{ts}." + raw body
        msgA = ts.encode('utf-8') + b"." + body
        cA = hmac.new(secret_bytes, msgA, hashlib.sha256).hexdigest()

        # B) "{ts}.{sha256(body)}"
        body_hash_hex = hashlib.sha256(body).hexdigest()
        msgB = f"{ts}.{body_hash_hex}".encode('utf-8')
        cB = hmac.new(secret_bytes, msgB, hashlib.sha256).hexdigest()

        if hmac.compare_digest(sig, cA) or hmac.compare_digest(sig, cB):
            return f(*args, **kwargs)

        abort(401, description="Invalid signature.")
    return _wrap

from sqlalchemy import func, extract

def process_file_async(app_instance_config, filepath):
    thread_id = threading.get_ident()
    logger.info(f"[Thread:{thread_id}] Starting V9 (Final Corrected) processing for file: {filepath}")
    
    # Create a new app instance for the thread to have its own context
    thread_app = Flask(__name__)
    thread_app.config.update(app_instance_config)
    db.init_app(thread_app)

    with thread_app.app_context():
        # Use a single session and transaction for the entire operation
        session = db.session
        try:
            # --- Stage 1: Load, Clean, and Standardize Data ---
            logger.info(f"[Thread:{thread_id}] Loading and cleaning data from file...")
            try:
                weekly_df = pd.read_csv(
                    filepath,
                    dtype=str,
                    converters={'ITEMUPC': _normalize_upc},  # <-- normalise UPC as it's read (edit 4)
                    encoding='utf-8',
                    on_bad_lines='warn',
                    low_memory=False,
                )
            except UnicodeDecodeError:
                # keep the same dtype and converters for Latin-1 fallback
                weekly_df = pd.read_csv(
                    filepath,
                    dtype=str,
                    converters={'ITEMUPC': _normalize_upc},  # (edit 4)
                    encoding='latin-1',
                    on_bad_lines='warn',
                    low_memory=False,
                )

            cleaned_weekly_df = clean_data(weekly_df.copy())

            # Standardize incoming column names.  We explicitly map UPC to the
            # canonical item_code and keep the raw distributor code in
            # distributor_item_code for auditing.  If a column isn't present
            # it will remain unchanged.
            column_rename_map = {
                'POSTINGDATE': 'posting_date',
                'ITEMUPC': 'item_code',              # use the UPC as the canonical product identifier
                'ITEM': 'distributor_item_code',     # keep distributor item code for reference
                'QUANTITY': 'quantity',
                'AMOUNT': 'amount',
                'NAME': 'name',
                'DESCRIPTION': 'description',
                'SalesRep': 'sales_rep',
                'SlpName': 'sales_rep_name',
                'Distributor': 'distributor',
                'CUSTOMERID': 'customer_id',
                'ADDRESS': 'address',
                'CITY': 'city',
                'STATE': 'state',
                'ZIPCODE': 'zipcode'
            }
            cleaned_weekly_df.rename(columns=lambda c: column_rename_map.get(c, c), inplace=True)
            # Robust Date Parsing: normalize posting_date to a proper date (edit 4)
            def _parse_date(val):
                if pd.isna(val) or val is None or str(val).strip() == '':
                    return None
                s = str(val).strip()
                # Try common formats first, then fall back to pandas parser
                for fmt in (
                    '%Y-%m-%d', '%m/%d/%Y', '%Y/%m/%d', '%m-%d-%Y', '%d-%b-%Y', '%Y.%m.%d'
                ):
                    try:
                        return datetime.strptime(s, fmt).date()
                    except Exception:
                        continue
                try:
                    dt = pd.to_datetime(s, errors='coerce')
                    return None if pd.isna(dt) else dt.date()
                except Exception:
                    return None

            if 'posting_date' in cleaned_weekly_df.columns:
                cleaned_weekly_df['posting_date'] = cleaned_weekly_df['posting_date'].apply(_parse_date)

            if 'item_code' in cleaned_weekly_df.columns:
                cleaned_weekly_df['item_code'] = (
                    cleaned_weekly_df['item_code']
                    .fillna('')
                    .astype(str)
                    .str.strip()
                    .apply(_normalize_upc)   # normalize to pure digits, no ".0" (edit 4)
                )

            if 'distributor_item_code' in cleaned_weekly_df.columns:
                cleaned_weekly_df['distributor_item_code'] = (
                    cleaned_weekly_df['distributor_item_code']
                    .fillna('')
                    .astype(str)
                    .str.strip()
                )


            # Prefer the CardName column for the store's display name when it is
            # present and non-empty.  For some distributors (e.g. KEHE/UNFI)
            # the NAME column may contain only internal codes.  We preserve
            # whatever was in NAME, but override with CardName when available.
            if 'CardName' in cleaned_weekly_df.columns:
                cleaned_weekly_df['CardName'] = cleaned_weekly_df['CardName'].fillna('').astype(str).str.strip()
                # ensure name exists even if original column missing
                if 'name' not in cleaned_weekly_df.columns:
                    cleaned_weekly_df['name'] = ''
                cleaned_weekly_df['name'] = cleaned_weekly_df['name'].fillna('').astype(str).str.strip()
                mask = cleaned_weekly_df['CardName'] != ''
                cleaned_weekly_df.loc[mask, 'name'] = cleaned_weekly_df.loc[mask, 'CardName']

            # If there is no revenue column yet, derive it from the amount.  We
            # coerce to numeric and fill NaNs with zero.  This must run before
            # computing transaction hashes or aggregations.
            if 'revenue' not in cleaned_weekly_df.columns:
                cleaned_weekly_df['revenue'] = pd.to_numeric(cleaned_weekly_df.get('amount'), errors='coerce').fillna(0)
            # -------------------------------------------------------------------

            # Generate canonical product code (stable across sources)
            # Use item_code (UPC) where present; fall back to description if needed.
            def _canon(row):
                upc = (row.get('item_code') or '').strip()
                desc = (row.get('description') or '').strip()
                canon = generate_canonical_code(upc, desc)
                return canon or upc or desc or None

            cleaned_weekly_df['canonical_code'] = cleaned_weekly_df.apply(_canon, axis=1)

            # --- Stage 2: Generate Hashes and Insert Transactions ---
            logger.info(f"[Thread:{thread_id}] Calculating deterministic hashes for incoming transactions...")
            duplicate_check_cols = ['canonical_code', 'posting_date', 'item_code', 'revenue', 'quantity']
            for col in duplicate_check_cols:
                if col not in cleaned_weekly_df.columns: cleaned_weekly_df[col] = None

            cleaned_weekly_df.sort_values(by=duplicate_check_cols, inplace=True, na_position='first')
            cleaned_weekly_df['duplicate_rank'] = cleaned_weekly_df.groupby(duplicate_check_cols).cumcount()

            def generate_hash(row):
                unique_string = (f"{row.get('canonical_code', '')}|{row.get('posting_date', '')}|"
                                 f"{row.get('item_code', '')}|{row.get('revenue', '')}|{row.get('quantity', '')}|"
                                 f"{row.get('duplicate_rank', '')}")
                return hashlib.sha256(unique_string.encode()).hexdigest()

            cleaned_weekly_df['transaction_hash'] = cleaned_weekly_df.apply(generate_hash, axis=1)

            # Prepare for insert
            transactions_to_insert = cleaned_weekly_df[[
                'transaction_hash', 'canonical_code', 'item_code', 'posting_date', 'revenue', 'quantity',
                'description', 'customer_id', 'distributor_item_code'
            ]].replace({pd.NaT: None, np.nan: None}).to_dict(orient='records')

            if transactions_to_insert:
                stmt = pg_insert(Transaction.__table__).values(transactions_to_insert)
                update_cols = {
                    'canonical_code': stmt.excluded.canonical_code,
                    'item_code': stmt.excluded.item_code,
                    'posting_date': stmt.excluded.posting_date,
                    'revenue': stmt.excluded.revenue,
                    'quantity': stmt.excluded.quantity,
                    'description': stmt.excluded.description,
                    'customer_id': stmt.excluded.customer_id,
                    'distributor_item_code': stmt.excluded.distributor_item_code,
                }
                stmt = stmt.on_conflict_do_update(
                    index_elements=['transaction_hash'],
                    set_=update_cols
                )
                session.execute(stmt)

            # --- Stage 3: Idempotent historical aggregation ---
            # Rather than adding deltas to the historical table, recompute the
            # total revenue and transaction count directly from the transactions
            # table.  This ensures that reprocessing the same file multiple
            # times does not inflate totals and also creates missing historical
            # records for new accounts.  We only recalculate for the account/year
            # pairs present in this batch.
            logger.info(f"[Thread:{thread_id}] Recomputing historical totals from transactions...")

            # Pull the year for each row (from posting_date) and track the set of (canonical_code, year)
            cleaned_weekly_df['year'] = cleaned_weekly_df['posting_date'].apply(lambda d: d.year if d else None)
            affected_pairs = cleaned_weekly_df[['canonical_code', 'year']].drop_duplicates()
            affected_pairs = affected_pairs.dropna()

            for _, r in affected_pairs.iterrows():
                canon_code = r['canonical_code']
                yr = int(r['year'])

                # authoritative totals from transactions table
                total_revenue = (session.query(func.coalesce(func.sum(Transaction.revenue), 0.0))
                                       .filter(Transaction.canonical_code == canon_code)
                                       .filter(extract('year', Transaction.posting_date) == yr)
                                       .scalar() or 0.0)
                transaction_count = (session.query(func.count(Transaction.id))
                                      .filter(Transaction.canonical_code == canon_code)
                                      .filter(extract('year', Transaction.posting_date) == yr)
                                      .scalar() or 0)

                # Attempt to fetch an existing historical record; update or insert
                hist_record = (session.query(AccountHistoricalRevenue)
                                .filter_by(canonical_code=canon_code, year=yr)
                                .with_for_update()
                                .one_or_none())
                if hist_record:
                    hist_record.total_revenue = total_revenue
                    hist_record.transaction_count = transaction_count
                else:
                    # Derive base_card_code and ship_to_code from the first occurrence in this batch
                    base_card_code = None
                    ship_to_code = None
                    try:
                        base_card_code = cleaned_weekly_df.loc[cleaned_weekly_df['canonical_code'] == canon_code, 'base_card_code'].dropna().iloc[0]
                    except Exception:
                        pass
                    try:
                        ship_to_code = cleaned_weekly_df.loc[cleaned_weekly_df['canonical_code'] == canon_code, 'ship_to_code'].dropna().iloc[0]
                    except Exception:
                        pass

                    new_hist = AccountHistoricalRevenue(
                        canonical_code=canon_code,
                        year=yr,
                        total_revenue=total_revenue,
                        transaction_count=transaction_count,
                        base_card_code=base_card_code,
                        ship_to_code=ship_to_code
                    )
                    session.add(new_hist)

            # --- Stage 4: Recalculate predictions and metrics (idempotent) ---
            logger.info(f"[Thread:{thread_id}] Recalculating predictions/metrics...")
            predictions_df = recalculate_predictions_and_metrics(session=session)

            if predictions_df is None or predictions_df.empty:
                raise ValueError("Recalculation returned no data. Aborting to prevent data loss.")

            # --- Stage 5: Bulk Upsert Final Predictions ---
            logger.info(f"[Thread:{thread_id}] Performing bulk upsert for {len(predictions_df)} predictions...")
            # Convert DataFrame to list of dicts for insert
            update_data = predictions_df.replace({pd.NaT: None, np.nan: None}).to_dict('records')
            if update_data:
                stmt_pred = pg_insert(AccountPrediction).values(update_data)
                # Prepare columns to update on conflict.  We update all columns
                # except the primary key (id) and canonical_code.  Excluded
                # refers to the incoming data when a conflict occurs.
                updatable_cols = {}
                for c in AccountPrediction.__table__.columns.keys():
                    if c not in ('id', 'canonical_code'):
                        updatable_cols[c] = getattr(stmt_pred.excluded, c)
                stmt_pred = stmt_pred.on_conflict_do_update(
                    index_elements=['canonical_code'],
                    set_=updatable_cols
                )
                session.execute(stmt_pred)

            session.commit()
            logger.info(f"[Thread:{thread_id}] Processing complete and committed for {filepath}")
                        # ============================================
            # ADD THIS VERIFICATION SECTION RIGHT HERE
            # ============================================
            
            # Verify product coverage was calculated correctly
            try:
                coverage_check = session.execute(text("""
                    SELECT 
                        COUNT(*) as total,
                        COUNT(CASE WHEN product_coverage_percentage > 0 THEN 1 END) as with_coverage,
                        COUNT(CASE WHEN product_coverage_percentage = 0 THEN 1 END) as zero_coverage,
                        ROUND(CAST(AVG(product_coverage_percentage) AS numeric), 2) as avg_coverage,
                        ROUND(CAST(MAX(product_coverage_percentage) AS numeric), 2) as max_coverage,
                        ROUND(CAST(MIN(CASE WHEN product_coverage_percentage > 0 THEN product_coverage_percentage END) AS numeric), 2) as min_nonzero_coverage
                    FROM account_predictions
                """)).fetchone()
                
                logger.info(f"[Thread:{thread_id}] ===== PRODUCT COVERAGE VERIFICATION =====")
                logger.info(f"[Thread:{thread_id}] Total accounts: {coverage_check.total}")
                logger.info(f"[Thread:{thread_id}] Accounts WITH coverage: {coverage_check.with_coverage} ({coverage_check.with_coverage*100.0/coverage_check.total:.1f}%)")
                logger.info(f"[Thread:{thread_id}] Accounts with ZERO coverage: {coverage_check.zero_coverage} ({coverage_check.zero_coverage*100.0/coverage_check.total:.1f}%)")
                logger.info(f"[Thread:{thread_id}] Average coverage (all accounts): {coverage_check.avg_coverage}%")
                logger.info(f"[Thread:{thread_id}] Max coverage: {coverage_check.max_coverage}%")
                logger.info(f"[Thread:{thread_id}] Min non-zero coverage: {coverage_check.min_nonzero_coverage}%")
                
                # Alert if something seems wrong
                if coverage_check.with_coverage == 0:
                    logger.error(f"[Thread:{thread_id}] ❌ ERROR: No accounts have product coverage! Check TOP_30_SET configuration.")
                elif coverage_check.with_coverage < coverage_check.total * 0.3:
                    logger.warning(f"[Thread:{thread_id}] ⚠️ WARNING: Only {coverage_check.with_coverage*100.0/coverage_check.total:.1f}% of accounts have coverage. This seems low.")
                else:
                    logger.info(f"[Thread:{thread_id}] ✅ Product coverage calculated successfully!")
                    
            except Exception as verify_err:
                logger.error(f"[Thread:{thread_id}] Error during coverage verification: {verify_err}")
            
            logger.info(f"[Thread:{thread_id}] ========================================")
            
            # ============================================
            # END OF VERIFICATION SECTION
            # ============================================
        except Exception as e:
            logger.exception(f"[Thread:{thread_id}] Error during processing: {e}")
            session.rollback()
        finally:
            # Remove only temp uploads we created; leave server-side paths alone
            try:
                upload_root = current_app.config.get('UPLOAD_FOLDER', os.path.join('data', 'uploads'))
                if filepath and filepath.startswith(upload_root) and os.path.exists(filepath):
                    os.remove(filepath)
                    logger.info(f"[Thread:{thread_id}] Removed temp upload: {filepath}")
            except Exception as e:
                logger.warning(f"[Thread:{thread_id}] Could not remove temp file {filepath}: {e}")


@webhook_bp.route('/sales', methods=['POST'])
@require_hmac_signature
def receive_sales_file():
    """
    Receives a CSV sales file and processes it asynchronously.
    """
    # Accept both multipart/form-data with 'file' and JSON body with 'file_path'
    if request.content_type and 'application/json' in request.content_type.lower():
        try:
            data = request.get_json(force=True, silent=False) or {}
        except Exception as e:
            logger.error(f"JSON body parse error: {e}", exc_info=True)
            return jsonify({"error": "Invalid JSON body"}), 400

        file_path = data.get('file_path')
        if not file_path or not os.path.exists(file_path):
            return jsonify({"error": "Missing or invalid 'file_path' in JSON body."}), 400

        try:
            # start background processing
            thread = threading.Thread(
                target=process_file_async, 
                args=(current_app.config, file_path),
                daemon=True
            )
            thread.start()
            return jsonify({
                "status": "accepted", 
                "message": f"File '{os.path.basename(file_path)}' received, processing started in background."
            }), 202
        except Exception as e:
            logger.error(f"Error starting background processor (JSON mode): {e}", exc_info=True)
            return jsonify({"error": "Error starting background processing thread."}), 500

    # else: multipart/form-data path
    if 'file' not in request.files:
        logger.warning("No file part in the request.")
        return jsonify({"error": "No file part"}), 400
    
    file = request.files['file']
    if file.filename == "":
        logger.warning("Empty filename.")
        return jsonify({"error": "Empty filename"}), 400
    
    if file:
        temp_filepath = None 
        try:
            filename = secure_filename(file.filename)
            upload_folder = current_app.config.get('UPLOAD_FOLDER', os.path.join('data', 'uploads'))
            os.makedirs(upload_folder, exist_ok=True)
            timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
            temp_filename = f"{timestamp}_{filename}"
            temp_filepath = os.path.join(upload_folder, temp_filename)
            file.save(temp_filepath)
            logger.info(f"Saved uploaded file to {temp_filepath}")

            # start background processing
            thread = threading.Thread(
                target=process_file_async, 
                args=(current_app.config, temp_filepath),
                daemon=True
            )
            thread.start()
            return jsonify({
                "status": "accepted", 
                "message": f"File '{filename}' received, processing started in background."
            }), 202

        except Exception as e:
            logger.error(f"Error during file upload/saving or thread start: {str(e)}", exc_info=True)
            if temp_filepath and os.path.exists(temp_filepath):
                try: 
                    os.remove(temp_filepath)
                    logger.info(f"Cleaned up failed upload: {temp_filepath}")
                except Exception as rem_err: 
                    logger.error(f"Error removing failed upload {temp_filepath}: {rem_err}")
            return jsonify({"error": f"Failed to process file upload: {str(e)}"}), 500
    
    return jsonify({"status": "error", "message": "Could not process file."}), 400


# ---- Option B Reference (edit 5): keep readers from re-floating UPCs -----------------
# Wherever you READ from the DB into pandas, do one of:
# 1) CAST to TEXT in SQL:
#    SELECT canonical_code, CAST(item_code AS TEXT) AS item_code, posting_date, revenue FROM transactions
# 2) Or force dtype on read_sql_query:
#    dtype={'item_code': str}
# 3) And (belt & suspenders) sanitize string form:
#    df['item_code'] = (df['item_code'].astype(str).str.strip()
#                        .str.split('.').str[0]
#                        .str.replace(r'\D','', regex=True))
#    df.loc[df['item_code'].str.strip('0') == '', 'item_code'] = pd.NA
# Also, before JSON serialization, ensure item_code is str.
# ---------------------------------------------------------------------------
