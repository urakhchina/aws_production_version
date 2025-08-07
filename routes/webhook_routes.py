import os
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
from sqlalchemy import select 
# This is the specific SQLAlchemy command for PostgreSQL's "UPSERT" feature
from sqlalchemy.dialects.postgresql import insert as pg_insert

try:
    from pipeline import (recalculate_predictions_and_metrics, clean_data, 
                          aggregate_item_codes, safe_json_dumps,
                          generate_canonical_code, get_base_card_code)
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
        return int(float(value))
    except (ValueError, TypeError): return default

def require_hmac(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        secret_key = current_app.config.get('HMAC_SECRET_KEY')
        if not secret_key:
            logger.error("HMAC validation failed: HMAC_SECRET_KEY not configured on server.")
            abort(500, description="Internal configuration error.") 

        req_signature = request.headers.get('X-Signature')
        req_timestamp_str = request.headers.get('X-Request-Timestamp')

        if not req_signature or not req_timestamp_str:
            logger.warning("HMAC validation failed: Missing X-Signature or X-Request-Timestamp header.")
            abort(400, description="Missing required signature headers.")

        try:
            req_timestamp = int(req_timestamp_str)
            current_timestamp = int(time.time())
            max_time_diff = 300 
            time_difference = abs(current_timestamp - req_timestamp)
            if time_difference > max_time_diff:
                logger.warning(f"HMAC validation failed: Timestamp expired or invalid. Diff: {time_difference}s > {max_time_diff}s")
                abort(401, description="Request timestamp is too old or invalid.") 
        except ValueError:
            logger.warning(f"HMAC validation failed: Invalid timestamp format '{req_timestamp_str}'.")
            abort(400, description="Invalid timestamp format.")

        uploaded_file = request.files.get('file')
        if not uploaded_file:
             logger.warning("HMAC validation failed: No file uploaded in 'file' field.")
             abort(400, description="No file part found.")

        try:
            file_content = uploaded_file.read()
            uploaded_file.seek(0) 
        except Exception as read_err:
            logger.error(f"Error reading uploaded file stream: {read_err}")
            abort(500, description="Error reading uploaded file.")

        calculated_hash = hashlib.sha256(file_content).hexdigest()
        message_to_sign = f"{req_timestamp_str}.{calculated_hash}"
        message_bytes = message_to_sign.encode('utf-8')
        secret_bytes = secret_key.encode('utf-8')
        calculated_signature = hmac.new(secret_bytes, message_bytes, hashlib.sha256).hexdigest()

        if not hmac.compare_digest(calculated_signature, req_signature):
            logger.warning("HMAC validation failed: Signature mismatch.")
            abort(401, description="Invalid signature.") 

        logger.info("HMAC signature verified successfully.")
        return f(*args, **kwargs)
    return decorated_function

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
                weekly_df = pd.read_csv(filepath, dtype=str, encoding='utf-8', on_bad_lines='warn', low_memory=False)
            except UnicodeDecodeError:
                weekly_df = pd.read_csv(filepath, dtype=str, encoding='latin-1', on_bad_lines='warn', low_memory=False)

            if weekly_df.empty:
                logger.warning(f"File {filepath} is empty. Aborting process.")
                return

            if "CardCode" in weekly_df.columns: weekly_df.rename(columns={"CardCode": "CARD_CODE"}, inplace=True)
            for col in ['ShipTo', 'NAME', 'ADDRESS', 'CITY', 'STATE', 'ZIPCODE', 'ITEM']:
                 if col not in weekly_df.columns: weekly_df[col] = ''
            
            weekly_df['base_card_code'] = weekly_df['CARD_CODE'].apply(get_base_card_code)
            weekly_df['canonical_code'] = weekly_df.apply(generate_canonical_code, axis=1)
            weekly_df.dropna(subset=['canonical_code'], inplace=True)
            
            cleaned_weekly_df = clean_data(weekly_df.copy())
            if cleaned_weekly_df.empty:
                logger.warning("Data is empty after cleaning. Aborting process."); return

            # --- FIX: Standardize names and create revenue column BEFORE any other step ---
            logger.info(f"[Thread:{thread_id}] Standardizing column names for consistency...")
            column_rename_map = {
                'POSTINGDATE': 'posting_date', 'ITEM': 'item_code', 'QUANTITY': 'quantity',
                'AMOUNT': 'amount', 'NAME': 'name', 'DESCRIPTION': 'description',
                'SalesRep': 'sales_rep', 'Distributor': 'distributor', 'CUSTOMERID': 'customer_id',
                'ADDRESS': 'address', 'CITY': 'city', 'STATE': 'state', 'ZIPCODE': 'zipcode',
                'SlpName': 'sales_rep_name'
            }
            cleaned_weekly_df.rename(columns=lambda c: column_rename_map.get(c, c), inplace=True)

            if 'CardName' in cleaned_weekly_df.columns:
                # Make sure both columns are strings
                cleaned_weekly_df['CardName'] = cleaned_weekly_df['CardName'].fillna('').astype(str).str.strip()
                cleaned_weekly_df['name'] = cleaned_weekly_df['name'].fillna('').astype(str).str.strip()

                # Prefer CardName when it is non‑empty; otherwise leave name as is.
                mask = cleaned_weekly_df['CardName'] != ''
                cleaned_weekly_df.loc[mask, 'name'] = cleaned_weekly_df.loc[mask, 'CardName']

                # Optionally drop CardName if you don’t need to store it
                # cleaned_weekly_df.drop(columns=['CardName'], inplace=True)

            if 'revenue' not in cleaned_weekly_df.columns:
                cleaned_weekly_df['revenue'] = pd.to_numeric(cleaned_weekly_df.get('amount'), errors='coerce').fillna(0)
            # --- END FIX ---

            # --- Stage 2: Generate Hashes and Insert Transactions ---
            logger.info(f"[Thread:{thread_id}] Calculating deterministic hashes for incoming transactions...")
            duplicate_check_cols = ['canonical_code', 'posting_date', 'item_code', 'revenue', 'quantity']
            for col in duplicate_check_cols:
                if col not in cleaned_weekly_df.columns:
                    raise KeyError(f"DataFrame is missing required column for hashing: '{col}'")
            
            cleaned_weekly_df.sort_values(by=duplicate_check_cols, inplace=True, na_position='first')
            cleaned_weekly_df['duplicate_rank'] = cleaned_weekly_df.groupby(duplicate_check_cols).cumcount()

            def generate_hash(row):
                unique_string = (f"{row.get('canonical_code', '')}|{row.get('posting_date', '')}|"
                                 f"{row.get('item_code', '')}|{row.get('revenue', '')}|{row.get('quantity', '')}|"
                                 f"{row.get('duplicate_rank', '')}")
                return hashlib.sha256(unique_string.encode()).hexdigest()

            cleaned_weekly_df['transaction_hash'] = cleaned_weekly_df.apply(generate_hash, axis=1)

            logger.info(f"[Thread:{thread_id}] Preparing to insert {len(cleaned_weekly_df)} rows into transactions table...")
            transaction_cols = [c.name for c in Transaction.__table__.columns if c.name != 'id']
            for col in transaction_cols:
                if col not in cleaned_weekly_df.columns: cleaned_weekly_df[col] = None
            
            transactions_to_insert = cleaned_weekly_df[transaction_cols].replace({pd.NaT: None, np.nan: None}).to_dict(orient='records')
            
            if transactions_to_insert:
                #stmt = pg_insert(Transaction).values(transactions_to_insert)
                #stmt = stmt.on_conflict_do_nothing(index_elements=['transaction_hash'])
                #session.execute(stmt)
                #logger.info(f"Executed idempotent insert for {len(transactions_to_insert)} transaction records.")

                stmt = pg_insert(Transaction).values(transactions_to_insert)
                update_cols = {
                    'name': stmt.excluded.name,
                    'distributor': stmt.excluded.distributor,
                    'sales_rep': stmt.excluded.sales_rep,
                    # add any other columns you want to refresh
                }
                stmt = stmt.on_conflict_do_update(
                    index_elements=['transaction_hash'],
                    set_=update_cols
                )
                session.execute(stmt)

            # --- Stage 3: Aggregate and Update Historical Table ---
            logger.info(f"[Thread:{thread_id}] Aggregating and updating historical revenue...")
            weekly_agg = cleaned_weekly_df.groupby(['canonical_code', 'year'], as_index=False).agg(
                revenue_change=('revenue', 'sum'),
                transaction_change=('posting_date', 'count')
            )
            for _, row in weekly_agg.iterrows():
                hist_record = session.query(AccountHistoricalRevenue).filter_by(
                    canonical_code=row['canonical_code'], year=row['year']
                ).with_for_update().one_or_none()
                if hist_record:
                    hist_record.total_revenue = (hist_record.total_revenue or 0) + row['revenue_change']
                    hist_record.transaction_count = (hist_record.transaction_count or 0) + row['transaction_change']
                else:
                    logger.warning(f"No existing AccountHistoricalRevenue record for {row['canonical_code']}/{row['year']}. Webhook only updates existing accounts.")

            # --- Stage 4: Run Full Recalculation ---
            logger.info(f"[Thread:{thread_id}] Triggering full prediction recalculation...")
            predictions_df = recalculate_predictions_and_metrics()
            if predictions_df is None or predictions_df.empty:
                raise ValueError("Recalculation returned no data. Aborting to prevent data loss.")

            # --- Stage 5: Bulk Update Final Predictions ---
            logger.info(f"[Thread:{thread_id}] Performing bulk update for {len(predictions_df)} predictions...")
            update_data = predictions_df.replace({pd.NaT: None, np.nan: None}).to_dict('records')
            session.bulk_update_mappings(AccountPrediction, update_data)
            
            # --- FINAL COMMIT ---
            session.commit()
            logger.info("--- SUCCESS: All database operations committed successfully! ---")

        except Exception as e:
            logger.error(f"--- FAILED: An error occurred. Rolling back all changes. Error: {e} ---", exc_info=True)
            session.rollback()
        finally:
            session.close()
            if os.path.exists(filepath):
                os.remove(filepath)
                logger.info(f"Temporary file removed: {filepath}")

@webhook_bp.route('/sales', methods=['POST'])
@require_hmac
def ingest_sales():
    logger.info("Webhook endpoint called - receiving file...")
    if 'file' not in request.files:
        logger.warning("No file part.")
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

            logger.info(f"Saving uploaded file temporarily to: {temp_filepath}")
            file.save(temp_filepath)
            logger.info("File saved successfully.")

            logger.info(f"Starting background processing thread for {temp_filename}.")
            thread_config = {k: v for k, v in current_app.config.items()}
            processing_thread = threading.Thread(
                target=process_file_async, 
                args=(thread_config, temp_filepath,)
            )
            processing_thread.daemon = True 
            processing_thread.start()

            logger.info(f"File '{filename}' received, background processing started.")
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