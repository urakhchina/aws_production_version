#populate_transaction_item_codes_optimized.py

import click
from flask.cli import with_appcontext
from flask import current_app
from sqlalchemy import func, and_, update
import pandas as pd
import logging
import sys
import time
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError, ClientError # For S3 error handling
import os

# Attempt to import local modules
try:
    from pipeline import get_base_card_code, generate_canonical_code, normalize_address, normalize_store_name
    from store_mapper import apply_card_code_mapping, load_card_code_mapping
except ImportError as e:
    print(f"CRITICAL IMPORT ERROR in populate_transaction_item_codes_optimized.py: {e}")
    sys.exit("Aborting due to import error.")

# Logger setup
logger = logging.getLogger("populate_item_codes_optimized_script")
if not logger.handlers:
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

def create_match_key(row, is_csv=True):
    """
    Creates a standardized matching key from a row (Pandas Series or SQLAlchemy result).
    Adjust field names based on whether it's a CSV row or DB row if they differ.
    """
    try:
        if is_csv: # For CSV row (Pandas Series)
            can_code = str(row['csv_canonical_code']).strip() if pd.notna(row['csv_canonical_code']) else ""
            post_date_str = row['POSTINGDATE_DT'].strftime('%Y-%m-%d') if pd.notna(row['POSTINGDATE_DT']) else ""
            desc = str(row['DESCRIPTION']).strip() if pd.notna(row['DESCRIPTION']) else ""
            amt_str = f"{float(row['AMOUNT_NUM']):.2f}" if pd.notna(row['AMOUNT_NUM']) else ""
            qty_str = str(int(row['QUANTITY_NUM'])) if pd.notna(row['QUANTITY_NUM']) else ""
        else: # For DB row (SQLAlchemy RowMapping - acts like a dict)
            can_code = str(row.get('canonical_code')).strip() if row.get('canonical_code') else ""
            post_date_obj = row.get('posting_date')
            post_date_str = post_date_obj.strftime('%Y-%m-%d') if post_date_obj else ""
            desc = str(row.get('description')).strip() if row.get('description') else ""
            amt_val = row.get('amount')
            amt_str = f"{float(amt_val):.2f}" if amt_val is not None else ""
            qty_val = row.get('quantity')
            qty_str = str(int(qty_val)) if qty_val is not None else ""
        
        return f"{can_code}|{post_date_str}|{desc}|{amt_str}|{qty_str}"
    except Exception as e:
        # Simplified row logging to avoid excessive output or errors with complex row objects
        row_info_for_log = f"is_csv={is_csv}, keys available (approx): {list(row.keys()) if hasattr(row, 'keys') else 'N/A'}"
        logger.error(f"Error creating match key for row ({row_info_for_log}). Error: {e}", exc_info=True)
        return None

@click.command('populate-item-codes-optimized')
@click.option('--s3-uri', 
              required=False, 
              type=str, 
              help='S3 URI of the CSV file (e.g., s3://bucket-name/path/to/file.csv). Takes precedence over --csv-file.')
@click.option('--csv-file', 
              default=None, 
              type=click.Path(exists=True, dir_okay=False, resolve_path=True),
              help='Local path to the original CSV file (used if --s3-uri is not provided).')
@click.option('--csv-chunk-size', default=100000, type=int, show_default=True, help="Number of CSV rows to read into memory at a time.")
@click.option('--db-update-batch-size', default=10000, type=int, show_default=True, help="Number of DB updates to batch before committing.")
@click.option('--limit-csv-rows-total', default=0, type=int, show_default=True, help="Total CSV rows to process (0 for all). For testing.")
@with_appcontext
def populate_item_codes_optimized_command(s3_uri, csv_file, csv_chunk_size, db_update_batch_size, limit_csv_rows_total):
    from models import db, Transaction # Import db and Transaction here
    script_logger = current_app.logger.getChild('populate_item_codes_optimized')
    script_logger.setLevel(logging.DEBUG)

    actual_csv_to_process = None
    downloaded_from_s3 = False
    home_dir = os.path.expanduser("~")
    temp_download_sub_dir = "s3_temp_downloads_data_migration"
    temp_download_dir = os.path.join(home_dir, temp_download_sub_dir)

    script_logger.info(f"Attempting to use temporary download directory: {temp_download_dir}")
    try:
        os.makedirs(temp_download_dir, exist_ok=True)
        script_logger.info(f"Successfully ensured user's temporary download directory exists: {temp_download_dir}")
    except OSError as e:
        script_logger.error(f"Could not create user's temporary download directory {temp_download_dir}: {e}. This is unexpected. Aborting.", exc_info=True)
        click.echo(f"Critical error: Failed to create temporary download directory at {temp_download_dir}. Please check permissions.", err=True)
        return

    if not s3_uri and not csv_file:
        script_logger.error("Either --s3-uri or --csv-file must be provided.")
        click.echo("Error: Missing data source. Use --s3-uri or --csv-file.", err=True)
        return

    if s3_uri and csv_file:
        script_logger.info("Both --s3-uri and --csv-file provided. --s3-uri will be used.")

    if s3_uri:
        script_logger.info(f"Attempting to download CSV from S3 URI: {s3_uri}")
        print(f"Downloading CSV from S3: {s3_uri}...")
        try:
            s3 = boto3.client('s3')
            if not s3_uri.startswith("s3://"): 
                raise ValueError("Invalid S3 URI format. Must start with s3://")
            path_parts = s3_uri.replace("s3://", "").split("/")
            bucket_name = path_parts[0]
            object_key = "/".join(path_parts[1:])
            if not bucket_name or not object_key: 
                raise ValueError("Invalid S3 URI: Missing bucket name or object key.")
            
            filename = os.path.basename(object_key)
            # Ensure filename is not empty if object_key ends with /
            if not filename and object_key.endswith('/'):
                raise ValueError("Invalid S3 URI: Object key appears to be a directory (ends with '/'). Please provide a key to a file.")
            elif not filename: # Should not happen if bucket_name and object_key are valid and not a dir
                 raise ValueError("Invalid S3 URI: Could not determine filename from object key.")


            actual_csv_to_process = os.path.join(temp_download_dir, f"s3_dl_{int(time.time())}_{filename}")
            
            script_logger.info(f"FINAL DOWNLOAD PATH being used: {actual_csv_to_process}")
            print(f"DEBUG: Attempting to download to: {actual_csv_to_process}")
            
            s3.download_file(bucket_name, object_key, actual_csv_to_process)
            downloaded_from_s3 = True
            script_logger.info(f"Successfully downloaded S3 file to: {actual_csv_to_process}")
            print(f"Successfully downloaded S3 file to: {actual_csv_to_process}")
        except (NoCredentialsError, PartialCredentialsError) as e_cred:
            script_logger.error(f"S3 Download Error: AWS credentials problem: {e_cred}. Ensure EC2 instance role has S3 read access or credentials configured.", exc_info=True)
            click.echo(f"S3 Download Error: AWS credentials problem. Check logs.", err=True)
            return
        except ClientError as e_s3_client:
            error_code = e_s3_client.response.get('Error', {}).get('Code')
            if error_code == '404':
                script_logger.error(f"S3 Download Error: File not found (404) at {s3_uri}", exc_info=True)
                click.echo(f"S3 Download Error: File not found (404) at {s3_uri}", err=True)
            elif error_code == '403':
                script_logger.error(f"S3 Download Error: Access denied (403) for {s3_uri}. Check permissions.", exc_info=True)
                click.echo(f"S3 Download Error: Access denied (403) for {s3_uri}. Check permissions.", err=True)
            else:
                script_logger.error(f"S3 ClientError during download: {e_s3_client}", exc_info=True)
                click.echo(f"S3 ClientError during download: {e_s3_client}", err=True)
            return
        except ValueError as e_val: # Catch our custom ValueErrors for URI format
            script_logger.error(f"S3 URI Error: {e_val}", exc_info=True)
            click.echo(f"S3 URI Error: {e_val}", err=True)
            return
        except Exception as e_s3_other:
            script_logger.error(f"Unexpected error during S3 download: {e_s3_other}", exc_info=True)
            click.echo(f"Unexpected error during S3 download: {e_s3_other}", err=True)
            return
    elif csv_file: 
        actual_csv_to_process = csv_file
        script_logger.info(f"Using local CSV file: {actual_csv_to_process}")
        print(f"Using local CSV file: {actual_csv_to_process}")
    
    if not actual_csv_to_process or not os.path.exists(actual_csv_to_process): # Check existence after potential download
        script_logger.error(f"No valid CSV data source could be determined or file does not exist: {actual_csv_to_process}")
        click.echo(f"Error: No valid CSV data source found or file {actual_csv_to_process} does not exist.", err=True)
        if downloaded_from_s3 and actual_csv_to_process and os.path.exists(actual_csv_to_process): # Attempt cleanup if download happened but path is bad
            try: os.remove(actual_csv_to_process)
            except OSError: pass
        return

    print(f"Starting OPTIMIZED item_code population using data from: {actual_csv_to_process}")
    click.echo("\nWARNING: This script will modify 'item_code' in the 'transactions' table for matching records.", err=True)
    click.echo("It is STRONGLY recommended to back up your database before proceeding if you haven't already.", err=True)
    if not click.confirm('Do you acknowledge the WARNING about data modification and want to continue?', abort=True, default=False):
        if downloaded_from_s3 and os.path.exists(actual_csv_to_process):
            try: os.remove(actual_csv_to_process)
            except OSError: pass
        return

    start_time_total = time.time()
    try:
        script_logger.info("Phase 1: Building index of database transactions (item_code IS NULL)...")
        print("Phase 1: Building index of database transactions (item_code IS NULL)...")
        db_transaction_index = {} 
        db_rows_indexed_count = 0
        
        query = db.select(
            Transaction.id, Transaction.canonical_code, Transaction.posting_date,
            Transaction.description, Transaction.amount, Transaction.quantity
        ).where(Transaction.item_code.is_(None))

        stream_batch_size = 10000 
        results_iterator = db.session.execute(query.execution_options(yield_per=stream_batch_size)).mappings() 

        for db_row_mapping in results_iterator:
            key = create_match_key(db_row_mapping, is_csv=False)
            if key:
                transaction_id = db_row_mapping['id']
                if key in db_transaction_index:
                    script_logger.warning(f"DB_INDEX_DUPE_KEY: Key '{key}' for DB ID {transaction_id} already exists with DB ID {db_transaction_index[key]}. Overwriting.")
                db_transaction_index[key] = transaction_id
            else:
                script_logger.warning(f"DB_INDEX_KEY_FAIL: Failed to generate match key for DB row ID: {db_row_mapping.get('id', 'Unknown')}. Row data: {dict(db_row_mapping)}")
            
            db_rows_indexed_count += 1
            if db_rows_indexed_count % 100000 == 0:
                 script_logger.info(f"PROGRESS_DB_INDEX: Indexed {db_rows_indexed_count} DB transactions...")
                 print(f"PROGRESS_DB_INDEX: Indexed {db_rows_indexed_count} DB transactions...")
        
        script_logger.info(f"Finished Phase 1. Indexed {db_rows_indexed_count} DB transactions, resulting in {len(db_transaction_index)} unique keys in memory map.")
        print(f"Finished Phase 1. DB Index has {len(db_transaction_index)} entries.")
        
        if not db_transaction_index and db_rows_indexed_count > 0:
            script_logger.warning("DB Index is empty, but rows were processed. This might indicate all keys failed generation. Check create_match_key for DB rows.")
        elif not db_transaction_index and db_rows_indexed_count == 0:
            script_logger.info("No transactions in DB currently have item_code as NULL. No updates needed from this script if this is correct.")
            print("INFO: No transactions in DB need item_code update (all seem populated or query returned no NULLs).")
            if downloaded_from_s3 and os.path.exists(actual_csv_to_process):
                 try: os.remove(actual_csv_to_process); script_logger.info(f"Cleaned up {actual_csv_to_process}")
                 except OSError: pass
            return

        script_logger.info(f"Phase 2: Processing CSV '{actual_csv_to_process}' in chunks of {csv_chunk_size}...")
        print(f"Phase 2: Processing CSV '{actual_csv_to_process}' in chunks of {csv_chunk_size}...")
        dtypes_csv = {
            'CardCode': str, 'CUSTOMERID': str, 'NAME': str, 'ADDRESS': str, 'CITY': str, 'STATE': str, 
            'ZIPCODE': str, 'ITEM': str, 'DESCRIPTION': str, 'ITEMDESC': str, 'POSTINGDATE': str, 
            'QUANTITY': str, 'AMOUNT': str, 'SalesRep': str, 'SlpName': str, 'Distributor': str, 'ShipTo': str,
        }
        
        updates_to_apply = []
        total_csv_rows_read_by_pandas = 0
        total_csv_rows_fully_processed_for_match = 0
        updated_count = 0
        not_found_in_db_index = 0
        csv_rows_with_no_item = 0
        csv_rows_failed_key_gen = 0
        decision_log_count = 0
        non_match_detail_log_count = 0
        stop_processing_csv = False

        for df_chunk in pd.read_csv(actual_csv_to_process, dtype=dtypes_csv, chunksize=csv_chunk_size, low_memory=False, encoding='utf-8', keep_default_na=False, na_filter=False):
            if stop_processing_csv:
                break

            current_chunk_start_row_overall = total_csv_rows_read_by_pandas
            total_csv_rows_read_by_pandas += len(df_chunk)
            script_logger.info(f"Read CSV chunk with {len(df_chunk)} raw rows (Total raw read: {total_csv_rows_read_by_pandas}). Preparing chunk...")

            try:
                df_chunk.replace('', pd.NA, inplace=True) # Convert empty strings to NA for consistent handling
                df_chunk['CardCode'] = df_chunk['CardCode'].fillna('').astype(str).str.strip()
                for col in ['ShipTo', 'ADDRESS', 'CITY', 'STATE', 'ZIPCODE', 'NAME', 'DESCRIPTION', 'ITEM']:
                    if col in df_chunk.columns: 
                        df_chunk[col] = df_chunk[col].fillna('').astype(str).str.strip()
                    else: 
                        df_chunk[col] = '' # Add missing expected columns as empty strings
                
                df_chunk['POSTINGDATE_DT'] = pd.to_datetime(df_chunk['POSTINGDATE'], errors='coerce')
                df_chunk['AMOUNT_NUM'] = pd.to_numeric(df_chunk['AMOUNT'].astype(str).str.replace(',', '', regex=False), errors='coerce')
                df_chunk['QUANTITY_NUM'] = pd.to_numeric(df_chunk['QUANTITY'].astype(str).str.replace(',', '', regex=False), errors='coerce')

                original_chunk_len = len(df_chunk)
                # Ensure ITEM is present for matching logic, along with other critical fields
                df_chunk.dropna(subset=['CardCode', 'POSTINGDATE_DT', 'AMOUNT_NUM', 'QUANTITY_NUM', 'DESCRIPTION', 'ITEM'], inplace=True)
                if len(df_chunk) < original_chunk_len:
                    script_logger.info(f"Dropped {original_chunk_len - len(df_chunk)} rows from current chunk due to missing essential data for matching.")
                
                if df_chunk.empty:
                    script_logger.info("Current CSV chunk is empty after cleaning. Skipping to next chunk.")
                    if limit_csv_rows_total > 0 and total_csv_rows_fully_processed_for_match >= limit_csv_rows_total: stop_processing_csv = True
                    continue

                # Canonical code generation (assuming functions are robust)
                df_chunk['base_card_code'] = df_chunk['CardCode'].apply(get_base_card_code)
                temp_map_col = 'CardCode_for_explicit_map_chunk_temp'
                df_chunk[temp_map_col] = df_chunk['CardCode']
                df_chunk_mapped_explicitly = apply_card_code_mapping(df_chunk.copy(), card_code_column=temp_map_col) # Uses global card_code_mapping
                df_chunk['csv_canonical_code_stage1'] = df_chunk_mapped_explicitly[temp_map_col]
                
                final_canonical_codes_chunk = df_chunk['csv_canonical_code_stage1'].copy()
                needs_fallback_mask_chunk = (df_chunk['csv_canonical_code_stage1'].str.strip().eq('') | df_chunk['csv_canonical_code_stage1'].eq(df_chunk['CardCode']))
                if needs_fallback_mask_chunk.any():
                    generated_fallback_codes_chunk = df_chunk[needs_fallback_mask_chunk].apply(generate_canonical_code, axis=1)
                    final_canonical_codes_chunk.loc[needs_fallback_mask_chunk] = generated_fallback_codes_chunk
                df_chunk['csv_canonical_code'] = final_canonical_codes_chunk
                df_chunk.drop(columns=[temp_map_col, 'csv_canonical_code_stage1'], inplace=True, errors='ignore')
                
                df_chunk['match_key'] = df_chunk.apply(create_match_key, axis=1, is_csv=True)

            except Exception as e_prepare_chunk:
                script_logger.error(f"Error preparing CSV chunk (starting raw row ~{current_chunk_start_row_overall}): {e_prepare_chunk}", exc_info=True)
                if not click.confirm("Error during chunk preparation. Continue to next chunk?", default=False): 
                    raise # Re-raise to abort script
                continue # Skip to next chunk if user confirms

            script_logger.debug(f"DEBUG_CHUNK: Prepared {len(df_chunk)} rows in chunk. Iterating for matches...")
            for csv_idx, csv_row in df_chunk.iterrows():
                if limit_csv_rows_total > 0 and total_csv_rows_fully_processed_for_match >= limit_csv_rows_total:
                    stop_processing_csv = True
                    break 

                csv_item_code = csv_row.get('ITEM', '') # Use .get for safety
                if not csv_item_code or pd.isna(csv_item_code): # Check for empty string as well
                    csv_rows_with_no_item += 1
                    continue 
                
                row_match_key = csv_row.get('match_key') # Use .get for safety
                if not row_match_key or pd.isna(row_match_key):
                    csv_rows_failed_key_gen +=1
                    continue
                
                total_csv_rows_fully_processed_for_match += 1
                transaction_db_id = db_transaction_index.get(row_match_key)

                if decision_log_count < 20:
                    log_prefix = f"DECISION_LOG (OverallAttempt#{total_csv_rows_fully_processed_for_match}, CSVOriginalIndex:{csv_idx}): Key='{row_match_key}', DB_ID='{transaction_db_id}' -> "
                    script_logger.debug(f"{log_prefix}{'MATCHED_IN_INDEX' if transaction_db_id else 'NO_MATCH_IN_INDEX'}")
                    decision_log_count += 1

                if transaction_db_id:
                    updates_to_apply.append({'id': transaction_db_id, 'item_code': csv_item_code})
                    updated_count += 1
                else:
                    not_found_in_db_index += 1
                    if non_match_detail_log_count < 20:
                        script_logger.info(f"NO_DB_MATCH_IN_INDEX (OverallAttempt#{total_csv_rows_fully_processed_for_match}, CSVOriginalIndex:{csv_idx}):")
                        script_logger.info(f"  L-> CSV Data: CardCode='{csv_row.get('CardCode', 'N/A')}', Date='{csv_row.get('POSTINGDATE', 'N/A')}', Desc='{str(csv_row.get('DESCRIPTION', 'N/A'))[:50]}...', ITEM='{csv_row.get('ITEM', 'N/A')}'")
                        script_logger.info(f"  L-> CSV Key: '{row_match_key}'")
                        non_match_detail_log_count += 1
            
            if stop_processing_csv: 
                script_logger.info(f"Limit of {limit_csv_rows_total} fully processed CSV rows reached within chunk. Breaking from chunk loop.")
                break

            script_logger.info(f"Finished CSV chunk. Overall rows fully processed for matching: {total_csv_rows_fully_processed_for_match}. Updates in batch: {len(updates_to_apply)}.")
            if len(updates_to_apply) >= db_update_batch_size:
                script_logger.info(f"Committing batch of {len(updates_to_apply)} updates...")
                db.session.bulk_update_mappings(Transaction, updates_to_apply)
                db.session.commit()
                script_logger.info(f"Committed. Total DB updates so far: {updated_count}")
                updates_to_apply = []

        if updates_to_apply: 
            script_logger.info(f"Committing final batch of {len(updates_to_apply)} updates...")
            db.session.bulk_update_mappings(Transaction, updates_to_apply)
            db.session.commit()
            script_logger.info(f"Committed. Total DB updates: {updated_count}")

        total_time = time.time() - start_time_total
        script_logger.info("--------------------------------------------------------------------")
        script_logger.info(f"OPTIMIZED item_code population finished in {total_time:.2f} seconds.")
        script_logger.info(f"Total CSV rows read by pandas: {total_csv_rows_read_by_pandas}")
        script_logger.info(f"Total CSV rows fully processed (attempted for matching): {total_csv_rows_fully_processed_for_match}")
        script_logger.info(f"Database transactions updated with item_code: {updated_count}")
        script_logger.info(f"CSV rows (attempted for matching) not matched in DB index: {not_found_in_db_index}")
        script_logger.info(f"CSV rows skipped due to missing ITEM (before matching attempt): {csv_rows_with_no_item}")
        script_logger.info(f"CSV rows skipped due to failing match key generation (before matching attempt): {csv_rows_failed_key_gen}")
        script_logger.info("--------------------------------------------------------------------")
        
        print("\n--- OPTIMIZED SUMMARY ---")
        print(f"Finished in {total_time:.2f} seconds.")
        print(f"Total CSV rows read by pandas: {total_csv_rows_read_by_pandas}")
        print(f"Total CSV rows fully processed (attempted for matching): {total_csv_rows_fully_processed_for_match}")
        print(f"Database transactions updated with item_code: {updated_count}")
        print(f"CSV rows not matched in DB index: {not_found_in_db_index}")
        print(f"CSV rows skipped (no ITEM): {csv_rows_with_no_item}")
        print(f"CSV rows skipped (key gen fail): {csv_rows_failed_key_gen}")

        if not_found_in_db_index > 0 and updated_count == 0: 
            print("\nWARNING: No DB transactions were updated. All processed CSV rows failed to match existing DB records needing update.")
            print("  This could be due to: data discrepancies, issues with `create_match_key`, or already populated `item_code` in DB.")
        elif not_found_in_db_index > 0: 
            print(f"\nWARNING: {not_found_in_db_index} CSV rows did not match existing DB transactions needing update.")
        elif updated_count > 0: 
            print("\nItem code population completed successfully.")
        elif total_csv_rows_fully_processed_for_match == 0 and (csv_rows_with_no_item > 0 or csv_rows_failed_key_gen > 0):
            print("\nNo item codes were updated because no CSV rows passed initial filtering (missing ITEM or key generation failed).")
        else: 
            print("\nNo item codes were updated. No CSV rows matched DB records, or no CSV rows were processed.")
        
        print("\nNEXT STEP (if successful): Run 'flask migrate-historical-skus' to update AccountHistoricalRevenue.")

    except FileNotFoundError:
        script_logger.error(f"CSV file not found: {actual_csv_to_process}") # Use actual_csv_to_process here
        print(f"ERROR: CSV file not found at {actual_csv_to_process}")
    except pd.errors.EmptyDataError:
        script_logger.error(f"CSV file is empty or unreadable: {actual_csv_to_process}")
        print(f"ERROR: CSV file is empty or unreadable: {actual_csv_to_process}")
    except ImportError as e_imp: # Catch import errors for pipeline/store_mapper if they occur late (should be caught early)
        script_logger.critical(f"A CRITICAL import error occurred during script execution: {e_imp}. This should have been caught at startup.", exc_info=True)
        print(f"CRITICAL IMPORT ERROR: {e_imp}. Aborting.")
    except Exception as e:
        if db.session.is_active: # Check if session is active before rollback
            db.session.rollback()
            script_logger.info("Database session rolled back due to error.")
        script_logger.error(f"A critical error occurred during OPTIMIZED item_code population: {e}", exc_info=True)
        print(f"CRITICAL ERROR: An unexpected error occurred: {e}")
        print("Process aborted. Database changes rolled back if any were pending.")
    finally:
        if downloaded_from_s3 and actual_csv_to_process and os.path.exists(actual_csv_to_process):
            try:
                os.remove(actual_csv_to_process)
                script_logger.info(f"Cleaned up temporary downloaded S3 file: {actual_csv_to_process}")
                print(f"Cleaned up temporary S3 file: {actual_csv_to_process}")
            except Exception as e_remove:
                script_logger.error(f"Error removing temporary S3 file {actual_csv_to_process}: {e_remove}")

# For Flask CLI registration (typically in app.py or a commands.py file)
# from migrations.populate_transaction_item_codes_optimized import populate_item_codes_optimized_command
# def register_commands(app):
#     app.cli.add_command(populate_item_codes_optimized_command)