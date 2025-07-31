# data_migration_sku_setup.py

import click
from flask.cli import with_appcontext
from sqlalchemy import func, select
#from app import db, app # Assuming your Flask app instance is named 'app' in app.py
from flask import current_app
from models import Transaction, AccountHistoricalRevenue, AccountPrediction
import json
import logging
from collections import defaultdict
import sys
import time

# Logger setup
logger = logging.getLogger("migrate_historical_skus_script")
if not logger.handlers:
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)


def safe_json_dumps_for_historical(data_list):
    """Helper to safely dump a list to a JSON string for historical revenue."""
    if not data_list:
        return None # Store None if the list is empty or None
    try:
        # Ensure SKUs are strings and unique, then sort
        unique_sorted_skus = sorted(list(set(str(sku) for sku in data_list if sku and str(sku).strip())))
        if not unique_sorted_skus: # If after cleaning, the list is empty
            return None
        return json.dumps(unique_sorted_skus)
    except TypeError as e:
        logger.error(f"Error dumping data to JSON for historical: {e}. Data: {data_list}")
        return None # Store None on error

@click.command('migrate-historical-skus')
@click.option('--clear-prediction-products', is_flag=True, default=False, help="Clear product-related fields in AccountPrediction table.")
@click.option('--batch-size-ahr', default=500, type=int, show_default=True, help="Batch size for updating AccountHistoricalRevenue records.")
@with_appcontext
def migrate_historical_skus_command(clear_prediction_products, batch_size_ahr):
    """
    Data migration script to:
    1. Rebuild AccountHistoricalRevenue.yearly_products_json with SKUs from Transactions.
    2. Optionally clear product-related fields in AccountPrediction.
    This script ASSUMES Transaction.item_code has been populated.
    """
    from models import db
    script_logger = logging.getLogger('flask.app').getChild('migrate_historical_skus')
    script_logger.info("Starting data migration: Rebuilding AccountHistoricalRevenue.yearly_products_json with SKUs.")
    print("Starting data migration: Rebuilding AccountHistoricalRevenue.yearly_products_json with SKUs...")
    print("Ensure 'flask populate-item-codes-optimized' has been run successfully before this script.")
    if not click.confirm("Have you backed up your database and populated Transaction.item_code?", default=False, abort=True):
        script_logger.info("User aborted operation.")
        return

    start_time = time.time()

    # --- Step 1: Rebuild AccountHistoricalRevenue.yearly_products_json ---
    script_logger.info("Fetching all transactions (canonical_code, year, item_code) to aggregate SKUs...")
    print("Fetching all transactions to aggregate SKUs...")
    
    transactions_query = db.select(
        Transaction.canonical_code,
        Transaction.year,
        Transaction.item_code # This is the now-populated SKU
    ).where(
        Transaction.item_code.isnot(None),
        Transaction.item_code != '' 
    ).distinct() # Get distinct combinations to reduce initial data size if item_code is repeated within same year/canon
                 # Or, more robustly, group by and aggregate unique item_codes directly via SQL if DB supports it well.
                 # For now, fetching and processing in Python is fine given previous performance.

    all_transactions_for_sku_agg = db.session.execute(transactions_query).fetchall()
    
    if not all_transactions_for_sku_agg:
        script_logger.warning("No transactions found with valid item_codes. Cannot rebuild AccountHistoricalRevenue.yearly_products_json.")
        print("WARNING: No transactions with item_codes found. Migration for yearly_products_json skipped.")
    else:
        script_logger.info(f"Fetched {len(all_transactions_for_sku_agg)} transaction records for SKU aggregation.")
        print(f"Fetched {len(all_transactions_for_sku_agg)} transaction records for SKU aggregation.")

        skus_by_account_year = defaultdict(set)
        for t_canon, t_year, t_item in all_transactions_for_sku_agg:
            if t_canon and t_year is not None and t_item:
                skus_by_account_year[(t_canon, t_year)].add(str(t_item))

        script_logger.info(f"Aggregated SKUs for {len(skus_by_account_year)} unique (account, year) combinations.")
        print(f"Aggregated SKUs for {len(skus_by_account_year)} unique (account, year) combinations.")

        # Update AccountHistoricalRevenue records
        updated_ahr_count = 0
        
        all_ahr_records_stmt = db.select(AccountHistoricalRevenue) # Select all AHR records
        # Consider processing AHR records in batches if the table is enormous,
        # but typically it's smaller than transactions.
        all_ahr_records = db.session.execute(all_ahr_records_stmt).scalars().all()
        total_ahr_to_process = len(all_ahr_records)
        
        script_logger.info(f"Processing {total_ahr_to_process} AccountHistoricalRevenue records...")
        print(f"Processing {total_ahr_to_process} AccountHistoricalRevenue records...")

        for i, ahr_record in enumerate(all_ahr_records):
            key = (ahr_record.canonical_code, ahr_record.year)
            yearly_skus_set = skus_by_account_year.get(key, set())
            
            new_yearly_products_json = safe_json_dumps_for_historical(list(yearly_skus_set))

            if str(ahr_record.yearly_products_json) != str(new_yearly_products_json): # Compare as strings to handle None vs "null" etc.
                ahr_record.yearly_products_json = new_yearly_products_json
                updated_ahr_count += 1
            
            if (i + 1) % batch_size_ahr == 0 or (i + 1) == total_ahr_to_process:
                if updated_ahr_count > 0 : # Only commit if there are changes accumulated in this conceptual "batch"
                    try:
                        # The actual count of records physically changed in THIS commit might be less than updated_ahr_count
                        # if some were already correct. updated_ahr_count tracks how many records were *marked* for update.
                        script_logger.info(f"Committing batch. Processed {i+1}/{total_ahr_to_process} AHR records. Updates in this logical batch: {updated_ahr_count - ( (i+1)//batch_size_ahr * batch_size_ahr if (i+1) != total_ahr_to_process else 0 ) }")
                        print(f"Committing batch... (processed {i+1}/{total_ahr_to_process})")
                        db.session.commit()
                        script_logger.info(f"Successfully committed changes. Total AHR records marked for update so far: {updated_ahr_count}")
                    except Exception as e:
                        script_logger.error(f"Error committing AHR updates: {e}", exc_info=True)
                        db.session.rollback()
                        print(f"ERROR: Could not commit AHR updates. Rolling back batch. See logs.")
                        if not click.confirm("A batch commit for AHR failed. Continue?", default=False, abort=True):
                           return
        
        # Final commit if any remaining uncommitted changes
        # The loop structure should handle the last batch correctly.
        # A final check:
        # if db.session.dirty:
        #    script_logger.info("Committing final dirty session for AHR.")
        #    db.session.commit()

        script_logger.info(f"Finished updating AccountHistoricalRevenue. Total records whose yearly_products_json was changed: {updated_ahr_count}")
        print(f"Finished updating AccountHistoricalRevenue. Total records changed: {updated_ahr_count}")

    if clear_prediction_products:
        script_logger.info("Clearing product-related JSON fields in AccountPrediction table for a fresh start...")
        print("Clearing product-related JSON fields in AccountPrediction for a fresh start...")
        try:
            update_stmt = (
                db.update(AccountPrediction)
                .values(
                    products_purchased=None,
                    carried_top_products_json=None,
                    missing_top_products_json=None,
                    product_coverage_percentage=None # Or 0.0 if you prefer
                )
            )
            result = db.session.execute(update_stmt)
            db.session.commit()
            script_logger.info(f"Successfully cleared product-related JSON fields for {result.rowcount} AccountPrediction records.")
            print(f"Successfully cleared product-related JSON fields for {result.rowcount} AccountPrediction records.")
        except Exception as e:
            script_logger.error(f"Error clearing AccountPrediction product fields: {e}", exc_info=True)
            db.session.rollback()
            print("ERROR: Could not clear AccountPrediction product fields. See logs.")
            print("Please consider running 'flask recalculate-predictions' MANUALLY after addressing any errors.")
            return
    else:
        script_logger.info("Skipped clearing product-related fields in AccountPrediction (--clear-prediction-products not set).")
        print("Skipped clearing product-related fields in AccountPrediction.")

    total_script_time = time.time() - start_time
    script_logger.info(f"Data migration for SKUs in AccountHistoricalRevenue completed in {total_script_time:.2f} seconds.")
    print(f"Data migration for SKUs in AccountHistoricalRevenue completed in {total_script_time:.2f} seconds.")
    print("\nNEXT STEP: Run 'flask recalculate-predictions' to update AccountPrediction table with new SKU data.")
    print("Example: flask recalculate-predictions")

# Registration in app.py:
# from data_migration_sku_setup import migrate_historical_skus_command # Adjust filename/path if needed
# app.cli.add_command(migrate_historical_skus_command)