# backfill_hashes.py
import hashlib
import pandas as pd
from sqlalchemy import create_engine, text
from app import create_app
import time

def generate_hash(row):
    # Use .get() with default values to handle potential None/NaN in source data
    unique_string = (f"{row.get('canonical_code', '')}|{row.get('posting_date', '')}|"
                     f"{row.get('item_code', '')}|{row.get('revenue', '')}|{row.get('quantity', '')}|"
                     f"{row.get('duplicate_rank', '')}")
    return hashlib.sha256(unique_string.encode()).hexdigest()

def run_backfill():
    app = create_app()
    with app.app_context():
        db_uri = app.config['SQLALCHEMY_DATABASE_URI']
        engine = create_engine(db_uri)

        # The 'with' block manages the overall transaction
        with engine.connect() as conn:
            # Manually begin a transaction that we will commit after all chunks
            outer_trans = conn.begin()
            try:
                print("Starting memory-efficient backfill process...")
                
                query = text("""
                    SELECT id, canonical_code, posting_date, item_code, revenue, quantity,
                           ROW_NUMBER() OVER(PARTITION BY canonical_code, posting_date, item_code, revenue, quantity ORDER BY id) as duplicate_rank
                    FROM transactions WHERE transaction_hash IS NULL
                """)
                # Added WHERE transaction_hash IS NULL to make the script restartable

                chunk_size = 50000 
                total_updated = 0
                start_time = time.time()
                
                for i, chunk_df in enumerate(pd.read_sql_query(query, conn, chunksize=chunk_size)):
                    if chunk_df.empty:
                        print("No more rows to process.")
                        break

                    print(f"Processing chunk {i + 1} ({len(chunk_df)} rows)...")
                    
                    # Calculate hashes
                    chunk_df['transaction_hash'] = chunk_df.apply(generate_hash, axis=1)
                    
                    # Prepare data for update
                    updates = chunk_df[['id', 'transaction_hash']].to_dict('records')
                    
                    # Execute the update for the current chunk
                    conn.execute(
                        text("UPDATE transactions SET transaction_hash = :transaction_hash WHERE id = :id"),
                        updates
                    )
                    total_updated += len(updates)
                    elapsed = time.time() - start_time
                    print(f"  ...processed. Total rows updated so far: {total_updated}. Time elapsed: {elapsed:.2f}s")
                
                print("\nBackfill processing complete. Committing all changes...")
                outer_trans.commit() # Commit the single, large transaction
                print(f"--- SUCCESS: Committed updates for {total_updated} rows. ---")

            except Exception as e:
                print(f"\n--- ERROR: An error occurred during the backfill. Rolling back. Error: {e} ---")
                outer_trans.rollback()
                raise # Re-raise the exception to see the full traceback

if __name__ == "__main__":
    run_backfill()