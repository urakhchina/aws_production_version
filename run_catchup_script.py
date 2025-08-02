#!/usr/bin/env python3
"""
Run a one‑time catch‑up for missing account and historical revenue data.

This script is intended to be executed locally within your Flask environment
(the same environment that hosts the webhook).  It looks at all transactions
currently in the database and ensures that each unique canonical code has
corresponding rows in both the `account_predictions` and
`account_historical_revenues` tables.  After inserting missing rows, it
triggers a full metric recalculation so the new accounts receive health
scores, cadence metrics and other analytics.

Usage:
    python run_catchup_script.py

Make sure your environment variables (e.g. SQLALCHEMY_DATABASE_URI) are set
properly before running.  The script uses the application factory pattern
(`create_app` from app.py) to load configuration and initialize the database.
"""

import sys
from datetime import datetime
from collections import defaultdict

import pandas as pd
from sqlalchemy import select, func, extract
from sqlalchemy.dialects.postgresql import insert as pg_insert

try:
    # Import the Flask app factory and models
    from app import create_app
    from models import db, Transaction, AccountPrediction, AccountHistoricalRevenue
    from pipeline import recalculate_predictions_and_metrics
except ImportError as imp_err:
    print(f"Error importing application or models: {imp_err}")
    sys.exit(1)


def perform_catchup():
    """Run the catch‑up routine to seed new accounts and historical data."""
    app = create_app()

    with app.app_context():
        session = db.session

        # Step 1: Collect all canonical codes from the transactions table
        print("Collecting canonical codes from transactions…")
        codes_in_trans = {
            row[0]
            for row in session.execute(select(Transaction.canonical_code).where(Transaction.canonical_code.isnot(None))).all()
        }
        print(f"Found {len(codes_in_trans)} distinct canonical codes in transactions.")

        # Step 2: Determine which codes already exist in account_predictions
        existing_codes = {
            row[0]
            for row in session.execute(
                select(AccountPrediction.canonical_code).where(AccountPrediction.canonical_code.in_(codes_in_trans))
            ).all()
        }
        new_codes = codes_in_trans - existing_codes
        print(f"{len(new_codes)} canonical codes are missing from account_predictions and will be inserted.")

        # Step 3: Build minimal AccountPrediction records for new codes
        new_ap_records = []
        for code in new_codes:
            # Fetch the most recent transaction for this canonical code to derive meta information
            tx = (
                session.execute(
                    select(Transaction).where(Transaction.canonical_code == code).order_by(Transaction.posting_date.desc())
                ).scalar()
            )
            if tx is None:
                continue

            # Determine base_card_code from transaction (if available) or derive from canonical code prefix
            base_card = getattr(tx, 'base_card_code', None) or (code.split('_')[0] if '_' in code else code)
            # Compose full address from parts (may be None if address info is missing)
            address_parts = [getattr(tx, 'address', None), getattr(tx, 'city', None), getattr(tx, 'state', None), getattr(tx, 'zipcode', None)]
            full_address = ' '.join([str(p).strip() for p in address_parts if p]).strip() if any(address_parts) else None
            # Last purchase date and amount come from latest transaction
            last_purchase_date = getattr(tx, 'posting_date', None)
            last_purchase_amount = getattr(tx, 'revenue', None)

            new_ap_records.append({
                'canonical_code': code,
                'base_card_code': base_card,
                'ship_to_code': getattr(tx, 'ship_to_code', None),
                'name': getattr(tx, 'name', '') or '',
                'full_address': full_address,
                'customer_id': getattr(tx, 'customer_id', None),
                'sales_rep': getattr(tx, 'sales_rep', None),
                'sales_rep_name': getattr(tx, 'sales_rep_name', None),
                'distributor': getattr(tx, 'distributor', None),
                'last_purchase_date': last_purchase_date,
                'last_purchase_amount': float(last_purchase_amount) if last_purchase_amount is not None else None,
                'avg_interval_py': None,
                'created_at': datetime.utcnow(),
                'updated_at': datetime.utcnow(),
            })

        # Insert new AccountPrediction rows with upsert semantics
        if new_ap_records:
            print(f"Inserting {len(new_ap_records)} new AccountPrediction records…")
            stmt = pg_insert(AccountPrediction).values(new_ap_records)
            stmt = stmt.on_conflict_do_nothing(index_elements=['canonical_code'])
            session.execute(stmt)
            session.commit()
            print("AccountPrediction seeding complete.")
        else:
            print("No new AccountPrediction records needed.")

        # Step 4: Seed AccountHistoricalRevenue for any missing (canonical_code, year) pairs
        print("Aggregating historical revenue across all years and accounts…")
        trans_agg_query = (
            select(
                Transaction.canonical_code,
                extract('year', Transaction.posting_date).label('year'),
                func.sum(Transaction.revenue).label('total_rev'),
                func.count(Transaction.id).label('trans_count')
            )
            .where(Transaction.canonical_code.isnot(None))
            .group_by(Transaction.canonical_code, extract('year', Transaction.posting_date))
        )
        trans_agg_results = session.execute(trans_agg_query).all()

        # Track new historical rows and updates
        new_hist_records = []
        updated_hist_records = 0

        for canonical_code, year, total_rev, trans_count in trans_agg_results:
            # Check if this (canonical_code, year) exists
            hist = session.query(AccountHistoricalRevenue).filter_by(canonical_code=canonical_code, year=int(year)).one_or_none()
            if hist:
                # Update existing record to match aggregated totals
                hist.total_revenue = float(total_rev)
                hist.transaction_count = int(trans_count)
                updated_hist_records += 1
            else:
                # Build a new historical revenue row
                # Use base_card_code and ship_to_code from canonical code pattern if not present
                base_card = canonical_code.split('_')[0] if '_' in canonical_code else canonical_code
                ship_to = None
                if '_' in canonical_code:
                    # if more than two parts, the second part might be ship_to
                    parts = canonical_code.split('_', 2)
                    if len(parts) >= 2:
                        ship_to = parts[1]
                new_hist_records.append({
                    'canonical_code': canonical_code,
                    'base_card_code': base_card,
                    'ship_to_code': ship_to,
                    'year': int(year),
                    'total_revenue': float(total_rev),
                    'transaction_count': int(trans_count),
                    'name': None,
                    'sales_rep': None,
                    'distributor': None,
                    'yearly_products_json': None,
                })

        # Bulk insert new historical revenue rows
        if new_hist_records:
            print(f"Inserting {len(new_hist_records)} new AccountHistoricalRevenue records…")
            session.bulk_insert_mappings(AccountHistoricalRevenue, new_hist_records)
        if updated_hist_records > 0:
            print(f"Updated {updated_hist_records} existing AccountHistoricalRevenue records.")
        session.commit()
        print("Historical revenue seeding/update complete.")

        # Step 5: Run a full recalculation to populate metrics and health scores for all accounts
        print("Running full recalculation of account metrics… this may take a while.")
        predictions_df = recalculate_predictions_and_metrics()
        if predictions_df is None or predictions_df.empty:
            print("Recalculation returned no data; aborting update to avoid overwriting.")
            return

        # Convert DataFrame to list of dicts and perform a bulk update
        update_records = predictions_df.replace({pd.NaT: None}).to_dict('records')
        session.bulk_update_mappings(AccountPrediction, update_records)
        session.commit()
        print(f"Recalculation complete. {len(update_records)} AccountPrediction records updated.")

        print("Catch‑up routine finished successfully.")


if __name__ == '__main__':
    try:
        perform_catchup()
    except Exception as e:
        print(f"Catch‑up script encountered an error: {e}")