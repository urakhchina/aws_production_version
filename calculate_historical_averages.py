# calculate_historical_averages.py

import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
import os
import sys
import logging
from datetime import datetime

# --- Basic Logging Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Configuration ---
# Get Database URI from environment variable or use a default
# IMPORTANT: Replace 'sqlite:///data/sales_reminder.db' with your actual default if needed
DATABASE_URI = os.environ.get('SQLALCHEMY_DATABASE_URI', 'sqlite:///data/sales_reminder.db')
OUTPUT_CSV_FILE = f"historical_account_averages_{datetime.now().strftime('%Y%m%d')}.csv"

def calculate_averages():
    """
    Connects to the database, fetches all transactions, calculates historical
    average daily order value, and saves to CSV.
    """
    logger.info(f"Connecting to database: {DATABASE_URI}")
    try:
        engine = create_engine(DATABASE_URI)
        with engine.connect() as connection:
            logger.info("Fetching all transactions...")
            # Fetch necessary columns from the transactions table
            # No YEAR filter here - we want all history
            query = text("""
                SELECT
                    canonical_code,
                    posting_date,
                    revenue
                FROM transactions
                WHERE revenue IS NOT NULL AND posting_date IS NOT NULL
                ORDER BY canonical_code, posting_date
            """)
            transactions_df = pd.read_sql(query, connection)
            logger.info(f"Fetched {len(transactions_df)} transaction records.")

        if transactions_df.empty:
            logger.warning("No transactions found in the database.")
            return

        # --- Data Processing ---
        logger.info("Processing transaction data...")

        # Ensure correct data types
        transactions_df['posting_date'] = pd.to_datetime(transactions_df['posting_date'], errors='coerce')
        transactions_df['revenue'] = pd.to_numeric(transactions_df['revenue'], errors='coerce')

        # Drop rows where conversion failed
        initial_rows = len(transactions_df)
        transactions_df.dropna(subset=['posting_date', 'revenue'], inplace=True)
        if len(transactions_df) < initial_rows:
            logger.warning(f"Dropped {initial_rows - len(transactions_df)} rows due to conversion errors (date/revenue).")

        if transactions_df.empty:
            logger.warning("No valid transactions remaining after cleaning.")
            return

        # 1. Calculate TOTAL revenue per DAY for each account
        logger.info("Calculating daily totals...")
        # Normalize date to remove time component before grouping
        transactions_df['order_date'] = transactions_df['posting_date'].dt.normalize()
        daily_totals_df = transactions_df.groupby(['canonical_code', 'order_date'], as_index=False)['revenue'].sum()
        daily_totals_df.rename(columns={'revenue': 'daily_total'}, inplace=True)
        logger.info(f"Calculated {len(daily_totals_df)} daily total records.")

        # 2. Calculate average and other stats per account based on DAILY totals
        logger.info("Calculating historical averages per account...")
        account_summary_df = daily_totals_df.groupby('canonical_code').agg(
            average_daily_order_value=('daily_total', 'mean'),
            median_daily_order_value=('daily_total', 'median'), # Median is less sensitive to outliers
            total_historical_revenue=('daily_total', 'sum'),
            number_of_order_days=('order_date', 'count'), # Count distinct days with orders
            first_order_date=('order_date', 'min'),
            last_order_date=('order_date', 'max')
        ).reset_index()

        # Format for readability
        account_summary_df['average_daily_order_value'] = account_summary_df['average_daily_order_value'].round(2)
        account_summary_df['median_daily_order_value'] = account_summary_df['median_daily_order_value'].round(2)
        account_summary_df['total_historical_revenue'] = account_summary_df['total_historical_revenue'].round(2)

        # Add days since last order
        today = pd.Timestamp.now().normalize()
        account_summary_df['days_since_last_order'] = (today - account_summary_df['last_order_date']).dt.days

        logger.info(f"Calculated historical summaries for {len(account_summary_df)} accounts.")

        # --- Save to CSV ---
        logger.info(f"Saving results to {OUTPUT_CSV_FILE}...")
        account_summary_df.to_csv(OUTPUT_CSV_FILE, index=False, encoding='utf-8')
        logger.info("Successfully saved historical averages to CSV.")

    except Exception as e:
        logger.error(f"An error occurred: {e}", exc_info=True)

if __name__ == "__main__":
    logger.info("--- Starting Historical Account Average Calculation ---")
    calculate_averages()
    logger.info("--- Calculation Finished ---")