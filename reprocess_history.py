#!/usr/bin/env python
"""
Historical Data Reprocessing Script for ShipTo Integration

Reads raw historical data, generates canonical codes based on ShipTo (or fallback),
re-aggregates metrics, calculates initial predictions, optionally truncates target tables,
and populates the database with clean, canonical data including a detailed transaction log.

Usage:
    python reprocess_history.py <path_to_raw_data.csv> [--db-uri <database_connection_string>] [--chunksize <rows>] [--start-fresh]

Example:
    # Run locally, wiping tables first, using default config DB URI
    python reprocess_history.py data/raw/Your_Consolidated_Historical_Data.csv --start-fresh

    # Run locally, appending (if tables already exist), using specific DB URI
    python reprocess_history.py data/raw/Your_Consolidated_Historical_Data.csv --db-uri sqlite:///data/another_test.db
"""

import argparse
import os
import re
import sys
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text, select, func, and_, MetaData, Table
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.exc import IntegrityError, ProgrammingError
from datetime import datetime, timedelta
import time
import json
import logging
import hashlib
import math 

from pipeline import calculate_product_coverage_from_db, calculate_yoy_metrics_from_db

# --- Logging Setup ---
#log_file = f"reprocess_history_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
log_file = f"/tmp/reprocess_history_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(module)s:%(lineno)d] - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler(sys.stdout) # Also print to console
    ]
)
logger = logging.getLogger(__name__)

# --- Import Models, Config, and Pipeline Functions ---
# Add project root to path to find modules if running script directly
project_root = os.path.dirname(os.path.abspath(__file__))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

try:
    # Import DB object and models directly for table reflection/metadata
    from models import AccountPrediction, AccountHistoricalRevenue, AccountSnapshot, Transaction
    import config # Import config for default DB URI
    # Import necessary functions from pipeline
    from pipeline import ( aggregate_item_codes, safe_json_dumps, transform_days_overdue,
                           calculate_rfm_scores, calculate_health_score,
                           calculate_enhanced_priority_score, safe_float, safe_int, normalize_address, normalize_store_name, get_base_card_code )
    logger.info("Successfully imported models, config, and pipeline functions.")
except ImportError as e:
    logger.error(f"Failed to import necessary modules (models, config, pipeline): {e}", exc_info=True)
    logger.error("Ensure this script is run from the project root with venv active or necessary paths are set.")
    sys.exit(1)
except Exception as e_gen:
    logger.error(f"An unexpected error occurred during imports: {e_gen}", exc_info=True)
    sys.exit(1)

# --- Constants ---
DEFAULT_CHUNK_SIZE = 50000

# === Normalization & Key Generation Functions ===

def normalize_store_name(name):
    """Normalize store names for better matching (Copied from store_normalization.py)."""
    if not name or pd.isna(name): return ""
    name = str(name).upper().strip()

    # Remove common prefixes
    prefixes = ["THE "]
    for prefix in prefixes:
        if name.startswith(prefix): name = name[len(prefix):]

    # Replace common variations
    replacements = {
        " & ": " AND ", "#": " ", "NO.": " ", "MRKT": "MARKET", "MKT": "MARKET",
        "HLTH": "HEALTH", "NATRL": "NATURAL", "NUTR": "NUTRITION", "NUTRITN": "NUTRITION",
        "CTR": "CENTER", "CNTR": "CENTER", "FARMS": "FARMERS", "PATCH": "PATCH",
        "WHEATERY": "WHEATERY", "'S": "S", "-": " ", "_": " ", ",": " ", ".": " "
    }
    for old, new in replacements.items(): name = name.replace(old, new)

    # Remove common suffixes
    suffixes = [" INC", " LLC", " CO", " MARKET", " FOODS", " 1", " 2"] # Add store numbers if needed
    for suffix in suffixes:
        if name.endswith(suffix): name = name[:-len(suffix)]

    # Remove numbers at the end and store numbers like #XXX
    name = re.sub(r'\s+\d+$', '', name)
    name = re.sub(r'#\s*\d+', '', name)

    # Normalize whitespace
    name = ' '.join(name.split())
    return name

def normalize_address(row):
    """Normalize address components for better matching (Adapted from store_normalization.py)."""
    try:
        # Extract components from row - MATCH YOUR RAW CSV COLUMN NAMES
        addr_str = str(row.get('ADDRESS', '') or '').strip()
        city = str(row.get('CITY', '') or '').strip()
        state = str(row.get('STATE', '') or '').strip()
        zipcode = str(row.get('ZIPCODE', '') or '').strip()

        # Combine components first if they exist
        address_parts = [part for part in [addr_str, city, state, zipcode] if part]
        if not address_parts: return "NO_ADDRESS"
        addr = " ".join(address_parts).upper() # Combine with space, make uppercase

        # 1. PO Box Check
        if re.search(r'P\.?\s*O\.?\s*BOX', addr):
            po_match = re.search(r'P\.?\s*O\.?\s*BOX\s*(\d+)', addr)
            return f"PO BOX {po_match.group(1)}" if po_match else "PO BOX"

        # 2. Handle "ADDRESS NOT AVAILABLE" variations
        if "ADDRESS NOT AVAILABLE" in addr or "NOT AVAILABLE" in addr:
            return "NO_ADDRESS"
        # Check if address is just city/state/zip after combining
        # (This requires careful thought - maybe allow if name is distinct?)
        # For hashing, we need something unique per location. If only city/state/zip provided,
        # maybe hash that combo? Or rely on name hash as last resort.
        if not addr_str and city and state: # If original street address was missing
             logger.debug(f"Address missing street, using City/State/Zip for norm: {city} {state} {zipcode}")
             # Fall through to general cleaning for city/state/zip

        # 3. Initial punctuation/whitespace cleanup
        addr = addr.replace(',', ' ').replace('.', ' ')
        addr = re.sub(r'\s+', ' ', addr).strip()

        # 4. Normalize common street type abbreviations
        street_types = {
            r'\bST\b|\bSTREET\b|\bSTR\b': 'ST',
            r'\bRD\b|\bROAD\b': 'RD',
            r'\bDR\b|\bDRIVE\b|\bDRVE\b': 'DR',
            r'\bBLVD\b|\bBOULEVARD\b|\bBLVDN\b': 'BLVD',
            r'\bPKWY\b|\bPARKWAY\b': 'PKWY',
            r'\bCIR\b|\bCIRCLE\b': 'CIR',
            r'\bHWY\b|\bHIGHWAY\b': 'HWY',
            r'\bLN\b|\bLANE\b': 'LN',
            r'\bCT\b|\bCOURT\b': 'CT',
            r'\bTER\b|\bTERRACE\b': 'TER',
            r'\bPL\b|\bPLACE\b': 'PL',
        }
        for _ in range(2):
            for pattern, replacement in street_types.items(): addr = re.sub(pattern, replacement, addr)

        # 5. Standardize directionals
        directions = {
            r'\bN\b|\bNORTH\b|\bNTH\b': 'N',
            r'\bS\b|\bSOUTH\b|\bSTH\b': 'S',
            r'\bE\b|\bEAST\b': 'E',
            r'\bW\b|\bWEST\b|\bWST\b': 'W',
            r'\bNW\b|\bNORTHWEST\b': 'NW',
            r'\bSW\b|\bSOUTHWEST\b': 'SW',
            r'\bNE\b|\bNORTHEAST\b': 'NE',
            r'\bSE\b|\bSOUTHEAST\b': 'SE',
        }
        for pattern, replacement in directions.items(): addr = re.sub(pattern + r'\b', replacement, addr)

        # 6. Remove unit/suite/apt/floor/etc. designations
        addr = re.sub(r'\s+(?:UNIT|STE|SUITE|APT|APARTMENT|FL|FLOOR|ROOM|RM|DEPT|#)\s*([A-Z0-9\-]+)\b', '', addr, flags=re.IGNORECASE)
        addr = re.sub(r'\s+(?:UNIT|STE|SUITE|APT|APARTMENT|FL|FLOOR|ROOM|RM|DEPT|#)\b', '', addr, flags=re.IGNORECASE)

        # 7. Fix specific OCR/Spelling issues
        spelling_fixes = {
            'SHINGTON': 'WASHINGTON',
            'LVER RING': 'SILVER SPRING',
            'LVER GS': 'SILVER SPRING',
            'LNUT': 'WALNUT',
            'DEMP': 'DEMPSTER',
            'YNE': 'WAYNE',
            'GANBIER': 'GAMBIER',
            'GEMONT': 'EGEMONT',
            'AUL POWDER': 'AUSTELL POWDER',
            'MOUNT HOOD': 'MT HOOD',
            'BAY VIEW': 'BAYVIEW',
            'OY CREEK': 'JOY CREEK',
            'MC CULLOCH': 'MCCULLOCH',
            'DUCKETT': 'DUCKETT',
            'CKTON': 'ROCKTON',
        }
        for misspelled, correct in spelling_fixes.items(): addr = addr.replace(misspelled, correct)

        # 8. Clean up extra details often added after address components
        addr = re.sub(r'\b(?:[A-Z]{2})\s+\d{5}(?:-\d{4})?$', '', addr).strip() # Remove state/zip if at end
        addr = re.sub(r'\b(?:' + '|'.join([city.split()[0] for part in city.split() if len(part)>2]) + r')\b', '', addr).strip() if city else addr # Attempt removing city name if present


        # 9. Final Cleanup: Remove all non-alphanumeric (keep space, hyphen), normalize ws
        addr = re.sub(r'[^\w\s\-]', '', addr)
        addr = re.sub(r'\s+', ' ', addr).strip()

        if not addr: return "NO_ADDRESS"
        return addr

    except Exception as e:
        error_context = f"ADDRESS='{row.get('ADDRESS', '')}', CITY='{row.get('CITY', '')}', STATE='{row.get('STATE', '')}', ZIP='{row.get('ZIPCODE', '')}'"
        logger.warning(f"Error normalizing address components ({error_context}): {e}", exc_info=False)
        return "NORM_ERROR"

def get_base_card_code(card_code):
    """Extracts the base part of a CardCode."""
    if not card_code or not isinstance(card_code, str): return ""
    base = re.split(r'[_\s-]+', card_code.strip(), 1)[0]
    return base

def generate_canonical_code(row):
    """
    Generates the canonical code based on ShipTo or fallback logic.
    Expects row to contain pre-calculated 'base_card_code' and original
    'ShipTo', 'NAME', 'ADDRESS', 'CITY', 'STATE', 'ZIPCODE'.
    """
    base_code = str(row.get('base_card_code', '')).strip() # <<< USE PRE-CALCULATED BASE CODE
    ship_to_col_name = 'ShipTo' if 'ShipTo' in row else 'SHIPTO' # Handle potential case diff
    ship_to = str(row.get(ship_to_col_name, '') or '').strip()

    if not base_code:
        logger.warning("Cannot generate canonical code: Missing base_card_code in input row.")
        return None

    # --- Strategy 1: Use ShipTo if valid ---
    if ship_to and ship_to.lower() not in ['', 'nan', 'none', 'null', '0']:
        clean_ship_to = re.sub(r'[^\w\-]+', '', ship_to).upper()
        if clean_ship_to:
            return f"{base_code}_{clean_ship_to}"
        else:
            logger.debug(f"ShipTo code '{ship_to}' empty after cleaning for base {base_code}. Falling back.")

    # --- Strategy 2: Fallback using Normalized Address ---
    norm_address = normalize_address(row) # Assumes normalize_address takes the row
    if norm_address and norm_address not in ["NO_ADDRESS", "NORM_ERROR"]:
        address_hash = hashlib.sha1(norm_address.encode('utf-8')).hexdigest()[:12]
        return f"{base_code}_LOC_{address_hash}"

    # --- Strategy 3: Last resort fallback (Name Hash) ---
    norm_name = normalize_store_name(str(row.get('NAME', '')).strip())
    if norm_name:
        name_hash = hashlib.sha1(norm_name.encode('utf-8')).hexdigest()[:12]
        logger.warning(f"Using Name Hash fallback for {base_code} (Name: '{norm_name}'): NAME_{name_hash}")
        return f"{base_code}_NAME_{name_hash}"

    logger.error(f"Cannot generate unique canonical code for base {base_code}: Missing ShipTo, Address, and Name.")
    return None


# === Data Processing Functions ===

def process_chunk(chunk_df):
    """
    Cleans raw data chunk, generates canonical identifiers, and prepares for aggregation/loading.
    Returns a DataFrame with essential columns including canonical_code, base_card_code, etc.
    """
    logger.debug(f"Processing chunk with {len(chunk_df)} rows...")

    # 1. Define expected raw columns (Match YOUR CSV header exactly)
    # Distributor | POSTINGDATE | CUSTOMERID | NAME | ADDRESS | CITY | STATE | ZIPCODE | ITEM | DESCRIPTION | QUANTITY | AMOUNT | CardCode | CardName | SalesRep | SlpName | Manager | ShipTo
    expected_raw_cols = [
        'CardCode', 'ShipTo', 'NAME', 'ADDRESS', 'CITY', 'STATE', 'ZIPCODE',
        'POSTINGDATE', 'AMOUNT', 'QUANTITY', 'DESCRIPTION', 'SalesRep', 'Distributor',
        'ITEM', 'CUSTOMERID', 'CardName', 'SlpName', 'Manager'
    ]
    actual_cols = chunk_df.columns.tolist()

    # Handle potential variations in source column names if necessary
    if 'CardCode' not in actual_cols and 'CARD_CODE' in actual_cols:
        chunk_df.rename(columns={'CARD_CODE':'CardCode'}, inplace=True)
        actual_cols = chunk_df.columns.tolist() # Refresh columns
    if 'SlpName' not in actual_cols and 'SigName' in actual_cols:
         chunk_df.rename(columns={'SigName':'SlpName'}, inplace=True)
         actual_cols = chunk_df.columns.tolist()

    # Check for essential columns needed for processing
    essential_cols = ['CardCode', 'POSTINGDATE', 'AMOUNT', 'QUANTITY', 'DESCRIPTION', 'NAME', 'ADDRESS', 'CITY', 'STATE', 'ZIPCODE'] # Added Name/Address components
    if not all(col in actual_cols for col in essential_cols):
        missing = [c for c in essential_cols if c not in actual_cols]
        logger.error(f"Chunk missing essential processing columns: {missing}. Skipping chunk.")
        return None # Indicate failure

    # Add missing optional columns as empty if they don't exist in this specific chunk/file
    for col in expected_raw_cols:
         if col not in chunk_df.columns: chunk_df[col] = ""

    # 2. Basic Cleaning & Type Conversion
    logger.debug("Cleaning data types...")
    chunk_df['CardCode'] = chunk_df['CardCode'].fillna('').astype(str).str.strip()
    chunk_df['ShipTo'] = chunk_df['ShipTo'].fillna('').astype(str).str.strip()
    chunk_df['POSTINGDATE'] = pd.to_datetime(chunk_df['POSTINGDATE'], errors='coerce')
    chunk_df['AMOUNT'] = pd.to_numeric(chunk_df['AMOUNT'], errors='coerce').fillna(0.0)
    chunk_df['QUANTITY'] = pd.to_numeric(chunk_df['QUANTITY'], errors='coerce').fillna(0.0).astype(int) # Assume int qty
    chunk_df['DESCRIPTION'] = chunk_df['DESCRIPTION'].fillna('').astype(str)
    chunk_df['NAME'] = chunk_df['NAME'].fillna('Unknown').astype(str)
    chunk_df['ADDRESS'] = chunk_df['ADDRESS'].fillna('').astype(str)
    chunk_df['CITY'] = chunk_df['CITY'].fillna('').astype(str)
    chunk_df['STATE'] = chunk_df['STATE'].fillna('').astype(str)
    chunk_df['ZIPCODE'] = chunk_df['ZIPCODE'].fillna('').astype(str)
    chunk_df['SalesRep'] = chunk_df['SalesRep'].fillna('').astype(str)
    chunk_df['SlpName'] = chunk_df['SlpName'].fillna('').astype(str)
    chunk_df['Distributor'] = chunk_df['Distributor'].fillna('').astype(str)
    chunk_df['ITEM'] = chunk_df['ITEM'].fillna('').astype(str)
    chunk_df['CUSTOMERID'] = chunk_df['CUSTOMERID'].fillna('').astype(str)

    # Drop rows with invalid essential data BEFORE normalization/key gen
    initial_rows = len(chunk_df)
    chunk_df.dropna(subset=['POSTINGDATE', 'CardCode'], inplace=True)
    chunk_df = chunk_df[chunk_df['CardCode'] != ''] # Ensure CardCode is not empty
    if len(chunk_df) < initial_rows:
        logger.warning(f"Dropped {initial_rows - len(chunk_df)} rows from chunk due to missing Date/CardCode.")

    if chunk_df.empty:
        logger.info("Chunk is empty after initial filtering.")
        return pd.DataFrame() # Return empty DF

    # Calculate derived fields AFTER cleaning
    chunk_df['year'] = chunk_df['POSTINGDATE'].dt.year
    chunk_df['revenue'] = chunk_df['AMOUNT']

    # 3. Generate Canonical Key Components
    logger.debug("Generating base and canonical codes...")
    chunk_df['base_card_code'] = chunk_df['CardCode'].apply(get_base_card_code)
    # Apply generate_canonical_code row-wise. It uses base_card_code, ShipTo, NAME, ADDRESS, etc.
    chunk_df['canonical_code'] = chunk_df.apply(generate_canonical_code, axis=1)

    # Drop rows where canonical code couldn't be generated
    initial_rows = len(chunk_df)
    chunk_df.dropna(subset=['canonical_code'], inplace=True)
    if len(chunk_df) < initial_rows:
        logger.warning(f"Dropped {initial_rows - len(chunk_df)} rows from chunk due to missing canonical code generation.")

    if chunk_df.empty:
        logger.info("Chunk is empty after canonical code generation.")
        return pd.DataFrame()

    logger.debug(f"Finished processing chunk. Returning {len(chunk_df)} rows with canonical codes.")

    # 4. Select and Rename columns needed for downstream tables
    # Define required columns with names matching the MODELS
    output_cols_map = {
        # Original Raw -> Model/Processing Name
        'canonical_code': 'canonical_code',
        'base_card_code': 'base_card_code',
        'ShipTo': 'ship_to_code', # Map raw 'ShipTo' to 'ship_to_code'
        'year': 'year',
        'POSTINGDATE': 'posting_date', # Rename for consistency
        'AMOUNT': 'amount',
        'QUANTITY': 'quantity',
        'revenue': 'revenue',
        'DESCRIPTION': 'description',
        'ITEM': 'item_code', # Rename for clarity/model
        'NAME': 'NAME', # Keep NAME for historical aggregation ('last')
        'ADDRESS': 'ADDRESS', # Keep for historical aggregation ('last')
        'CITY': 'CITY', # Keep for historical aggregation ('last')
        'STATE': 'STATE', # Keep for historical aggregation ('last')
        'ZIPCODE': 'ZIPCODE', # Keep for historical aggregation ('last')
        'SalesRep': 'sales_rep', # Rename to match model
        'SlpName': 'sales_rep_name', # Rename to match model
        'Distributor': 'distributor', # Rename to match model
        'CUSTOMERID': 'customer_id' # Rename to match model
    }

    # Select and rename columns that actually exist in the chunk
    final_cols_df = pd.DataFrame()
    for raw_col, final_col in output_cols_map.items():
        if raw_col in chunk_df.columns:
            final_cols_df[final_col] = chunk_df[raw_col]
        else:
             # Handle missing source columns if necessary (e.g., add None column)
             # For now, assume essential ones were checked earlier
             logger.warning(f"Expected raw column '{raw_col}' not found in chunk.")

    # Verify essential columns are present in the final selection
    essential_final_cols = ['canonical_code', 'posting_date', 'revenue', 'year', 'amount', 'quantity', 'description']
    missing_final = [c for c in essential_final_cols if c not in final_cols_df.columns]
    if missing_final:
        logger.error(f"CRITICAL: Essential columns missing after final selection: {missing_final}. Returning None.")
        return None

    return final_cols_df

def aggregate_historical(all_processed_df):
    """Aggregates by canonical_code and year."""
    logger.info(f"Aggregating historical data for {all_processed_df['canonical_code'].nunique()} canonical codes...")
    if all_processed_df.empty: logger.warning("No processed data to aggregate."); return pd.DataFrame()

    all_processed_df['revenue'] = pd.to_numeric(all_processed_df['revenue'], errors='coerce').fillna(0.0)
    all_processed_df['year'] = pd.to_numeric(all_processed_df['year'], errors='coerce').astype('Int64')
    all_processed_df.dropna(subset=['canonical_code', 'year'], inplace=True)

    agg_funcs = {
        'total_revenue': ('revenue', 'sum'),
        'transaction_count': ('posting_date', 'count'),
        'yearly_products': ('item_code', aggregate_item_codes),
        'name': ('NAME', 'last'), # Use last known name in that year
        'sales_rep': ('sales_rep', 'last'), # Use last known rep ID in that year
        'distributor': ('distributor', 'last'), # Use last known distributor in that year
        'base_card_code': ('base_card_code', 'first'), # Should be constant
        'ship_to_code': ('ship_to_code', 'first'), # Should be constant
    }
    try:
        all_processed_df.sort_values(['canonical_code', 'year', 'posting_date'], inplace=True)
        yearly_agg = all_processed_df.groupby(['canonical_code', 'year'], as_index=False).agg(**agg_funcs)
    except Exception as agg_err: logger.error(f"Error during aggregation: {agg_err}", exc_info=True); return pd.DataFrame()

    if 'yearly_products' in yearly_agg.columns:
        yearly_agg['yearly_products_json'] = yearly_agg['yearly_products'].apply(safe_json_dumps)
        yearly_agg = yearly_agg.drop(columns=['yearly_products'])
    else: yearly_agg['yearly_products_json'] = None

    logger.info(f"Aggregation complete. Generated {len(yearly_agg)} historical summary rows.")
    return yearly_agg


def calculate_initial_predictions(all_processed_df, historical_agg_df, engine):
    """
    Calculates initial predictions based on full processed history.
    """
    logger.info(f"Calculating initial predictions for {all_processed_df['canonical_code'].nunique()} canonical codes...")
    if all_processed_df.empty or historical_agg_df.empty:
        logger.warning("Missing detailed or aggregated data for predictions.")
        return pd.DataFrame()

    all_processed_df['posting_date'] = pd.to_datetime(all_processed_df['posting_date'], errors='coerce')
    all_processed_df.dropna(subset=['canonical_code', 'posting_date'], inplace=True)

    predictions = []
    processing_end_datetime = all_processed_df['posting_date'].max()
    if pd.isna(processing_end_datetime): logger.error("Could not determine max posting date."); return pd.DataFrame()
    today_for_calc = processing_end_datetime.date() # Use date part for comparisons
    current_year_num = today_for_calc.year
    start_of_current_year = datetime(current_year_num, 1, 1)
    logger.info(f"Using {today_for_calc} as reference date for historical calculations.")

    grouped_detailed = all_processed_df.groupby('canonical_code')
    total_accounts = len(grouped_detailed); processed_count = 0

    for canonical_code, group in grouped_detailed:
        processed_count += 1
        if processed_count % 250 == 0: logger.info(f"Calculating predictions: {processed_count}/{total_accounts}...")

        group = group.sort_values('posting_date')
        purchase_datetimes = pd.to_datetime(group['posting_date'].unique()) # Unique timestamps

        # --- Basic Info ---
        last_known_row = group.iloc[-1]
        name = last_known_row['NAME']
        full_address = f"{last_known_row['ADDRESS']}, {last_known_row['CITY']}, {last_known_row['STATE']} {last_known_row['ZIPCODE']}".strip(', ')
        sales_rep_id = last_known_row['sales_rep']
        sales_rep_name = last_known_row['sales_rep_name']
        distributor = last_known_row['distributor']
        base_card_code = last_known_row['base_card_code']
        ship_to_code = last_known_row['ship_to_code']
        customer_id = last_known_row['customer_id']

        # --- Last Purchase ---
        last_purchase_datetime = purchase_datetimes[-1] if len(purchase_datetimes) > 0 else None
        last_purchase_date = last_purchase_datetime.date() if last_purchase_datetime else None
        last_purchase_amount = group[group['posting_date'] == last_purchase_datetime]['revenue'].sum() if last_purchase_datetime else 0.0

        # --- Lifetime Aggregates ---
        acc_hist_data = historical_agg_df[historical_agg_df['canonical_code'] == canonical_code]
        account_total = acc_hist_data['total_revenue'].sum()
        purchase_frequency = acc_hist_data['transaction_count'].sum() # Total # of transactions/rows

        # --- Interval Calculations ---
        median_interval_days = 30; avg_interval_cytd = None; avg_interval_py = None
        avg_interval_overall = 30.0 # Default overall average
        intervals_days = pd.Series(dtype=float) # Initialize empty series
        if len(purchase_datetimes) > 1:
            date_series = pd.Series(purchase_datetimes).sort_values()
            intervals_days = date_series.diff().dt.days.dropna()
            if not intervals_days.empty:
                median_float = intervals_days.median()
                if pd.notna(median_float): median_interval_days = max(1, int(float(median_float)))
                avg_interval_overall = intervals_days.mean() # Calculate overall average here
                if avg_interval_overall <=0 or pd.isna(avg_interval_overall): avg_interval_overall = 30.0 # Ensure valid fallback

                # Calc PY Avg
                prev_year_num = current_year_num - 1
                py_dates = date_series[date_series.dt.year == prev_year_num]
                if len(py_dates) > 1:
                    py_intervals = py_dates.diff().dt.days.dropna()
                    if not py_intervals.empty: avg_interval_py = py_intervals.mean()
                # Calc CYTD Avg
                cy_dates = date_series[date_series.dt.year == current_year_num]
                if len(cy_dates) > 1:
                    cy_intervals = cy_dates.diff().dt.days.dropna()
                    if not cy_intervals.empty: avg_interval_cytd = cy_intervals.mean()

        if median_interval_days <= 0: median_interval_days = 30

        # --- Prediction & Overdue ---
        next_expected_purchase_date = None; days_overdue = 0; days_since_last_purchase = 9999
        if last_purchase_datetime:
            next_expected_purchase_date = last_purchase_datetime + timedelta(days=median_interval_days)
            # Days since last relative to the end of the dataset, NOT today's actual date
            days_since_last_purchase = (processing_end_datetime - last_purchase_datetime).days
            if next_expected_purchase_date and next_expected_purchase_date.date() < today_for_calc:
                 days_overdue = (today_for_calc - next_expected_purchase_date.date()).days

        # --- CYTD / YEP / Pace Calculations ---
        # (Keep logic as before - using detailed data)
        cytd_trans = group[group['posting_date'] >= start_of_current_year]
        cytd_revenue = float(cytd_trans['revenue'].sum())  # Ensure this is a float
        cytd_count = len(cytd_trans)
        avg_order_amount_cytd = cytd_revenue / cytd_count if cytd_count > 0 else None

        yep_revenue = None
        pace_vs_ly = None

        # --- FIX: This is the corrected YEP logic for the historical script ---
        # It correctly uses the account's own last purchase date, not a global one.
        if cytd_revenue > 0 and not cytd_trans.empty:
            # Use this account's last purchase date for an accurate time window
            last_cytd_date = cytd_trans['posting_date'].max().date()

            if last_cytd_date >= start_of_current_year.date():
                # Ensure days calculation is always a positive float
                days_for_ytd_accumulation = float((last_cytd_date - start_of_current_year.date()).days + 1)
                
                if days_for_ytd_accumulation > 0:
                    yep_revenue = (cytd_revenue / days_for_ytd_accumulation) * 365
                else:
                    logger.warning(f"Account {canonical_code} had 0 days for YTD accumulation. YEP not calculated.")
        # --- END FIX ---
        
        py_total_revenue = float(acc_hist_data[acc_hist_data['year'] == (current_year_num - 1)]['total_revenue'].sum())
        
        # Calculate pace vs last year using the corrected YEP
        if yep_revenue is not None:
            pace_vs_ly = yep_revenue - py_total_revenue
        
        # Optional: Add a debug log to see the values during processing
        logger.debug(
            f"Reprocess Predictions for {canonical_code}: "
            f"CYTD={cytd_revenue:.2f}, YEP={yep_revenue:.2f if yep_revenue is not None else 'N/A'}, "
            f"PY_Revenue={py_total_revenue:.2f}, Pace={pace_vs_ly:.2f if pace_vs_ly is not None else 'N/A'}"
        )

        # --- Latest Products ---
        latest_hist_row = acc_hist_data.sort_values('year', ascending=False).iloc[0] if not acc_hist_data.empty else None
        products_purchased_json = latest_hist_row['yearly_products_json'] if latest_hist_row is not None else json.dumps([])

        # --- Assemble Prediction Row ---
        pred_row = {
            'canonical_code': canonical_code, 'base_card_code': base_card_code, 'ship_to_code': ship_to_code,
            'name': name, 'full_address': full_address, 'customer_id': customer_id,
            'sales_rep': sales_rep_id, 'sales_rep_name': sales_rep_name, 'distributor': distributor,
            'last_purchase_date': last_purchase_datetime, 'last_purchase_amount': last_purchase_amount,
            'account_total': account_total, 'purchase_frequency': purchase_frequency,
            'days_since_last_purchase': days_since_last_purchase,
            'median_interval_days': median_interval_days,
            'next_expected_purchase_date': next_expected_purchase_date,
            'days_overdue': days_overdue,
            'avg_interval_py': avg_interval_py, 'avg_interval_cytd': avg_interval_cytd,
            'cytd_revenue': cytd_revenue, 'yep_revenue': yep_revenue, 'pace_vs_ly': pace_vs_ly,
            'avg_order_amount_cytd': avg_order_amount_cytd,
            'products_purchased': products_purchased_json,
            # Placeholders for scores calculated next
            'recency_score': 0, 'frequency_score': 0, 'monetary_score': 0, 'rfm_score': 0.0, 'rfm_segment': '',
            'health_score': 0.0, 'health_category': '', 'priority_score': 0.0, 'enhanced_priority_score': 0.0,
            #'yoy_revenue_growth': 0.0, 'yoy_purchase_count_growth': 0.0, # Calculated later
            #'product_coverage_percentage': 0.0, 'carried_top_products_json': None, 'missing_top_products_json': None # Calculated later
        }
        predictions.append(pred_row)

    logger.info(f"Finished initial metric calculations for {len(predictions)} accounts.")
    if not predictions: return pd.DataFrame()

    predictions_df = pd.DataFrame(predictions)
    # Explicitly check if 'canonical_code' exists after creation
    if 'canonical_code' not in predictions_df.columns:
        logger.error("CRITICAL: 'canonical_code' column missing after assembling predictions list.")
        # Handle this error - maybe return empty or try to recover if possible
        return pd.DataFrame()
    logger.info(f"Assembled initial predictions DataFrame with shape: {predictions_df.shape}")

    # ... after creating predictions_df from list ...
    logger.info(f"Columns AFTER creating predictions_df: {predictions_df.columns.tolist()}")
    if 'canonical_code' not in predictions_df.columns: logger.error("MISSING canonical_code AFTER DF CREATION!")

    # --- Calculate Scores ---
    logger.info("Calculating scores (RFM, Health, Priority)...")
    try:
        # Ensure inputs exist and fill NaNs appropriately for scorers
        score_input_cols = ['account_total', 'purchase_frequency', 'days_since_last_purchase', 'median_interval_days', 'days_overdue', 'health_score', 'pace_vs_ly', 'yep_revenue'] # Add all required by scorers
        for col in score_input_cols:
            if col not in predictions_df.columns: predictions_df[col] = 0.0 if 'revenue' in col or 'total' in col or 'pace' in col or 'score' in col else 0 # Sensible defaults
            predictions_df[col] = pd.to_numeric(predictions_df[col], errors='coerce').fillna(0) # Fill remaining numeric NaNs with 0

        logger.info(f"Columns BEFORE calculate_rfm_scores: {predictions_df.columns.tolist()}")
        predictions_df = calculate_rfm_scores(predictions_df.copy())
        logger.info(f"Columns AFTER calculate_rfm_scores: {predictions_df.columns.tolist()}")
        if 'canonical_code' not in predictions_df.columns: logger.error("MISSING canonical_code AFTER RFM!")

        logger.info(f"Columns BEFORE calculate_health_score: {predictions_df.columns.tolist()}")
        predictions_df = calculate_health_score(predictions_df.copy())
        logger.info(f"Columns AFTER calculate_health_score: {predictions_df.columns.tolist()}")
        if 'canonical_code' not in predictions_df.columns: logger.error("MISSING canonical_code AFTER HEALTH!")
        
        logger.info(f"Columns in predictions_df BEFORE calling enhanced_priority: {predictions_df.columns.tolist()}")
        predictions_df = calculate_enhanced_priority_score(predictions_df.copy())
        logger.info(f"Columns AFTER calculate_enhanced_priority_score: {predictions_df.columns.tolist()}")
        if 'canonical_code' not in predictions_df.columns: logger.error("MISSING canonical_code AFTER ENH PRIORITY!")
        
        # Calculate original priority score
        if all(c in predictions_df.columns for c in ['days_overdue', 'account_total', 'purchase_frequency']):
            predictions_df['overdue_component'] = predictions_df['days_overdue'].apply(transform_days_overdue)
            w1, w2, w3 = 1.0, 0.001, 1.0
            predictions_df['priority_score'] = (w1 * predictions_df['overdue_component'].fillna(0) + w2 * predictions_df['account_total'].fillna(0) + w3 * predictions_df['purchase_frequency'].fillna(0))
        else: predictions_df['priority_score'] = 0.0

    except Exception as score_err: logger.error(f"Error calculating scores: {score_err}", exc_info=True)

    # --- Calculate Final YoY and Product Coverage ---
    # (Keep logic as corrected before - using temporary session to call helpers)
    logger.info("Calculating final YoY and Product Coverage...")
    engine_for_helpers = create_engine(getattr(config, 'SQLALCHEMY_DATABASE_URI'))
    #SessionLocal = sessionmaker(bind=engine_for_helpers)
    SessionLocal = sessionmaker(bind=engine) # Use passed engine
    final_yoy = pd.DataFrame(columns=['canonical_code', 'yoy_revenue_growth', 'yoy_purchase_count_growth'])
    final_coverage = pd.DataFrame(columns=['canonical_code', 'product_coverage_percentage', 'carried_top_products_json', 'missing_top_products_json'])
    try:
        with SessionLocal() as temp_session:
             final_yoy = calculate_yoy_metrics_from_db(current_year_num, session=temp_session)
             final_coverage = calculate_product_coverage_from_db(session=temp_session)
             logger.info(f"YoY DF Shape: {final_yoy.shape if not final_yoy.empty else 'Empty'}")
             logger.info(f"Coverage DF Shape: {final_coverage.shape if not final_coverage.empty else 'Empty'}")
             if not final_yoy.empty: logger.info(f"Sample YoY Data:\n{final_yoy.head().to_string()}")

    except Exception as helper_err: 
        logger.error(f"Error calling final metric helpers: {helper_err}", exc_info=True)
        final_yoy = pd.DataFrame(columns=['canonical_code', 'yoy_revenue_growth', 'yoy_purchase_count_growth'])
        final_coverage = pd.DataFrame(columns=['canonical_code', 'product_coverage_percentage', 'carried_top_products_json', 'missing_top_products_json'])


    # --- Merge results ---
    # *** LOGGING POINT 2 ***
    logger.info(f"Columns in predictions_df BEFORE merges: {predictions_df.columns.tolist()}")
    if 'yoy_revenue_growth' in predictions_df.columns: logger.warning("YoY columns already exist before merge!")

    if final_yoy is not None and not final_yoy.empty:
        # Check for column conflicts BEFORE merge
        common_cols = predictions_df.columns.intersection(final_yoy.columns).tolist()
        on_col = 'canonical_code'
        if on_col not in common_cols: logger.error("Merge key 'canonical_code' missing in one of the DFs for YoY!"); raise ValueError("Merge key missing")
        other_common = [c for c in common_cols if c != on_col]
        if other_common: logger.warning(f"Potential overlapping columns in YoY merge (excluding key): {other_common}. Using left DF's values.")

        # Perform merge
        predictions_df = pd.merge(predictions_df, final_yoy, on='canonical_code', how='left') # No suffixes needed if no overlap other than key
        logger.info("YoY metrics merged.")
        # *** LOGGING POINT 3 ***
        if 'yoy_revenue_growth' in predictions_df.columns:
            check_val = predictions_df.loc[predictions_df['canonical_code'] == '02AK1444_NATURALPANTRY2', 'yoy_revenue_growth']
            logger.info(f"YoY Growth for 02AK1444 AFTER YOY merge: {check_val.iloc[0] if not check_val.empty else 'Not Found or NaN'}")
        else: logger.error("YoY columns missing AFTER merge!")

    if final_coverage is not None and not final_coverage.empty:
        # ... (similar merge logic for coverage) ...
        predictions_df = pd.merge(predictions_df, final_coverage, on='canonical_code', how='left')
        logger.info("Product coverage merged.")

    # --- Log state BEFORE fillna ---
    logger.info("State of YoY columns BEFORE fillna:")
    if 'yoy_revenue_growth' in predictions_df.columns:
        logger.info(f"  YoY Rev Growth Head:\n{predictions_df[['canonical_code', 'yoy_revenue_growth']].head(15).to_string()}")
        logger.info(f"  YoY Rev Growth NaNs: {predictions_df['yoy_revenue_growth'].isnull().sum()}")
        logger.info(f"  YoY Rev Growth for 02AK1444: {predictions_df.loc[predictions_df['canonical_code'] == '02AK1444_NATURALPANTRY2', 'yoy_revenue_growth'].iloc[0] if not predictions_df[predictions_df['canonical_code'] == '02AK1444_NATURALPANTRY2'].empty else 'Not Found'}")
    else: logger.error("yoy_revenue_growth column MISSING before fillna!")
    if 'yoy_purchase_count_growth' in predictions_df.columns:
         logger.info(f"  YoY Purch Count Growth NaNs: {predictions_df['yoy_purchase_count_growth'].isnull().sum()}")
    # --- End Log state BEFORE fillna ---

    # --- More Selective Fill NaNs ---
    logger.info("Applying final fillna selectively...")
    fill_final = {
        'yoy_revenue_growth': 0.0, 'yoy_purchase_count_growth': 0.0,
        'product_coverage_percentage': 0.0, 'carried_top_products_json': json.dumps([]),
        'missing_top_products_json': json.dumps([])
    }
    for col, default_val in fill_final.items():
         if col in predictions_df.columns:
             nan_count_before = predictions_df[col].isnull().sum()
             if nan_count_before > 0: logger.debug(f"Filling {nan_count_before} NaNs in '{col}' with default")
             # *** LOGGING POINT 4 ***
             if col == 'yoy_revenue_growth': logger.debug(f"YoY Growth for 02AK1444 BEFORE fillna: {predictions_df.loc[predictions_df['canonical_code'] == '02AK1444_NATURALPANTRY2', col].iloc[0] if not predictions_df[predictions_df['canonical_code'] == '02AK1444_NATURALPANTRY2'].empty else 'Not Found'}")
             predictions_df[col] = predictions_df[col].fillna(value=default_val) # Corrected fillna assignment
             if col == 'yoy_revenue_growth': logger.debug(f"YoY Growth for 02AK1444 AFTER fillna: {predictions_df.loc[predictions_df['canonical_code'] == '02AK1444_NATURALPANTRY2', col].iloc[0] if not predictions_df[predictions_df['canonical_code'] == '02AK1444_NATURALPANTRY2'].empty else 'Not Found'}")
         else:
             logger.warning(f"Column '{col}' missing after merge, adding with default.")
             predictions_df[col] = default_val
    # --- End Selective Fill NaNs ---

    # Removed redundant merge here (it was duplicated in the original code)

    logger.info("Finished all initial prediction calculations.")
    return predictions_df


def populate_database(engine, historical_df, predictions_df, transaction_df, start_fresh=False):
    """
    Populates database tables in memory-efficient chunks, optionally clearing them first.
    This version is designed to handle very large datasets without timing out.
    """
    metadata = MetaData()
    try:
        logger.info("Reflecting target tables from database...")
        metadata.reflect(bind=engine, only=['transactions', 'account_historical_revenues', 'account_predictions'])
        transaction_table = metadata.tables['transactions']
        historical_table = metadata.tables['account_historical_revenues']
        prediction_table = metadata.tables['account_predictions']
    except Exception as reflect_err:
        logger.error(f"Database table reflection error: {reflect_err}", exc_info=True)
        return False

    # --- Step 1: Clear Tables if --start-fresh is used ---
    if start_fresh:
        logger.warning("--- DELETING DATA from target tables (--start-fresh detected) ---")
        with engine.connect() as conn:
            trans = conn.begin()
            try:
                # Delete in reverse order of potential dependencies
                conn.execute(prediction_table.delete())
                conn.execute(historical_table.delete())
                conn.execute(transaction_table.delete())
                trans.commit()
                logger.info("Target tables cleared successfully.")
            except Exception as e:
                logger.error(f"Error during table deletion: {e}", exc_info=True)
                trans.rollback()
                return False

    # --- Step 2: Insert Data in Chunks ---
    chunk_size = 25000  # Process 25,000 rows at a time
    total_inserted_trans = 0
    total_inserted_hist = 0
    total_inserted_pred = 0

    try:
        # 2a: Insert Transactions in Chunks
        if transaction_df is not None and not transaction_df.empty:
            logger.info(f"--- Starting chunked insert for {len(transaction_df)} transactions ---")
            # Prepare DataFrame for insertion once
            trans_model_cols = [c.name for c in transaction_table.columns if c.name != 'id']
            transaction_df_filtered = transaction_df[trans_model_cols].replace({np.nan: None, pd.NaT: None})

            for i in range(0, len(transaction_df_filtered), chunk_size):
                with engine.connect() as conn:
                    trans = conn.begin()
                    chunk = transaction_df_filtered.iloc[i:i + chunk_size]
                    logger.info(f"  Inserting transaction chunk {i//chunk_size + 1}...")
                    chunk_data = chunk.to_dict(orient='records')
                    conn.execute(transaction_table.insert(), chunk_data)
                    trans.commit()
                    total_inserted_trans += len(chunk)
            logger.info(f"--- Finished inserting {total_inserted_trans} transactions ---")

        # 2b: Insert Historical Data (Usually small, but chunking is safe)
        if historical_df is not None and not historical_df.empty:
            logger.info(f"--- Inserting {len(historical_df)} historical records ---")
            hist_model_cols = [c.name for c in historical_table.columns if c.name != 'id']
            historical_data = historical_df[hist_model_cols].replace({np.nan: None, pd.NaT: None}).to_dict(orient='records')
            with engine.connect() as conn:
                trans = conn.begin()
                conn.execute(historical_table.insert(), historical_data)
                trans.commit()
                total_inserted_hist = len(historical_data)
            logger.info(f"--- Finished inserting {total_inserted_hist} historical records ---")

        # 2c: Insert Predictions (Usually small, but chunking is safe)
        if predictions_df is not None and not predictions_df.empty:
            logger.info(f"--- Inserting {len(predictions_df)} prediction records ---")
            pred_model_cols = [c.name for c in prediction_table.columns if c.name != 'id']
            prediction_data = predictions_df[pred_model_cols].replace({np.nan: None, pd.NaT: None}).to_dict(orient='records')
            with engine.connect() as conn:
                trans = conn.begin()
                conn.execute(prediction_table.insert(), prediction_data)
                trans.commit()
                total_inserted_pred = len(prediction_data)
            logger.info(f"--- Finished inserting {total_inserted_pred} prediction records ---")
        
        logger.info("All data population stages completed successfully.")
        return True

    except Exception as e:
        logger.error(f"An error occurred during chunked database population: {e}", exc_info=True)
        # The failed chunk's transaction would have been automatically rolled back
        return False



# === Main Execution ===
def main():
    parser = argparse.ArgumentParser(description="Reprocess historical data and repopulate database.")
    parser.add_argument("raw_data_paths", nargs='+', help="Path(s) to the raw historical CSV data file(s).")
    parser.add_argument("--db-uri", default=None, help="Database connection string (overrides config.py).")
    parser.add_argument("--chunksize", type=int, default=DEFAULT_CHUNK_SIZE, help="Rows to process per chunk.")
    parser.add_argument("--start-fresh", action="store_true", help="DELETE data from tables before loading. USE WITH CAUTION!")
    args = parser.parse_args()

    db_uri = args.db_uri
    if not db_uri:
        # Fallback to config if not provided
        try:
            db_uri = config.SQLALCHEMY_DATABASE_URI
        except (AttributeError, NameError):
            logger.error("Database URI not provided via --db-uri and could not be found in config.py. Exiting.")
            sys.exit(1)

    raw_files_exist = [os.path.exists(f) for f in args.raw_data_paths]
    if not all(raw_files_exist):
        missing_files = [f for f, exists in zip(args.raw_data_paths, raw_files_exist) if not exists]
        logger.error(f"Raw data file(s) not found: {missing_files}. Exiting."); sys.exit(1)

    if args.start_fresh:
        logger.warning("!!! --start-fresh flag detected !!!")
        confirm = input("This will DELETE all data from relevant tables. Are you sure? (yes/no): ")
        if confirm.lower() != 'yes': logger.info("Aborting."); sys.exit(0)

    start_time = time.time()
    logger.info(f"--- Starting Historical Reprocessing ---")
    logger.info(f"Sources: {args.raw_data_paths}")
    logger.info(f"Target DB: {db_uri}")
    logger.info(f"Chunk Size: {args.chunksize}")
    logger.info(f"Start Fresh (DELETE): {args.start_fresh}")

    try:
        engine = create_engine(db_uri, connect_args={'connect_timeout': 30})
        logger.info("Database engine created successfully.")
    except Exception as engine_err:
        logger.error(f"Failed to create database engine with URI {db_uri}: {engine_err}", exc_info=True)
        sys.exit(1)

    all_processed_data_list = []; total_raw_rows = 0
    try:
        for file_path in args.raw_data_paths:
            logger.info(f"Processing file: {file_path}...")
            try:
                for i, chunk in enumerate(pd.read_csv(file_path, chunksize=args.chunksize, dtype=str, low_memory=False, encoding='utf-8', on_bad_lines='warn')):
                    logger.info(f"  Processing chunk {i+1} from {os.path.basename(file_path)}...")
                    total_raw_rows += len(chunk)
                    # The process_chunk function now correctly calculates revenue = amount
                    processed_chunk = process_chunk(chunk)
                    if processed_chunk is not None and not processed_chunk.empty:
                        all_processed_data_list.append(processed_chunk)
            except UnicodeDecodeError:
                 logger.warning(f"Encoding error in {file_path}, trying latin-1...")
                 for i, chunk in enumerate(pd.read_csv(file_path, chunksize=args.chunksize, dtype=str, low_memory=False, encoding='latin-1', on_bad_lines='warn')):
                           processed_chunk = process_chunk(chunk)
                           if processed_chunk is not None and not processed_chunk.empty:
                               all_processed_data_list.append(processed_chunk)
            except Exception as e_read:
                 logger.error(f"Failed to read or process chunks from {file_path}: {e_read}", exc_info=True)

        if not all_processed_data_list: 
            logger.error("No data was processed from the input files. Exiting."); sys.exit(1)

        logger.info("Concatenating all processed data chunks...")
        full_processed_df = pd.concat(all_processed_data_list, ignore_index=True)
        logger.info(f"Total valid processed transaction rows: {len(full_processed_df)}")
        del all_processed_data_list

        # --- NEW HASHING LOGIC ---
        logger.info("Calculating deterministic hashes for all historical transactions...")
        duplicate_check_cols = ['canonical_code', 'posting_date', 'item_code', 'revenue', 'quantity']
        
        # Ensure dtypes are correct before sorting and grouping
        full_processed_df['posting_date'] = pd.to_datetime(full_processed_df['posting_date'], errors='coerce')
        for col in ['item_code', 'revenue', 'quantity']:
            # Use .get() to avoid KeyError if a column is missing
            full_processed_df[col] = pd.to_numeric(full_processed_df.get(col), errors='coerce').fillna(0)

        full_processed_df.sort_values(by=duplicate_check_cols, inplace=True, na_position='first')
        full_processed_df['duplicate_rank'] = full_processed_df.groupby(duplicate_check_cols).cumcount()

        def generate_hash(row):
            # This function must be IDENTICAL to the one in the webhook
            unique_string = (f"{row.get('canonical_code', '')}|{row.get('posting_date', '')}|"
                             f"{row.get('item_code', '')}|{row.get('revenue', '')}|{row.get('quantity', '')}|"
                             f"{row.get('duplicate_rank', '')}")
            return hashlib.sha256(unique_string.encode()).hexdigest()

        full_processed_df['transaction_hash'] = full_processed_df.apply(generate_hash, axis=1)
        logger.info("Hashing complete.")
        # --- END OF NEW HASHING LOGIC ---

        logger.info("Aggregating yearly historical data...")
        historical_agg_df = aggregate_historical(full_processed_df)
        if historical_agg_df.empty: 
            logger.error("Historical aggregation failed. Exiting."); sys.exit(1)

        logger.info("Calculating initial predictions...")
        initial_predictions_df = calculate_initial_predictions(full_processed_df, historical_agg_df, engine=engine)
        if initial_predictions_df.empty: 
            logger.error("Initial prediction calculation failed. Exiting."); sys.exit(1)

        # The full_processed_df already contains all necessary columns for the transaction table
        transaction_load_df = full_processed_df

        logger.info("Connecting to database for final population...")
        success = populate_database(engine, historical_agg_df, initial_predictions_df, transaction_load_df, args.start_fresh)

        if success:
            end_time = time.time()
            logger.info(f"--- Reprocessing Complete (Duration: {end_time - start_time:.2f}s) ---")
        else:
            logger.error("Database population failed. Check logs for details.")
            sys.exit(1)

    except Exception as e:
        logger.error(f"A critical error occurred during reprocessing: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()