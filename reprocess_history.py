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
from pipeline import calculate_yearly_revenue_trend
from pipeline import _normalize_upc

# Helper to format currency for growth engine messages (similar to pipeline.py)
def _format_currency(value: float | None) -> str:
    """Format a float value as currency for display. Uses rounding to integer dollars.

    Args:
        value: Numeric value or None.

    Returns:
        A string like "$1,234" or "$0" if None or zero.
    """
    if value is None:
        return "$0"
    try:
        # Use comma separator for thousands and no decimals
        # Round to nearest integer for simplicity
        return f"${value:,.0f}"
    except Exception:
        return str(value)


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
logger.setLevel(logging.DEBUG)

# --- Import Models, Config, and Pipeline Functions ---
# Add project root to path to find modules if running script directly
project_root = os.path.dirname(os.path.abspath(__file__))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

try:
    # Import DB object and models directly for table reflection/metadata
    from models import AccountPrediction, AccountHistoricalRevenue, AccountSnapshot, Transaction
    try:
        import config
        from config import TOP_30_SET, TOP_30_MATCH_SET, is_top_30_product
        logger.info(f"Loaded TOP_30_SET with {len(TOP_30_SET)} products")
    except ImportError as e:
        logger.warning(f"Could not import TOP_30 config: {e}")
        TOP_30_SET = set()
        TOP_30_MATCH_SET = set()
        def is_top_30_product(upc): return False
    # Import necessary functions from pipeline
    from pipeline import ( aggregate_item_codes, safe_json_dumps, transform_days_overdue,
                           calculate_rfm_scores, calculate_health_score,
                           calculate_enhanced_priority_score, safe_float, safe_int, normalize_address, normalize_store_name, get_base_card_code,
                                    _normalize_upc, calculate_yoy_metrics_from_db, calculate_product_coverage_from_db, calculate_yearly_revenue_trend )
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


def _fmt2(v):
    try:
        return f"{float(v):.2f}"
    except (TypeError, ValueError):
        return "null"

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
        'ITEM', 'ITEMUPC', 'CUSTOMERID', 'CardName', 'SlpName', 'Manager'
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
    chunk_df['ITEM'] = chunk_df['ITEM'].fillna('').astype(str).str.strip()
    # Ensure UPC column exists and is a string; if missing, a blank will be added in expected columns above
    if 'ITEMUPC' not in chunk_df.columns:
        logger.warning("[UPC DEBUG] ITEMUPC column missing in this chunk; creating empty.")
        chunk_df['ITEMUPC'] = ''

    logger.debug(f"[UPC DEBUG] Raw ITEMUPC sample (before normalize): {chunk_df['ITEMUPC'].head().tolist()}")
    chunk_df['ITEMUPC'] = (
        chunk_df['ITEMUPC']
        .fillna('')            # avoid NaN inside the normalizer
        .astype(str)
        .str.strip()
        .apply(_normalize_upc)
    )
    logger.debug(f"[UPC DEBUG] Normalized ITEMUPC sample (after normalize): {chunk_df['ITEMUPC'].head().tolist()}")


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
        # Original Raw -> Final DataFrame column names matching the Transaction model
        'canonical_code': 'canonical_code',
        'base_card_code': 'base_card_code',
        'ShipTo': 'ship_to_code',
        'year': 'year',
        'POSTINGDATE': 'posting_date',
        'AMOUNT': 'amount',
        'QUANTITY': 'quantity',
        'revenue': 'revenue',
        'DESCRIPTION': 'description',
        # UPC column (ITEMUPC) holds the canonical SKU
        'ITEMUPC': 'item_code',
        # Store the original distributor item code separately for audit/debugging
        'ITEM': 'distributor_item_code',
        # Normalize names and addresses to lowercase column names for consistency with model
        'NAME': 'name',
        'ADDRESS': 'address',
        'CITY': 'city',
        'STATE': 'state',
        'ZIPCODE': 'zipcode',
        # Sales rep and other IDs
        'SalesRep': 'sales_rep',
        'SlpName': 'sales_rep_name',
        'Distributor': 'distributor',
        'CUSTOMERID': 'customer_id'
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
        # Use lowercase name from processed data
        'name': ('name', 'last'),
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
        # Use lowercase model columns from processed data.
        last_known_row = group.iloc[-1]
        # Store name: prefer CardName fallback processed earlier.
        name = (last_known_row.get('name') or '').strip()
        # Build full address from lowercase columns; gracefully handle missing parts.
        addr = (last_known_row.get('address') or '').strip()
        city = (last_known_row.get('city') or '').strip()
        state = (last_known_row.get('state') or '').strip()
        zipc = (last_known_row.get('zipcode') or '').strip()
        # Combine address parts, omitting empties.
        address_parts = [p for p in [addr, city, state] if p]
        full_address = ", ".join(address_parts)
        if zipc:
            # Append zipcode separated by space if there is an address body, otherwise just set to zipcode.
            full_address = f"{full_address} {zipc}".strip()
        # Extract other details using lowercase column names.
        sales_rep_id = last_known_row.get('sales_rep')
        sales_rep_name = last_known_row.get('sales_rep_name')
        distributor = last_known_row.get('distributor')
        base_card_code = last_known_row.get('base_card_code')
        ship_to_code = last_known_row.get('ship_to_code')
        customer_id = last_known_row.get('customer_id')

        # --- Last Purchase ---
        last_purchase_datetime = purchase_datetimes[-1] if len(purchase_datetimes) > 0 else None
        last_purchase_date = last_purchase_datetime.date() if last_purchase_datetime else None
        last_purchase_amount = group[group['posting_date'] == last_purchase_datetime]['revenue'].sum() if last_purchase_datetime else 0.0

        # --- Lifetime Aggregates ---
        acc_hist_data = historical_agg_df[historical_agg_df['canonical_code'] == canonical_code]
        account_total = acc_hist_data['total_revenue'].sum()
        purchase_frequency = acc_hist_data['transaction_count'].sum() # Total # of transactions/rows

        # Build yearly series for this account: [{'year': 2019, 'revenue': 12345.67}, ...]
        yearly_history_list = (
            acc_hist_data[['year', 'total_revenue']]
            .dropna(subset=['year'])
            .sort_values('year')
            .rename(columns={'total_revenue': 'revenue'})
            .to_dict(orient='records')
        )

        # >>> ADD HERE: PY total and trend <<<
        # Previous-year total revenue (PY)
        py_total_revenue = float(
            acc_hist_data.loc[acc_hist_data['year'] == (current_year_num - 1), 'total_revenue'].sum()
            or 0.0
        )

        # Trend (slope / intercept / R^2) over yearly revenues
        # Ensure you have: from pipeline import calculate_yearly_revenue_trend  (at top of file)
        trend = calculate_yearly_revenue_trend(yearly_history_list)  # dict or None
        if trend:
            revenue_trend_slope      = float(trend.get('slope'))        if trend.get('slope')        is not None else None
            revenue_trend_intercept  = float(trend.get('intercept'))    if trend.get('intercept')    is not None else None
            revenue_trend_r_squared  = float(trend.get('r_squared'))    if trend.get('r_squared')    is not None else None
        else:
            revenue_trend_slope = revenue_trend_intercept = revenue_trend_r_squared = None
        # <<< END ADD >>>
        

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
        # --- FIX: This is the corrected YEP logic for the historical script ---
        if cytd_revenue > 0 and not cytd_trans.empty:
            # Use "now" relative to Jan 1 for the run-rate window
            days_for_ytd_accumulation = (today_for_calc - pd.Timestamp(datetime(current_year_num, 1, 1)).date()).days + 1
            # Guard: avoid annualizing on tiny windows (<30 days)
            if days_for_ytd_accumulation < 30:
                yep_revenue = cytd_revenue  # no projection yet
            else:
                yep_revenue = (cytd_revenue / float(days_for_ytd_accumulation)) * 365.0

        # Last year's total revenue (full year)
        py_total_revenue = float(
            acc_hist_data.loc[acc_hist_data['year'] == (current_year_num - 1), 'total_revenue'].sum() or 0.0
        )

        # pace_vs_ly as PERCENT (pipeline style)
        pace_vs_ly = None
        if yep_revenue is not None and py_total_revenue is not None:
            if py_total_revenue > 0:
                pace_vs_ly = ((yep_revenue - py_total_revenue) / py_total_revenue) * 100.0
            elif yep_revenue > 0:
                # New Growth: pipeline leaves percent as None and lets UI show "New Growth"
                pace_vs_ly = None
            else:
                pace_vs_ly = 0.0


        
        # Optional: Add a debug log to see the values during processing.
        # Use safe formatting helper to avoid errors when values are None.
        logger.debug(
            "Reprocess Predictions for %s: CYTD=%s YEP=%s PY_Revenue=%s Pace=%s",
            canonical_code,
            _fmt2(cytd_revenue),
            _fmt2(yep_revenue),
            _fmt2(py_total_revenue),
            _fmt2(pace_vs_ly)
        )

        # --- Growth Opportunity Engine ---
        # Default values for growth engine fields
        target_yep_plus_1_pct = None
        additional_revenue_needed_eoy = None
        suggested_next_purchase_amount = None
        growth_engine_message = "Data insufficient for growth suggestion."

        # Determine baseline for calculating growth: use previous year's total revenue if available;
        # otherwise fall back to the projected year‑end pace (YEP). If neither exists, baseline stays 0.
        baseline_for_target = py_total_revenue if py_total_revenue > 0 else (yep_revenue if yep_revenue and yep_revenue > 0 else 0)

        # Compute growth metrics only if we have a baseline and CYTD revenue
        if baseline_for_target > 0 and cytd_revenue is not None:
            # Determine if the account is pacing well versus last year
            is_pacing_well = pace_vs_ly is not None and pace_vs_ly >= 0
            # Assign a growth target percentage: +10% if pacing well, +1% otherwise
            growth_target_pct = 0.10 if is_pacing_well else 0.01

            # Calculate the target total for the year (baseline + growth)
            target_total_for_calc = baseline_for_target * (1.0 + growth_target_pct)
            # Additional revenue needed to hit target from current CYTD revenue
            additional_needed = target_total_for_calc - cytd_revenue

            # Set currency values rounded to 2 decimals
            target_yep_plus_1_pct = round(target_total_for_calc, 2)
            additional_revenue_needed_eoy = round(additional_needed, 2)

            if additional_needed <= 0:
                # Already on track: no additional revenue needed
                growth_engine_message = (
                    f"Excellent! On track or has exceeded the +{growth_target_pct*100:.0f}% target (Target: {_format_currency(target_yep_plus_1_pct)})."
                )
                suggested_next_purchase_amount = None
            else:
                # Need to catch up: compute remaining days and suggested next purchase amount
                # Days remaining in the current year (use processing_end_datetime as 'today')
                try:
                    from datetime import date
                    dec31 = date(current_year_num, 12, 31)
                    days_left_in_year = max(1, (dec31 - processing_end_datetime.date()).days)
                except Exception:
                    days_left_in_year = max(1, 365 - int(days_elapsed) if 'days_elapsed' in locals() else 1)

                # Estimate number of remaining purchases based on median interval
                remaining_purchases_est = max(1.0, days_left_in_year / float(median_interval_days) if median_interval_days > 0 else 1.0)
                # Amount per purchase to meet the target
                amount_per_purchase = additional_needed / remaining_purchases_est
                # Suggest at least $50 and not more than the total additional needed
                suggested_next_purchase_amount = round(additional_revenue_needed_eoy / remaining_purchases_est, 2)


                growth_engine_message = (
                    f"To reach {_format_currency(target_yep_plus_1_pct)} (+{growth_target_pct*100:.0f}% vs baseline), aim for orders around ~{_format_currency(suggested_next_purchase_amount)}."
                )

        # Compute product recommendations: attempt to suggest missing top products or top revenue SKUs
        recommended_upcs = []
        try:
            top_set = getattr(config, 'TOP_30_SET', set())
            account_skus = set()
            if 'item_code' in group.columns:
                account_skus = {str(x).strip() for x in group['item_code'].dropna().unique() if str(x).strip()}
            # Prefer recommending missing products from the top set
            if isinstance(top_set, set) and len(top_set) > 0:
                missing = [sku for sku in top_set if sku not in account_skus]
                recommended_upcs = missing[:3]
            else:
                # Fallback: pick top revenue SKUs for this account
                if 'item_code' in group.columns:
                    revenue_by_sku = group.groupby('item_code')['revenue'].sum().sort_values(ascending=False)
                    recommended_upcs = [str(code) for code in revenue_by_sku.index.tolist() if str(code).strip()][:3]
        except Exception as rec_err:
            logger.warning(f"Could not compute recommended products for {canonical_code}: {rec_err}")
            recommended_upcs = []
        # Serialize recommended products as JSON string (list of SKUs)
        recommended_products_json = json.dumps([str(x) for x in recommended_upcs]) if recommended_upcs else json.dumps([])

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
            'py_total_revenue': py_total_revenue,
            'revenue_trend_slope': revenue_trend_slope,
            'revenue_trend_intercept': revenue_trend_intercept,
            'revenue_trend_r_squared': revenue_trend_r_squared,
            # Placeholders for scores calculated next
            'recency_score': 0, 'frequency_score': 0, 'monetary_score': 0, 'rfm_score': 0.0, 'rfm_segment': '',
            'health_score': 0.0, 'health_category': '', 'priority_score': 0.0, 'enhanced_priority_score': 0.0,
            #'yoy_revenue_growth': 0.0, 'yoy_purchase_count_growth': 0.0, # Calculated later
            #'product_coverage_percentage': 0.0, 'carried_top_products_json': None, 'missing_top_products_json': None # Calculated later
            # Growth engine output fields
            'target_yep_plus_1_pct': target_yep_plus_1_pct,
            'additional_revenue_needed_eoy': additional_revenue_needed_eoy,
            'suggested_next_purchase_amount': suggested_next_purchase_amount,
            'recommended_products_next_purchase_json': recommended_products_json,
            'growth_engine_message': growth_engine_message,
            'avg_purchase_cycle_days': float(median_interval_days)
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
 # In your reprocess_history.py, you need to REMOVE the duplicate merge
# Here's how the code should look:

    # --- Calculate Final YoY and Product Coverage ---
    logger.info("Calculating final YoY and Product Coverage...")
    
    # For Product Coverage, calculate directly from historical_agg_df
    logger.info(f"Calculating product coverage from aggregated data...")
    logger.info(f"Using TOP_30_SET with {len(config.TOP_30_SET)} products: {list(config.TOP_30_SET)[:3]}...")
    
    coverage_df_data = []
    
    # Group by canonical_code to get latest year's products for each account
    for canonical_code in historical_agg_df['canonical_code'].unique():
        account_hist = historical_agg_df[historical_agg_df['canonical_code'] == canonical_code]
        
        # Get the most recent year's data
        if not account_hist.empty:
            latest_year_data = account_hist.sort_values('year', ascending=False).iloc[0]
            
            carried_products = []
            products_json = latest_year_data.get('yearly_products_json')
            
            if products_json and products_json != '[]':
                try:
                    # Parse the JSON
                    if isinstance(products_json, str):
                        product_list = json.loads(products_json)
                    else:
                        product_list = products_json
                    
                    # Check each product against TOP_30_SET
                    if isinstance(product_list, list):
                        for product in product_list:
                            product_str = str(product).strip()
                            
                            # Check if it's in TOP_30_SET (which now has .0 versions)
                            if product_str in config.TOP_30_SET:
                                carried_products.append(product_str)
                        
                        # Remove duplicates
                        carried_products = list(dict.fromkeys(carried_products))
                        
                except (json.JSONDecodeError, TypeError) as e:
                    logger.debug(f"Error parsing products JSON for {canonical_code}: {e}")
            
            # Calculate coverage percentage
            coverage_pct = (len(carried_products) / len(config.TOP_30_SET)) * 100 if config.TOP_30_SET else 0
            
            # Find missing products
            carried_set = set(carried_products)
            missing_products = [p for p in config.TOP_30_SET if p not in carried_set]
            
            # Add to results
            coverage_df_data.append({
                'canonical_code': canonical_code,
                'product_coverage_percentage': round(coverage_pct, 2),
                'carried_top_products_json': json.dumps(carried_products),
                'missing_top_products_json': json.dumps(missing_products[:10])  # Limit to save space
            })
    
    # Convert to DataFrame
    final_coverage = pd.DataFrame(coverage_df_data)
    
    # Log statistics
    if not final_coverage.empty:
        coverage_stats = final_coverage['product_coverage_percentage']
        accounts_with_coverage = (coverage_stats > 0).sum()
        logger.info(f"Product Coverage Results: {accounts_with_coverage}/{len(final_coverage)} accounts have >0% coverage")
        logger.info(f"Coverage Statistics: Avg={coverage_stats.mean():.2f}%, Max={coverage_stats.max():.2f}%")
        
        # Log a few examples
        top_coverage = final_coverage.nlargest(3, 'product_coverage_percentage')
        for _, row in top_coverage.iterrows():
            logger.info(f"  Top coverage example: {row['canonical_code']}: {row['product_coverage_percentage']}% coverage")
    else:
        logger.warning("No coverage data calculated!")
        final_coverage = pd.DataFrame(columns=['canonical_code', 'product_coverage_percentage', 
                                               'carried_top_products_json', 'missing_top_products_json'])
    
    # For YoY metrics, set empty for now (since DB isn't populated yet)
    final_yoy = pd.DataFrame(columns=['canonical_code', 'yoy_revenue_growth', 'yoy_purchase_count_growth'])
    
    # === MERGE SECTION - DO THIS ONLY ONCE ===
    logger.info(f"Columns in predictions_df BEFORE any merges: {predictions_df.columns.tolist()}")
    
    # Merge the coverage data (ONLY ONCE!)
    if not final_coverage.empty:
        # Check if we already have coverage columns (shouldn't happen but be safe)
        if 'product_coverage_percentage' in predictions_df.columns:
            logger.warning("Coverage columns already exist in predictions_df! Dropping before merge.")
            predictions_df = predictions_df.drop(columns=['product_coverage_percentage', 
                                                         'carried_top_products_json', 
                                                         'missing_top_products_json'], errors='ignore')
        
        predictions_df = pd.merge(predictions_df, final_coverage, on='canonical_code', how='left')
        logger.info(f"Product coverage merged. Sample coverage values: {predictions_df['product_coverage_percentage'].head()}")
    
    
    # === FILL NaN VALUES SECTION ===
    # Fill NaN values for all the fields that might be missing
    fill_final = {
        'yoy_revenue_growth': 0.0,
        'yoy_purchase_count_growth': 0.0,
        'product_coverage_percentage': 0.0,
        'carried_top_products_json': json.dumps([]),
        'missing_top_products_json': json.dumps([])
    }
    
    for col, default_val in fill_final.items():
        if col in predictions_df.columns:
            nan_count_before = predictions_df[col].isnull().sum()
            if nan_count_before > 0:
                logger.debug(f"Filling {nan_count_before} NaNs in '{col}' with default")
            predictions_df[col] = predictions_df[col].fillna(default_val)
        else:
            logger.warning(f"Column '{col}' missing after merge, adding with default.")
            predictions_df[col] = default_val
    
    # === FINAL VERIFICATION ===
    # Check that we have coverage data
    final_coverage_check = predictions_df['product_coverage_percentage']
    final_with_coverage = (final_coverage_check > 0).sum()
    logger.info(f"FINAL CHECK: {final_with_coverage}/{len(predictions_df)} accounts have >0% coverage")
    logger.info(f"FINAL CHECK: Coverage range: {final_coverage_check.min():.2f}% to {final_coverage_check.max():.2f}%")
    
    logger.info("Finished all initial prediction calculations.")
    return predictions_df


def verify_product_coverage(engine):
    """
    Verify product coverage was calculated correctly after reprocessing
    (Fixed for PostgreSQL's type requirements)
    """
    try:
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT 
                    COUNT(*) as total_accounts,
                    COUNT(CASE WHEN product_coverage_percentage > 0 THEN 1 END) as with_coverage,
                    ROUND(AVG(product_coverage_percentage)::numeric, 2) as avg_coverage,
                    ROUND(MAX(product_coverage_percentage)::numeric, 2) as max_coverage
                FROM account_predictions
            """)).fetchone()
            
            logger.info("="*60)
            logger.info("PRODUCT COVERAGE VERIFICATION:")
            logger.info(f"  Total accounts: {result.total_accounts}")
            logger.info(f"  Accounts with coverage: {result.with_coverage} ({result.with_coverage*100/result.total_accounts:.1f}%)")
            logger.info(f"  Average coverage: {result.avg_coverage}%")
            logger.info(f"  Max coverage: {result.max_coverage}%")
            logger.info("="*60)
            
            if result.with_coverage == 0:
                logger.warning("⚠️ WARNING: No accounts have product coverage! Check TOP_30_SET configuration.")
            elif result.with_coverage < result.total_accounts * 0.3:
                logger.warning(f"⚠️ WARNING: Only {result.with_coverage*100/result.total_accounts:.1f}% of accounts have coverage. This seems low.")
            else:
                logger.info("✅ Product coverage looks healthy!")
                
    except Exception as e:
        logger.error(f"Error verifying product coverage: {e}")


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

            # If you want DB defaults to populate created_at/updated_at, exclude them here:
            # exclude_for_defaults = {'created_at', 'updated_at'}
            # pred_model_cols = [c for c in pred_model_cols if c not in exclude_for_defaults]

            # Ensure all reflected columns exist in the DataFrame (create missing as None)
            missing_pred_cols = [c for c in pred_model_cols if c not in predictions_df.columns]
            if missing_pred_cols:
                logger.info(f"Adding missing prediction columns as NULL: {missing_pred_cols}")
                for c in missing_pred_cols:
                    predictions_df[c] = None

            # Now safely select columns and insert
            prediction_data = (
                predictions_df[pred_model_cols]
                .replace({np.nan: None, pd.NaT: None})
                .to_dict(orient='records')
            )
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

            # Add product coverage verification
            logger.info("Verifying product coverage calculations...")
            verify_product_coverage(engine)

            logger.info(f"--- Reprocessing Complete (Duration: {end_time - start_time:.2f}s) ---")
        else:
            logger.error("Database population failed. Check logs for details.")
            sys.exit(1)

    except Exception as e:
        logger.error(f"A critical error occurred during reprocessing: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()