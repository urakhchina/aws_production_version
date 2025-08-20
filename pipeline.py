import json
import pandas as pd
import numpy as np
from scipy import stats
from datetime import datetime, timedelta, date
import time
import math
import os
import logging
import re
import hashlib
from sqlalchemy.orm import Session as SQLAlchemySession
from dateutil.relativedelta import relativedelta
import sys

# Import SQLAlchemy functions needed
from sqlalchemy import select, func, distinct, and_, desc, extract

# --- Configure Logger ---
logger = logging.getLogger(__name__)
if not logger.handlers:
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - PPLN_DEBUG: %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
logger.setLevel(logging.DEBUG)

# --- Import DB and Models ---
try:
    from models import db, AccountPrediction, AccountHistoricalRevenue, Transaction
    logger.info("Successfully imported database models.")
except ImportError as e:
    logger.error(f"Could not import models. Ensure script runs in project context: {e}")
    db = None
    AccountPrediction = None
    AccountHistoricalRevenue = None
    Transaction = None

# --- Import Config and Scoring Thresholds ---
try:
    import config
    logger.info("Successfully imported config module.")
    
    # Load scoring thresholds
    HEALTH_POOR_THRESHOLD = getattr(config, 'HEALTH_POOR_THRESHOLD', 40)
    PRIORITY_PACE_DECLINE_PCT_THRESHOLD = getattr(config, 'PRIORITY_PACE_DECLINE_PCT_THRESHOLD', -10)
    GROWTH_PACE_INCREASE_PCT_THRESHOLD = getattr(config, 'GROWTH_PACE_INCREASE_PCT_THRESHOLD', 10)
    GROWTH_HEALTH_THRESHOLD = getattr(config, 'GROWTH_HEALTH_THRESHOLD', 60)
    GROWTH_MISSING_PRODUCTS_THRESHOLD = getattr(config, 'GROWTH_MISSING_PRODUCTS_THRESHOLD', 3)
    
    # Load TOP_30 product sets
    TOP_30_SET = getattr(config, 'TOP_30_SET', set())
    TOP_30_MATCH_SET = getattr(config, 'TOP_30_MATCH_SET', set())
    
    # Import the is_top_30_product function if available
    try:
        from config import is_top_30_product
        logger.info(f"Loaded is_top_30_product function from config")
    except ImportError:
        # Define fallback function if not in config
        def is_top_30_product(upc):
            """Check if a UPC is in the TOP_30 list - handles all formats"""
            if not upc:
                return False
            if pd.isna(upc):
                return False
            upc_str = str(upc).strip()
            return (upc_str in TOP_30_MATCH_SET or 
                    f"{upc_str}.0" in TOP_30_MATCH_SET or
                    (upc_str.endswith('.0') and upc_str[:-2] in TOP_30_MATCH_SET))
        logger.info("Using fallback is_top_30_product function")
    
    # Ensure TOP_30_SET is a set of strings
    if not isinstance(TOP_30_SET, set):
        logger.warning(f"TOP_30_SET is not a set (type: {type(TOP_30_SET)}). Converting.")
        TOP_30_SET = set(str(sku) for sku in TOP_30_SET if sku) if TOP_30_SET else set()
    
    # Create TOP_30_MATCH_SET if not defined
    if not TOP_30_MATCH_SET:
        TOP_30_MATCH_SET = TOP_30_SET | {f"{s}.0" for s in TOP_30_SET}
    
    logger.info(f"Loaded config: TOP_30_SET has {len(TOP_30_SET)} items, "
                f"HEALTH_POOR={HEALTH_POOR_THRESHOLD}, PACE_DECLINE={PRIORITY_PACE_DECLINE_PCT_THRESHOLD}")

except ImportError as e:
    logger.warning(f"Could not import config.py: {e}. Using defaults.")
    
    # Define all defaults
    HEALTH_POOR_THRESHOLD = 40
    PRIORITY_PACE_DECLINE_PCT_THRESHOLD = -10
    GROWTH_PACE_INCREASE_PCT_THRESHOLD = 10
    GROWTH_HEALTH_THRESHOLD = 60
    GROWTH_MISSING_PRODUCTS_THRESHOLD = 3
    TOP_30_SET = set()
    TOP_30_MATCH_SET = set()
    
    # Define fallback function
    def is_top_30_product(upc):
        return False

# Derive dependent constants
PRIORITY_HEALTH_THRESHOLD = HEALTH_POOR_THRESHOLD

# --- Helper functions for safe type conversion ---
def safe_float(value, default=0.0):
    try:
        if pd.isna(value): return default
        return float(value)
    except (ValueError, TypeError): return default

def safe_int(value, default=0):
    try:
        if pd.isna(value): return default
        return int(float(value)) # Convert potential float first
    except (ValueError, TypeError): return default

# --- Normalization & Key Generation Functions ---

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

def get_base_card_code(card_code):
    """Extracts the base part of a CardCode."""
    if not card_code or not isinstance(card_code, str): return ""
    base = re.split(r'[_\s-]+', card_code.strip(), 1)[0]
    return base

# Add this helper function near the top of pipeline.py
def _normalize_upc(upc):
    """Normalize UPC to a string of pure digits, removing decimals, whitespace, and non-digits."""
    if pd.isna(upc) or upc is None:
        return ""
    try:
        # Convert to string and handle float inputs
        upc_str = str(upc)
        # Remove decimal and anything after it (e.g., "710363579791.0" -> "710363579791")
        upc_str = upc_str.split('.')[0]
        # Remove non-digits (e.g., hyphens, spaces)
        upc_str = re.sub(r'\D', '', upc_str).strip()
        # Ensure leading zeros are preserved (match TOP_30_SET format, e.g., "0071036358549")
        if upc_str and upc_str in TOP_30_SET:
            # If the UPC matches a TOP_30_SET entry, return it as-is to preserve leading zeros
            for top_sku in TOP_30_SET:
                if upc_str == top_sku.lstrip('0'):
                    return top_sku
        return upc_str if upc_str else ""
    except Exception as e:
        logger.warning(f"Error normalizing UPC '{upc}': {e}")
        return ""


def generate_canonical_code(row):
    """
    Generates the canonical code based on ShipTo or fallback logic.
    Expects row to contain pre-calculated 'base_card_code' and original
    'ShipTo', 'NAME', 'ADDRESS', 'CITY', 'STATE', 'ZIPCODE'.
    """
    # Use 'base_card_code' if it exists, otherwise get it from 'CARD_CODE'
    base_code = str(row.get('base_card_code') or get_base_card_code(row.get('CARD_CODE', ''))).strip()

    ship_to_col_name = 'ShipTo' if 'ShipTo' in row else 'SHIPTO' # Handle potential case diff
    ship_to = str(row.get(ship_to_col_name, '') or '').strip()

    if not base_code:
        logger.warning("Cannot generate canonical code: Missing base_card_code in input row.")
        return None

    # --- Strategy 1: Use ShipTo if valid ---
    if ship_to and ship_to.lower() not in ['', 'nan', 'none', 'null', '0']:
        clean_ship_to = re.sub(r'[^A-Z0-9\-]+', '', ship_to.upper()).strip()
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


# +++ NEW FUNCTION: Calculate Rolling SKU Analysis +++
def calculate_rolling_sku_analysis(canonical_codes: list, session: SQLAlchemySession):
    """
    Calculates rolling 12-month analysis for all SKUs for a given list of accounts.
    Includes robust timezone handling and debugging outputs.
    """
    logger.info(f"Calculating rolling 12-month SKU analysis for {len(canonical_codes)} accounts...")
    if not canonical_codes:
        return {}

    # --- Time Period Logic (Using your precise relativedelta version) ---
    now = datetime.utcnow()
    current_period_end = now
    current_period_start = now - timedelta(days=365)

    prior_period_end = current_period_start - timedelta(days=1)
    prior_period_start = prior_period_end - timedelta(days=365)

    # Add debug logging
    logger.debug(f"Rolling SKU Analysis - Time window: {current_period_start} to {current_period_end}")

    cy_start = datetime(now.year, 1, 1)

    # --- Query to fetch all relevant transactions ---
    stmt = select(
        Transaction.canonical_code,
        Transaction.item_code,
        Transaction.description,
        Transaction.posting_date,
        Transaction.revenue
    ).where(
        Transaction.canonical_code.in_(canonical_codes),
        Transaction.posting_date >= prior_period_start,
        Transaction.item_code.isnot(None),
        Transaction.item_code != ''
    ).order_by(Transaction.item_code)
    
    results = session.execute(stmt).fetchall()

    # After fetching results, add:
    logger.debug(f"Found {len(results)} transactions for rolling analysis")

    
    if not results:
        logger.warning("No transactions found in the last 24 months for the given accounts.")
        return {}

    # --- Process data in memory using pandas ---
    df = pd.DataFrame(results, columns=[
        'canonical_code', 
        'item_code', 
        'description', 
        'posting_date', 
        'revenue'
    ])
    #df['item_code'] = df['item_code'].apply(_normalize_upc)
    df['item_code'] = df['item_code'].astype(str).str.strip()
    df['revenue'] = pd.to_numeric(df['revenue'], errors='coerce').fillna(0.0)

    # +++ REVISED FIX: Convert to UTC *during* datetime conversion +++
    # This is more robust. It tells pandas to treat the naive timestamps from the DB as UTC.
    df['posting_date'] = pd.to_datetime(df['posting_date'], errors='coerce', utc=True)
    
    # Also make our comparison dates timezone-aware pandas Timestamps
    current_period_start_utc = pd.Timestamp(current_period_start, tz='UTC')
    current_period_end_utc = pd.Timestamp(current_period_end, tz='UTC')
    prior_period_start_utc = pd.Timestamp(prior_period_start, tz='UTC')
    prior_period_end_utc = pd.Timestamp(prior_period_end, tz='UTC')
    cy_start_utc = pd.Timestamp(cy_start, tz='UTC')
    now_utc = pd.Timestamp(now, tz='UTC')
    # +++ END REVISED FIX +++

    # --- Debugging Print Block ---
    #DEBUG_CANONICAL_CODE = '02PA1588_KIMBERTONWHOLEFOODS'
    #if DEBUG_CANONICAL_CODE in df['canonical_code'].unique():
    #    print(f"\n--- DEBUGGING FOR {DEBUG_CANONICAL_CODE} ---")
    #    print(f"Current Period Start (UTC): {current_period_start_utc}")
    #    print(f"Current Period End (UTC):   {current_period_end_utc}")
        
    #    account_df = df[df['canonical_code'] == DEBUG_CANONICAL_CODE].copy()
    #    # Drop rows where date conversion failed
    #    account_df.dropna(subset=['posting_date'], inplace=True)

    #    print(f"DataFrame sample for account (now with UTC timestamps):")
    #    print(account_df[['posting_date', 'revenue']].head())

        # Check the mask directly
    #    account_mask = (account_df['posting_date'] >= current_period_start_utc) & (account_df['posting_date'] <= current_period_end_utc)
    #    print(f"Number of rows matching current period mask: {account_mask.sum()}")
    #    print("------------------------------------------\n")
    # --- End Debugging Print Block ---

    # --- Aggregate revenue for each period using the new timezone-aware dates ---
    # Drop any rows where date conversion might have failed (became NaT) before masking
    df.dropna(subset=['posting_date'], inplace=True)

    current_mask = (df['posting_date'] >= current_period_start_utc) & (df['posting_date'] <= current_period_end_utc)
    prior_mask = (df['posting_date'] >= prior_period_start_utc) & (df['posting_date'] <= prior_period_end_utc)
    cytd_mask = (df['posting_date'] >= cy_start_utc) & (df['posting_date'] <= now_utc)

    current_rev = df[current_mask].groupby(['canonical_code', 'item_code'])['revenue'].sum().reset_index().rename(columns={'revenue': 'current_12m_rev'})
    prior_rev = df[prior_mask].groupby(['canonical_code', 'item_code'])['revenue'].sum().reset_index().rename(columns={'revenue': 'prior_12m_rev'})
    cytd_rev = df[cytd_mask].groupby(['canonical_code', 'item_code'])['revenue'].sum().reset_index().rename(columns={'revenue': 'cytd_sku_rev'})

    latest_desc = df.sort_values('posting_date', ascending=False).drop_duplicates(subset=['item_code'])[['item_code', 'description']]

    # --- Merge all dataframes together ---
    merged_df = pd.merge(current_rev, prior_rev, on=['canonical_code', 'item_code'], how='outer')
    merged_df = pd.merge(merged_df, cytd_rev, on=['canonical_code', 'item_code'], how='left')
    merged_df = pd.merge(merged_df, latest_desc, on='item_code', how='left')
    merged_df = merged_df.fillna(0.0)

    # --- Calculate metrics ---
    def calculate_yoy_change(row):
        current = row['current_12m_rev']
        prior = row['prior_12m_rev']
        if prior > 0:
            return ((current - prior) / prior) * 100.0
        elif current > 0 and prior == 0:
            return None
        return 0.0
    merged_df['yoy_change_pct'] = merged_df.apply(calculate_yoy_change, axis=1)

    days_in_year = (now.date() - cy_start.date()).days + 1
    merged_df['sku_yep'] = (merged_df['cytd_sku_rev'] / days_in_year) * 365.0 if days_in_year > 0 else 0

    # --- Format for JSON output ---
    final_analysis = {}
    for code in canonical_codes:
        final_analysis[code] = []

    for index, row in merged_df.iterrows():
        canonical_code = row['canonical_code']
        # Only include SKUs that had activity in the current 12-month period
        if row['current_12m_rev'] > 0 or row['prior_12m_rev'] > 0:
            yoy_change_val = row['yoy_change_pct']
            
            # FIX: Handle None values properly before rounding
            if yoy_change_val is not None and not pd.isna(yoy_change_val):
                final_yoy_change = round(yoy_change_val, 1)
            else:
                final_yoy_change = None
            
            sku_to_check = str(row['item_code']).strip()

            final_analysis[canonical_code].append({
                'item_code': row['item_code'],
                'description': row['description'] or "N/A",
                'current_12m_rev': round(row['current_12m_rev'], 2),
                'sku_yep': round(row['sku_yep'], 2) if row['sku_yep'] > 0 else None,
                'yoy_change_pct': final_yoy_change,  # Use the cleaned value
                'prior_12m_rev': round(row.get('prior_12m_rev', 0.0), 2),
                'is_top_30': is_top_30_product(str(row['item_code']).strip()) 
            })
    
    # Sort each account's list by current 12m revenue
    for code in final_analysis:
        final_analysis[code].sort(key=lambda x: x['current_12m_rev'], reverse=True)

    logger.info("Finished calculating rolling 12-month SKU analysis.")
    return final_analysis

# --- Calculation Functions ---

def calculate_yoy_metrics_from_db(current_year: int, session: SQLAlchemySession):
    """
    Calculates YoY metrics by querying AccountHistoricalRevenue directly using SQLAlchemy 2.0 syntax.
    Needs app context to run.
    Returns a DataFrame with canonical_code, yoy_revenue_growth, yoy_purchase_count_growth.
    """
    logger.info(f"Calculating YoY metrics from DB for year {current_year} vs {current_year-1}")
    db_session = session
    if not db_session:
        logger.error("Programming Error: No database session provided to calculate_yoy_metrics_from_db")
        return pd.DataFrame(columns=['canonical_code', 'yoy_revenue_growth', 'yoy_purchase_count_growth'])
    try:
        # Query current year data
        current_stmt = db.select(
            AccountHistoricalRevenue.canonical_code,
            AccountHistoricalRevenue.total_revenue,
            AccountHistoricalRevenue.transaction_count # This is yearly count
        ).where(AccountHistoricalRevenue.year == current_year)
        current_result = db_session.execute(current_stmt)
        current_year_data = current_result.all()

        # Query previous year data
        prev_stmt = db.select(
            AccountHistoricalRevenue.canonical_code,
            AccountHistoricalRevenue.total_revenue.label('prev_year_revenue'),
            AccountHistoricalRevenue.transaction_count.label('prev_year_transactions')
        ).where(AccountHistoricalRevenue.year == (current_year - 1))
        prev_result = db_session.execute(prev_stmt)
        prev_year_data = prev_result.all()

        current_df = pd.DataFrame(current_year_data, columns=['canonical_code', 'total_revenue', 'transaction_count'])
        prev_df = pd.DataFrame(prev_year_data, columns=['canonical_code', 'prev_year_revenue', 'prev_year_transactions'])

        if current_df.empty:
            logger.warning("No data found for the current year in DB for YoY calculation.")
            return pd.DataFrame(columns=['canonical_code', 'yoy_revenue_growth', 'yoy_purchase_count_growth']) # Use final name

        # Merge data using canonical_code
        merged_df = pd.merge(current_df, prev_df, on='canonical_code', how='left')
        merged_df.fillna(0, inplace=True) # Simpler fillna

        # Calculate growth
        merged_df['yoy_revenue_growth'] = merged_df.apply(
            lambda r: ((r['total_revenue'] - r['prev_year_revenue']) / r['prev_year_revenue'] * 100) if r['prev_year_revenue'] > 0 else (100.0 if r['total_revenue'] > 0 else 0.0), axis=1)
        # Use transaction_count (yearly count) for purchase count growth
        merged_df['yoy_purchase_count_growth'] = merged_df.apply(
             lambda r: ((r['transaction_count'] - r['prev_year_transactions']) / r['prev_year_transactions'] * 100) if r['prev_year_transactions'] > 0 else (100.0 if r['transaction_count'] > 0 else 0.0), axis=1)

        logger.info(f"Calculated YoY metrics from DB for {len(merged_df)} accounts.")
        return merged_df[['canonical_code', 'yoy_revenue_growth', 'yoy_purchase_count_growth']] # Return with final name

    except Exception as e:
        logger.error(f"Error calculating YoY metrics from DB: {e}", exc_info=True)
        return pd.DataFrame(columns=['canonical_code', 'yoy_revenue_growth', 'yoy_purchase_count_growth'])

def calculate_product_coverage_from_db(session: SQLAlchemySession):
    """
    Calculates product coverage by querying the latest yearly_products_json
    with proper handling of decimal UPCs from the database.
    """
    logger.info("Calculating product coverage (SKU-based) from DB...")
    db_session = session
    if not db_session:
        logger.error("Programming Error: No database session provided to calculate_product_coverage_from_db")
        return pd.DataFrame(columns=['canonical_code', 'product_coverage_percentage', 'carried_top_products_json', 'missing_top_products_json'])
    
    global TOP_30_SET, TOP_30_MATCH_SET
    if not TOP_30_SET:
        logger.warning("TOP_30_SET (SKUs) is empty. Cannot calculate product coverage.")
        return pd.DataFrame(columns=['canonical_code', 'product_coverage_percentage', 'carried_top_products_json', 'missing_top_products_json'])

    try:
        # Get latest year data for each account
        latest_year_stmt = db.select(
            AccountHistoricalRevenue.canonical_code,
            func.max(AccountHistoricalRevenue.year).label('latest_year')
        ).group_by(AccountHistoricalRevenue.canonical_code)
        latest_year_subq = latest_year_stmt.subquery()

        latest_year_data_stmt = db.select(
            AccountHistoricalRevenue.canonical_code,
            AccountHistoricalRevenue.yearly_products_json
        ).join(
            latest_year_subq,
            and_(
                AccountHistoricalRevenue.canonical_code == latest_year_subq.c.canonical_code,
                AccountHistoricalRevenue.year == latest_year_subq.c.latest_year
            )
        )
        latest_year_result = db_session.execute(latest_year_data_stmt)
        latest_year_data = latest_year_result.all()

        # Get ALL historical data for missing product insights
        all_historical_stmt = db.select(
            AccountHistoricalRevenue.canonical_code,
            AccountHistoricalRevenue.year,
            AccountHistoricalRevenue.yearly_products_json
        )
        all_historical_result = db_session.execute(all_historical_stmt)
        all_historical_data = all_historical_result.all()

        if not latest_year_data:
            logger.warning("No historical data found to calculate product coverage from DB.")
            return pd.DataFrame(columns=['canonical_code', 'product_coverage_percentage', 'carried_top_products_json', 'missing_top_products_json'])

        # Build historical SKU lookup for insights
        historical_sku_lookup = {}
        for row in all_historical_data:
            canonical_code = row.canonical_code
            year = row.year
            skus_json = row.yearly_products_json
            
            if canonical_code not in historical_sku_lookup:
                historical_sku_lookup[canonical_code] = {}
            
            if skus_json:
                try:
                    sku_list = json.loads(skus_json)
                    if isinstance(sku_list, list):
                        for sku in sku_list:
                            # Normalize the SKU for consistent lookup
                            sku_normalized = normalize_upc_for_matching(sku)
                            if sku_normalized:
                                if sku_normalized not in historical_sku_lookup[canonical_code]:
                                    historical_sku_lookup[canonical_code][sku_normalized] = []
                                historical_sku_lookup[canonical_code][sku_normalized].append(year)
                except Exception as e:
                    logger.warning(f"Could not parse historical SKUs for {canonical_code}, year {year}: {e}")

        logger.info(f"Processing SKU coverage for {len(latest_year_data)} accounts based on latest year from DB.")
        coverage_results = []
        
        for row in latest_year_data:
            canonical_code_val = row.canonical_code
            skus_json = row.yearly_products_json
            
            # Parse account's SKUs and check against TOP_30
            carried_products = []
            missing_products = []
            
            if skus_json:
                try:
                    sku_list = json.loads(skus_json)
                    if isinstance(sku_list, list):
                        # Check each account SKU against TOP_30
                        for sku in sku_list:
                            if is_top_30_product(sku):
                                # Normalize for clean storage
                                normalized = normalize_upc_for_matching(sku)
                                if normalized in TOP_30_SET and normalized not in carried_products:
                                    carried_products.append(normalized)
                except Exception as e:
                    logger.warning(f"Could not parse SKUs for {canonical_code_val}: {e}")
            
            # Determine missing products
            carried_set = set(carried_products)
            missing_products = [sku for sku in TOP_30_SET if sku not in carried_set]
            
            # Calculate coverage percentage
            coverage_percent = (len(carried_products) / len(TOP_30_SET)) * 100 if TOP_30_SET else 0.0
            
            # Build insights for missing products
            missing_products_with_insights = []
            account_historical_skus = historical_sku_lookup.get(canonical_code_val, {})
            
            for missing_sku in missing_products:
                if missing_sku in account_historical_skus:
                    years_purchased = account_historical_skus[missing_sku]
                    last_year = max(years_purchased) if years_purchased else None
                    yrs_ago = datetime.now().year - last_year if last_year else None
                    
                    if yrs_ago == 1:
                        insight = "Purchased last year but not this year - potential win-back opportunity"
                    elif yrs_ago == 2:
                        insight = "Purchased 2 years ago - consider reintroducing"
                    elif yrs_ago and yrs_ago >= 3:
                        insight = f"Last purchased {yrs_ago} years ago - long-term reactivation opportunity"
                    else:
                        insight = "Previously purchased - reactivation opportunity"
                    
                    missing_products_with_insights.append({
                        "sku": missing_sku,
                        "last_purchased_year": last_year,
                        "placeholder_insight": insight
                    })
                else:
                    missing_products_with_insights.append({
                        "sku": missing_sku,
                        "last_purchased_year": None,
                        "placeholder_insight": "Never purchased - new product opportunity"
                    })
            
            coverage_results.append({
                'canonical_code': canonical_code_val,
                'product_coverage_percentage': round(coverage_percent, 2),
                'carried_top_products_json': safe_json_dumps(carried_products),
                'missing_top_products_json': safe_json_dumps(missing_products_with_insights)
            })
        
        coverage_df = pd.DataFrame(coverage_results)
        logger.info(f"Calculated enhanced SKU-based product coverage from DB for {len(coverage_df)} accounts.")
        return coverage_df

    except Exception as e:
        logger.error(f"Error calculating enhanced SKU-based product coverage from DB: {e}", exc_info=True)
        return pd.DataFrame(columns=['canonical_code', 'product_coverage_percentage', 'carried_top_products_json', 'missing_top_products_json'])



# --- Helper function for safe product aggregation (now for SKUs) ---
def aggregate_item_codes(series): # Renamed for clarity
    """Safely aggregates unique ITEM CODES (SKUs) from a pandas Series."""
    try:
        # Ensure SKUs are strings, strip whitespace, replace empty with NaN, drop NaN, get unique, sort
        unique_item_codes = sorted(list(
            series.astype(str).str.strip().replace('', np.nan).dropna().unique()
        ))
        # Final filter for any residual empty strings
        unique_item_codes = [sku for sku in unique_item_codes if sku]
        return unique_item_codes
    except Exception as e:
        logger.error(f"Error during ITEM CODE aggregation step: {e}", exc_info=True)
        return []

# --- Helper function for safe JSON dumping ---
def safe_json_dumps(data):
    """Safely dumps data to JSON string, handling errors and non-list types."""
    if data is None: return None
    try:
        if isinstance(data, (list, dict, set)):
             # For sets, convert to list first
            if isinstance(data, set): data = list(data)
            return json.dumps(data)
        else:
            logger.warning(f"Data for JSON dump is not a list/dict/set, got {type(data)}. Storing as is.")
            return json.dumps(str(data)) # Best effort
    except Exception as dump_err:
        logger.error(f"Error dumping data to JSON: {dump_err}. Data sample: {str(data)[:100]}", exc_info=False)
        return None
    

# --- NEW: Yearly Revenue Trend Calculation Function ---
def calculate_yearly_revenue_trend(yearly_history_list_of_dicts):
    """
    Calculates linear trend from a list of yearly revenue data.
    Input: [{'year': YYYY, 'revenue': RRRR}, ...]
    Returns: {'slope': float, 'intercept': float, 'r_squared': float, 'p_value': float, 'stderr': float} or None
    """
    if not yearly_history_list_of_dicts or len(yearly_history_list_of_dicts) < 2:
        return None

    valid_points = [(d['year'], d['revenue']) for d in yearly_history_list_of_dicts 
                    if d.get('year') is not None and d.get('revenue') is not None and 
                       isinstance(d['year'], (int, float)) and isinstance(d['revenue'], (int, float))]
    
    if len(valid_points) < 2:
        return None

    years = np.array([p[0] for p in valid_points])
    revenues = np.array([p[1] for p in valid_points])

    try:
        slope, intercept, r_value, p_value, stderr = stats.linregress(years, revenues)
        r_squared = r_value**2
        
        if np.isnan(slope) or np.isnan(r_squared) or not np.isfinite(slope) or not np.isfinite(r_squared): # Check for NaN/inf
            logger.debug(f"Regression resulted in NaN/inf for data: years={years}, revenues={revenues}")
            return None 
        return {"slope": slope, "intercept": intercept, "r_squared": r_squared, "p_value": p_value, "stderr": stderr}
    except Exception as e:
        logger.error(f"Error during linear regression ({years}, {revenues}): {e}", exc_info=True)
        return None
# --- End Trend Calculation ---
    

# --- Product Matching Helper Functions ---
# normalize_product_name is NO LONGER USED for primary product identification.
# It might be kept if you want to store/display normalized descriptions alongside SKUs,
# but TOP_30_SET and coverage logic will use SKUs directly.
def normalize_product_name(product_name):
    """
    Normalizes product names by removing size indicators, standardizing format.
    THIS IS NOW FOR DESCRIPTIVE PURPOSES, NOT IDENTIFICATION for coverage.
    """
    if not product_name:
        return ""
    name = product_name.lower()
    name = re.sub(r'\b\d+[a-z]+\b', '', name)
    name = re.sub(r'\b\d+in\d+\b', '', name)
    name = name.replace('-', ' ')
    name = name.replace('+', ' plus ')
    if "testosterone" in name:
        if "up" in name:
            name = re.sub(r'testosterone\s+up', 'testosterone up', name)
    name = re.sub(r'\s+', ' ', name)
    return name.strip()



# --- Main historical data function ---
def create_historical_revenue_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Create historical revenue data aggregated ONLY BY YEAR, including a JSON list
    of unique ITEM CODES (SKUs) purchased that year.

    Parameters:
    -----------
    df : pd.DataFrame
        Cleaned DataFrame with CARD_CODE, POSTINGDATE, AMOUNT, QUANTITY, ITEM (SKU),
        NAME, SalesRep, Distributor.

    Returns:
    --------
    pd.DataFrame
        DataFrame with historical revenue aggregated ONLY by year, including a
        'yearly_products_json' column (containing JSON string of SKUs or None).
    """
    logger.info("Creating YEARLY historical revenue data including ITEM CODES (SKUs)")

    working_df = df.copy()

    # --- Data Validation and Preparation ---
    # *** MODIFICATION: Added 'ITEM' to required_cols ***
    required_cols = ['CARD_CODE', 'POSTINGDATE', 'AMOUNT', 'QUANTITY', 'ITEM', 
                     'NAME', 'SalesRep', 'Distributor', 'year'] 
    missing_cols = [col for col in required_cols if col not in working_df.columns]
    if missing_cols:
        logger.error(f"Missing required columns for historical aggregation: {missing_cols}")
        raise ValueError(f"Missing required columns: {missing_cols}")

    try:
        working_df['POSTINGDATE'] = pd.to_datetime(working_df['POSTINGDATE'], errors='coerce')
        working_df = working_df.dropna(subset=['POSTINGDATE', 'CARD_CODE', 'year'])
        working_df['CARD_CODE'] = working_df['CARD_CODE'].astype(str)
        # *** MODIFICATION: Fill NaN ITEM codes with empty string ***
        working_df['ITEM'] = working_df['ITEM'].fillna('').astype(str)
        working_df['year'] = working_df['year'].astype(int)

        working_df['AMOUNT'] = pd.to_numeric(working_df['AMOUNT'], errors='coerce').fillna(0)
        working_df['QUANTITY'] = pd.to_numeric(working_df['QUANTITY'], errors='coerce').fillna(0)
        working_df['revenue'] = working_df['AMOUNT']

        total_revenue_raw = working_df['revenue'].sum()
        logger.info(f"Total raw revenue before yearly aggregation: ${total_revenue_raw:.2f}")

    except Exception as prep_err:
        logger.error(f"Error during data preparation for historical aggregation: {prep_err}", exc_info=True)
        return pd.DataFrame(columns=['CARD_CODE', 'year', 'total_revenue', 'transaction_count',
                                     'name', 'sales_rep', 'distributor', 'yearly_products_json'])

    # --- Perform Yearly Aggregation with Products (SKUs) ---
    logger.info("Creating yearly aggregation with ITEM CODES (SKUs)")

    agg_funcs = {
        'total_revenue': ('revenue', 'sum'),
        'transaction_count': ('POSTINGDATE', 'count'),
        'name': ('NAME', 'first'),
        'sales_rep': ('SalesRep', 'first'),
        'distributor': ('Distributor', 'first'),
        # *** MODIFICATION: Aggregate 'ITEM' column using aggregate_item_codes ***
        'yearly_item_codes': ('ITEM', aggregate_item_codes) 
    }

    try:
        logger.info("Performing groupby aggregation...")
        yearly_agg = working_df.groupby(['CARD_CODE', 'year'], as_index=False).agg(**agg_funcs)
        logger.info(f"Aggregation complete. Result shape: {yearly_agg.shape}")
    except Exception as agg_err:
        logger.error(f"Error during groupby aggregation step: {agg_err}", exc_info=True)
        return pd.DataFrame(columns=['CARD_CODE', 'year', 'total_revenue', 'transaction_count',
                                     'name', 'sales_rep', 'distributor', 'yearly_products_json'])

    # --- Convert item code list to JSON string ---
    try:
        logger.info("Applying JSON conversion to ITEM CODE lists...")
        if 'yearly_item_codes' in yearly_agg.columns:
            # *** MODIFICATION: Rename to yearly_products_json for consistency with model, but it contains SKUs ***
            yearly_agg['yearly_products_json'] = yearly_agg['yearly_item_codes'].apply(safe_json_dumps)
            yearly_agg = yearly_agg.drop(columns=['yearly_item_codes'])
            logger.info("ITEM CODE JSON conversion complete.")
        else:
            logger.warning("'yearly_item_codes' column not found after aggregation. Setting 'yearly_products_json' to None.")
            yearly_agg['yearly_products_json'] = None

    except Exception as json_err:
        logger.error(f"Error during ITEM CODE JSON conversion step: {json_err}", exc_info=True)
        yearly_agg['yearly_products_json'] = None # Ensure column exists

    # --- Final Verification & Return ---
    # ... (rest of the function remains the same) ...
    yearly_total_agg = yearly_agg['total_revenue'].sum() if 'total_revenue' in yearly_agg.columns else 0
    logger.info(f"Total yearly aggregated revenue: ${yearly_total_agg:.2f}")
    tolerance = 0.01
    if total_revenue_raw != 0 and abs(yearly_total_agg - total_revenue_raw) > abs(total_revenue_raw * tolerance):
         logger.warning(f"Potential revenue discrepancy > {tolerance*100}%: Raw (${total_revenue_raw:.2f}) vs Aggregated (${yearly_total_agg:.2f}).")
    elif total_revenue_raw == 0 and yearly_total_agg != 0:
         logger.warning(f"Revenue discrepancy: Raw was zero but Aggregated is ({yearly_total_agg:.2f}).")
    else:
         logger.info("Yearly aggregation total matches raw total within tolerance.")

    logger.info(f"Created {len(yearly_agg)} yearly historical revenue records including ITEM CODE lists.")

    if 'yearly_products_json' not in yearly_agg.columns:
        logger.error("CRITICAL: 'yearly_products_json' column is missing before return! Adding as None.")
        yearly_agg['yearly_products_json'] = None
        
    return yearly_agg


def load_raw_data(filepath: str) -> pd.DataFrame:
    logger.info("Loading raw data from %s", filepath)

    # ── 0 │ dtype overrides ────────────────────────────────────────────────
    DTYPES = {
        'ID': str, 'INSERT': str,
        'CardCode': str, 'CARD_CODE': str,     # critical for canonical pipeline
        'CUSTOMERID': str, 'NAME': str,
        'ADDRESS': str, 'CITY': str, 'STATE': str, 'ZIPCODE': str,
        'WHSSTORECOUNTY': str, 'REGION': str,
        'ITEM': str, 'DESCRIPTION': str, 'ITEMDESC': str,
        'POSTINGDATE': str, 'MONTH': str, 'YEAR': str,
        'QUANTITY': str, 'AMOUNT': str,
        'SalesRep': str, 'SlpName': str, 'Distributor': str,
    }

    # ── 1 │ read CSV (force string, silence dtype warnings) ───────────────
    try:
        df = pd.read_csv(
            filepath,
            dtype=DTYPES,
            low_memory=False,
            encoding="utf-8",
            on_bad_lines="skip",
        )
        logger.info("Loaded %s rows (utf-8)", f"{len(df):,}")
    except UnicodeDecodeError:
        df = pd.read_csv(
            filepath,
            dtype=DTYPES,
            low_memory=False,
            encoding="latin-1",
            on_bad_lines="skip",
        )
        logger.info("Loaded %s rows (latin-1)", f"{len(df):,}")
    except Exception as e:
        logger.error("Failed to load %s: %s", filepath, e, exc_info=True)
        return pd.DataFrame()

    # ── 2 │ standardise header name ───────────────────────────────────────
    if "CardCode" in df.columns:
        df.rename(columns={"CardCode": "CARD_CODE"}, inplace=True)
        logger.info("Renamed 'CardCode' → 'CARD_CODE'")

    if "CARD_CODE" not in df.columns:
        logger.error("CRITICAL: No CardCode/CARD_CODE column found")
        return pd.DataFrame()

    # ── 3 │ derive base_card_code once per file ───────────────────────────
    if "base_card_code" not in df.columns:
        df["base_card_code"] = df["CARD_CODE"].apply(get_base_card_code)
        logger.info("Created 'base_card_code' column")

    # ── 4 │ ITEM fallback (optional) ──────────────────────────────────────
    if "ITEM" not in df.columns:
        logger.warning("ITEM column missing – using ITEMDESC or placeholder")
        df["ITEM"] = df.get("ITEMDESC", "")

    logger.info("Columns present: %s", ", ".join(df.columns))
    return df


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and standardize the raw DataFrame.
    Handles numeric/string types and ensures full_address has a placeholder.
    """
    logger.info("Cleaning data")
    if df.empty: logger.warning("Input DataFrame to clean_data is empty."); return df
    df = df.copy()

    logger.info(f"Columns RECEIVED by clean_data: {df.columns.tolist()}")
    if 'CARD_CODE' not in df.columns: logger.error("CARD_CODE missing at START of clean_data!")

    # Numeric Field Conversion
    # ... (AMOUNT, QUANTITY conversion remains the same) ...
    if 'AMOUNT' in df.columns:
        if df['AMOUNT'].dtype == 'object':
            amount_series = df['AMOUNT'].str.replace(',', '', regex=False)
        else:
            amount_series = df['AMOUNT'] 
        df['AMOUNT'] = pd.to_numeric(amount_series, errors='coerce')
        nan_count = df['AMOUNT'].isna().sum()
        if nan_count > 0: logger.warning(f"Coerced {nan_count} non-numeric values in AMOUNT column to NaN.")
        df['AMOUNT'] = df['AMOUNT'].fillna(0.0)
    else:
        logger.warning("AMOUNT column not found."); df['AMOUNT'] = 0.0

    if 'QUANTITY' in df.columns:
        if df['QUANTITY'].dtype == 'object':
            quantity_series = df['QUANTITY'].str.replace(',', '', regex=False)
        else:
            quantity_series = df['QUANTITY']
        df['QUANTITY'] = pd.to_numeric(quantity_series, errors='coerce')
        nan_count = df['QUANTITY'].isna().sum()
        if nan_count > 0: logger.warning(f"Coerced {nan_count} non-numeric values in QUANTITY column to NaN.")
        df['QUANTITY'] = df['QUANTITY'].fillna(0.0)
    else:
        logger.warning("QUANTITY column not found."); df['QUANTITY'] = 0.0

    # Date Conversion
    # ... (POSTINGDATE conversion remains the same) ...
    if 'POSTINGDATE' in df.columns:
        if not pd.api.types.is_datetime64_any_dtype(df['POSTINGDATE']): df['POSTINGDATE'] = pd.to_datetime(df['POSTINGDATE'], errors='coerce')
        mask_valid_date = df['POSTINGDATE'].notna()
        df.loc[mask_valid_date, 'year'] = df.loc[mask_valid_date, 'POSTINGDATE'].dt.year
        df.loc[mask_valid_date, 'quarter'] = df.loc[mask_valid_date, 'POSTINGDATE'].dt.quarter
        df.loc[mask_valid_date, 'month'] = df.loc[mask_valid_date, 'POSTINGDATE'].dt.month
        df.loc[mask_valid_date, 'week'] = df.loc[mask_valid_date, 'POSTINGDATE'].dt.isocalendar().week.astype('Int64') # Ensure Int64
        df['year'] = df['year'].astype('Int64'); df['quarter'] = df['quarter'].astype('Int64'); df['month'] = df['month'].astype('Int64')
        if not mask_valid_date.all(): logger.warning("Found NaT values in POSTINGDATE.")
    else: logger.error("POSTINGDATE column missing."); df['year']=pd.NA; df['quarter']=pd.NA; df['month']=pd.NA; df['week']=pd.NA


    # Address Cleaning
    # ... (full_address remains the same) ...
    address_components = ['ADDRESS', 'CITY', 'STATE', 'ZIPCODE']
    if all(col in df.columns for col in address_components):
        for col in address_components: df[col] = df[col].fillna('').astype(str).str.strip()
        df['full_address'] = (df['ADDRESS'] + df['CITY'].apply(lambda x:', '+x if x else '') + df['STATE'].apply(lambda x:', '+x if x else '') + df['ZIPCODE'].apply(lambda x:' '+x if x else ''))
        df['full_address'] = df['full_address'].str.strip().str.replace(r'^\s*,\s*|\s*,\s*$', '', regex=True).str.strip()
        df['full_address'] = np.where(df['full_address'].str.strip() == '', 'Address Not Available', df['full_address'])
    else: logger.warning(f"Missing address components. Setting placeholder."); df['full_address'] = 'Address Not Available'


    # Other Column Cleaning
    # *** MODIFICATION: Add ITEM and DESCRIPTION to string cleaning loop ***
    for col in ['SalesRep', 'SlpName', 'Distributor', 'CARD_CODE', 'ITEM', 'DESCRIPTION', 'CUSTOMERID', 'NAME']: # Ensure CARD_CODE is cased correctly
        if col in df.columns:
            df[col] = df[col].fillna('').astype(str).str.strip()
        else:
            logger.warning(f"Column '{col}' not found during cleaning, creating empty.")
            df[col] = ''


    # Final Validation
    # ... (validation remains the same) ...
    if 'CARD_CODE' in df.columns: # Check again after potential creation
         df['CARD_CODE'] = df['CARD_CODE'].fillna('').astype(str).str.strip()
         initial_rows = len(df); 
         # Also ensure ITEM is not empty if it's critical for your downstream logic, or handle empty SKUs
         df = df[(df['CARD_CODE'] != '') & (df['POSTINGDATE'].notna())] 
         rows_removed = initial_rows - len(df)
         if rows_removed > 0: logger.warning(f"Removed {rows_removed} rows due to missing CARD_CODE or POSTINGDATE.")
    else: logger.error("CRITICAL: CARD_CODE column not found after cleaning.")
    if df.empty: logger.warning("DataFrame is empty after cleaning.")


    logger.info(f"Data cleaning complete. {len(df)} valid rows remain.")
    return df

def collapse_purchases_by_date(df: pd.DataFrame) -> pd.DataFrame:
    """
    Collapse purchase data so each row corresponds to a unique (CARD_CODE, POSTINGDATE)
    and compute the total purchase amount for that day.
    """
    logger.info("Collapsing purchases by date")
    
    collapsed_df = df.groupby(['CARD_CODE', 'POSTINGDATE'], dropna=False).apply(
        lambda group: pd.Series({
            'name': group['NAME'].iloc[0] if not group['NAME'].empty else '',
            'full_address': group['full_address'].iloc[0] if not group['full_address'].empty else '',
            'total_purchase': (group['AMOUNT'] * group['QUANTITY']).sum(),
            'year': group['year'].iloc[0],
            'quarter': group['quarter'].iloc[0],
            'month': group['month'].iloc[0],
            'week': group['week'].iloc[0],
            'SalesRep': group['SalesRep'].iloc[0] if not group['SalesRep'].empty else '',
            'SlpName': group['SlpName'].iloc[0] if not group['SlpName'].empty else '',
            'Distributor': group['Distributor'].iloc[0] if not group['Distributor'].empty else ''
        })
    ).reset_index()
    
    collapsed_df = collapsed_df.sort_values(by=['CARD_CODE', 'POSTINGDATE'])
    logger.info(f"Collapsed to {len(collapsed_df)} unique date-account combinations")

    total_purchase_sum = collapsed_df['total_purchase'].sum()
    logger.info(f"Total purchase amount after collapsing: ${total_purchase_sum:.2f}")
    
    return collapsed_df

def compute_store_predictions(collapsed_df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute store-level predictions from the collapsed data, using CARD_CODE as identifier.
    """
    logger.info("Computing account predictions")
    
    # Ensure POSTINGDATE is datetime
    collapsed_df['POSTINGDATE'] = pd.to_datetime(collapsed_df['POSTINGDATE'], errors='coerce')
    
    def pred_func(group):
        sorted_dates = group['POSTINGDATE'].dropna().sort_values()
        if len(sorted_dates) == 0:
            last_date = pd.NaT
            median_interval = 0
            last_amount = 0.0
        else:
            last_date = sorted_dates.iloc[-1]
            last_amount = group.loc[group['POSTINGDATE'] == last_date, 'total_purchase'].sum()
            if len(sorted_dates) == 1:
                median_interval = 30
            else:
                intervals = sorted_dates.diff().dropna().dt.days
                median_interval = int(intervals.median())
        next_date = last_date + timedelta(days=median_interval) if pd.notnull(last_date) else pd.NaT
        account_total = group['total_purchase'].sum()
        
        # Use the most recent values for these attributes
        latest_idx = group['POSTINGDATE'].idxmax() if not group['POSTINGDATE'].empty else None
        name = group.loc[latest_idx, 'name'] if latest_idx is not None else ''
        full_address = group.loc[latest_idx, 'full_address'] if latest_idx is not None else ''
        sales_rep = group.loc[latest_idx, 'SalesRep'] if latest_idx is not None else ''
        sales_rep_name = group.loc[latest_idx, 'SlpName'] if latest_idx is not None else ''
        distributor = group.loc[latest_idx, 'Distributor'] if latest_idx is not None else ''
        
        return pd.Series({
            'name': name,
            'full_address': full_address,
            'last_purchase_date': last_date,
            'last_purchase_amount': last_amount,
            'median_interval_days': median_interval,
            'next_expected_purchase_date': next_date,
            'account_total': account_total,
            'sales_rep': sales_rep,
            'sales_rep_name': sales_rep_name,
            'distributor': distributor
        })
    
    predictions = collapsed_df.groupby(['CARD_CODE'], dropna=False).apply(pred_func).reset_index()
    logger.info(f"Generated predictions for {len(predictions)} accounts")

    total_account_sum = predictions['account_total'].sum()
    logger.info(f"Total account_total sum: ${total_account_sum:.2f}")
    
    return predictions


def calculate_yoy_metrics(yearly_historical_df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate Year-over-Year metrics for each account using purely yearly data.

    Parameters
    ----------
    yearly_historical_df : pd.DataFrame
        DataFrame with historical revenue data aggregated ONLY by year.

    Returns
    -------
    pd.DataFrame
        DataFrame with YoY metrics by card_code for the latest year.
    """
    logger.info("Calculating year-over-year metrics from yearly data")

    # Input is already filtered yearly data
    yearly_data = yearly_historical_df.copy()

    # Sort data by card_code and year is crucial for shift()
    yearly_data = yearly_data.sort_values(['CARD_CODE', 'year'])

    # Calculate YoY changes using shift
    yearly_data['prev_year_revenue'] = yearly_data.groupby('CARD_CODE')['total_revenue'].shift(1)
    yearly_data['prev_year_transactions'] = yearly_data.groupby('CARD_CODE')['transaction_count'].shift(1)

    # Calculate growth percentages safely
    yearly_data['yoy_revenue_growth'] = yearly_data.apply(
        lambda row: ((row['total_revenue'] - row['prev_year_revenue']) / row['prev_year_revenue'] * 100)
        if pd.notnull(row['prev_year_revenue']) and row['prev_year_revenue'] > 0
        # Handle cases where prev year revenue was 0 or negative (e.g., returns)
        else (100.0 if row['total_revenue'] > 0 else 0.0) if pd.notnull(row['prev_year_revenue']) and row['prev_year_revenue'] == 0
        else 0.0, # Default to 0 if no previous year data
        axis=1
    )

    yearly_data['yoy_transaction_growth'] = yearly_data.apply(
        lambda row: ((row['transaction_count'] - row['prev_year_transactions']) / row['prev_year_transactions'] * 100)
        if pd.notnull(row['prev_year_transactions']) and row['prev_year_transactions'] > 0
        else (100.0 if row['transaction_count'] > 0 else 0.0) if pd.notnull(row['prev_year_transactions']) and row['prev_year_transactions'] == 0
        else 0.0, # Default to 0 if no previous year data
        axis=1
    )

    # Get the metrics for the *latest* year available for each account
    # Use idxmax to find the row corresponding to the max year per group
    latest_year_idx = yearly_data.loc[yearly_data.groupby('CARD_CODE')['year'].idxmax()]

    yoy_metrics = latest_year_idx[[
        'CARD_CODE',
        'year', # Keep track of which year this metric applies to
        'yoy_revenue_growth',
        'yoy_transaction_growth'
    ]].rename(columns={'year': 'latest_metric_year'}) # Rename for clarity when merging

    logger.info(f"Calculated YoY metrics for {len(yoy_metrics)} accounts based on their latest year")

    return yoy_metrics

def transform_days_overdue(days_overdue):
    """
    Transforms a positive days_overdue (e.g., 1, 2, 263) into a decaying value.
    Lower days_overdue => larger output, so newly overdue is prioritized higher.
    """
    # Example decay rate. Increase or decrease k to tune how quickly large values approach 0.
    k = 0.05
    
    # If days_overdue=0, it means not overdue. We'll return 0 in that case.
    if days_overdue <= 0:
        return 0
    
    # Exponential decay: e^(-k * x)
    # The smaller the days_overdue, the larger the result.
    return math.exp(-k * days_overdue)




def calculate_rfm_scores(df):
    """
    Calculate RFM (Recency, Frequency, Monetary) scores for each account
    Updated to use more detailed segmentation and to work with the card_code identifier
    
    Parameters
    ----------
    df : pd.DataFrame
        DataFrame with account metrics including last_purchase_date, 
        purchase_frequency, and account_total
        
    Returns
    -------
    pd.DataFrame
        DataFrame with added RFM scores and segments
    """
    logger.info("Calculating RFM scores")
    
    # Make a copy to avoid modifying the original
    df_copy = df.copy()
    
    # Ensure we have the required columns with default values for missing ones
    required_cols = ['last_purchase_date', 'purchase_frequency', 'account_total']
    for col in required_cols:
        if col not in df_copy.columns:
            logger.warning(f"{col} not found in DataFrame. Adding with default values.")
            if col == 'last_purchase_date':
                df_copy[col] = pd.NaT
            else:
                df_copy[col] = 0
    
    # Calculate recency score (1-5, 5 is most recent)
    if 'days_since_last_purchase' in df_copy.columns:
        # Use pre-calculated days since last purchase if available
        days_array = df_copy['days_since_last_purchase'].fillna(999)
    elif 'last_purchase_date' in df_copy.columns:
        # Calculate days since last purchase
        today = pd.Timestamp('today').normalize()
        days_array = df_copy['last_purchase_date'].apply(
            lambda x: (today - pd.Timestamp(x).normalize()).days if pd.notnull(x) else 999
        )
    else:
        days_array = pd.Series([999] * len(df_copy))
    
    # Create quintiles for recency (1-5)
    try:
        if len(df_copy) >= 5:  # Need at least 5 records for quintiles
            df_copy['recency_score'] = pd.qcut(
                days_array, 
                q=5, 
                labels=[5, 4, 3, 2, 1],  # Reversed so lower days = higher score
                duplicates='drop'
            ).fillna(1).astype(int)
        else:
            # For small datasets, use a simpler calculation
            df_copy['recency_score'] = days_array.apply(
                lambda days: 5 if days <= 30 else (
                    4 if days <= 60 else (
                        3 if days <= 90 else (
                            2 if days <= 120 else 1
                        )
                    )
                )
            )
    except Exception as e:
        logger.error(f"Error calculating recency score: {str(e)}")
        # Use default values if calculation fails
        df_copy['recency_score'] = 3
    
    # Calculate frequency score (1-5, 5 is most frequent)
    try:
        if 'purchase_frequency' in df_copy.columns:
            # Make sure purchase_frequency is numeric and has no NaN
            df_copy['purchase_frequency'] = pd.to_numeric(df_copy['purchase_frequency'], errors='coerce').fillna(0)
            
            if len(df_copy) >= 5:
                df_copy['frequency_score'] = pd.qcut(
                    df_copy['purchase_frequency'].rank(method='first'), 
                    q=5, 
                    labels=[1, 2, 3, 4, 5],
                    duplicates='drop'
                ).fillna(1).astype(int)
            else:
                # For small datasets, use thresholds
                df_copy['frequency_score'] = df_copy['purchase_frequency'].apply(
                    lambda freq: 5 if freq >= 20 else (
                        4 if freq >= 10 else (
                            3 if freq >= 5 else (
                                2 if freq >= 2 else 1
                            )
                        )
                    )
                )
        else:
            df_copy['frequency_score'] = 3  # Default middle value
    except Exception as e:
        logger.error(f"Error calculating frequency score: {str(e)}")
        # Use default values if calculation fails
        df_copy['frequency_score'] = 3
    
    # Calculate monetary score (1-5, 5 is highest value)
    try:
        if 'account_total' in df_copy.columns:
            # Make sure account_total is numeric and has no NaN
            df_copy['account_total'] = pd.to_numeric(df_copy['account_total'], errors='coerce').fillna(0)
            
            if len(df_copy) >= 5:
                df_copy['monetary_score'] = pd.qcut(
                    df_copy['account_total'].rank(method='first'), 
                    q=5, 
                    labels=[1, 2, 3, 4, 5],
                    duplicates='drop'
                ).fillna(1).astype(int)
            else:
                # For small datasets, use thresholds
                df_copy['monetary_score'] = df_copy['account_total'].apply(
                    lambda total: 5 if total >= 10000 else (
                        4 if total >= 5000 else (
                            3 if total >= 1000 else (
                                2 if total >= 500 else 1
                            )
                        )
                    )
                )
        else:
            df_copy['monetary_score'] = 3  # Default middle value
    except Exception as e:
        logger.error(f"Error calculating monetary score: {str(e)}")
        # Use default values if calculation fails
        df_copy['monetary_score'] = 3
    
    # Calculate combined RFM score (3-15)
    df_copy['rfm_score'] = df_copy['recency_score'] + df_copy['frequency_score'] + df_copy['monetary_score']
    
    # Assign RFM segments based on individual and combined scores
    try:
        conditions = [
            # Champions: recent customers who buy often and spend the most
            (df_copy['recency_score'] >= 4) & (df_copy['frequency_score'] >= 4) & (df_copy['monetary_score'] >= 4),
            
            # Loyal Customers: buy regularly and recently, but not big spenders
            (df_copy['recency_score'] >= 3) & (df_copy['frequency_score'] >= 3) & (df_copy['monetary_score'] >= 3),
            
            # Potential Loyalists: recent customers with average frequency and spend
            (df_copy['recency_score'] >= 4) & (df_copy['frequency_score'] >= 2) & (df_copy['monetary_score'] >= 2),
            
            # New Customers: bought recently but not frequently yet
            (df_copy['recency_score'] >= 4) & (df_copy['frequency_score'] <= 2),
            
            # Promising: recent, but not frequent and low spend
            (df_copy['recency_score'] >= 3) & (df_copy['frequency_score'] <= 2) & (df_copy['monetary_score'] <= 2),
            
            # At Risk: above average recency, frequency and monetary values
            (df_copy['recency_score'] <= 2) & (df_copy['frequency_score'] >= 3) & (df_copy['monetary_score'] >= 3),
            
            # Can't Lose Them: used to buy frequently and spend a lot, but haven't purchased recently
            (df_copy['recency_score'] <= 2) & (df_copy['frequency_score'] >= 4) & (df_copy['monetary_score'] >= 4),
            
            # Hibernating: last purchase was long ago, low purchase frequency and amount
            (df_copy['recency_score'] <= 2) & (df_copy['frequency_score'] <= 2),
            
            # Lost: lowest scores in all criteria
            (df_copy['recency_score'] <= 1) & (df_copy['frequency_score'] <= 1) & (df_copy['monetary_score'] <= 2)
        ]
        
        choices = [
            'Champions',
            'Loyal Customers',
            'Potential Loyalists',
            'New Customers',
            'Promising',
            'At Risk',
            'Can\'t Lose',
            'Hibernating',
            'Lost'
        ]
        
        df_copy['rfm_segment'] = np.select(conditions, choices, default='Need Attention')
    except Exception as e:
        logger.error(f"Error assigning RFM segments: {str(e)}")
        # Use a default segment if calculation fails
        df_copy['rfm_segment'] = 'Need Attention'
    
    logger.info(f"Calculated RFM scores for {len(df_copy)} accounts")
    
    return df_copy

def calculate_health_score(df):
    """
    Calculate account health score (0-100) based on multiple factors,
    incorporating new cadence and pace metrics.
    """
    logger.info(f"Calculating health scores (v2) for {len(df)} accounts...")
    df_copy = df.copy()

    # --- Ensure necessary input columns exist with safe defaults ---
    required_cols = {
        'days_since_last_purchase': 9999, 'purchase_frequency': 0, 'account_total': 0.0,
        'avg_interval_py': None, 'avg_interval_cytd': None, # Cadence inputs
        'pace_vs_ly': None, 'yep_revenue': None, # Pace inputs (YEP needed for scaling)
    }
    for col, default in required_cols.items():
        if col not in df_copy.columns:
            logger.warning(f"Health Score: Missing input column '{col}'. Using default/None.")
            df_copy[col] = default
        # Fill NaNs specifically for calculations where needed
        if col in ['days_since_last_purchase', 'purchase_frequency', 'account_total']:
             df_copy[col] = pd.to_numeric(df_copy[col], errors='coerce').fillna(0) # Use 0 for these NaNs

    # --- Component Weights (Adjust as needed - should sum to 90) ---
    W_RECENCY = 25
    W_FREQUENCY = 15 # Reduced weight slightly
    W_MONETARY = 10 # Reduced weight slightly
    W_CADENCE = 25 # Increased weight for cadence consistency/improvement
    W_PACE = 15 # Added weight for pace vs last year

    # --- Calculate Components ---

    # 1. Recency Component (0-W_RECENCY) - Unchanged
    # Higher score for more recent purchases (using days_since_last_purchase)
    df_copy['recency_component'] = df_copy['days_since_last_purchase'].apply(
        lambda days: max(0, W_RECENCY - min(W_RECENCY, (days / 30.0))) # Lose points per month approx
    ).fillna(0).round(1)

    # 2. Frequency Component (0-W_FREQUENCY) - Unchanged (uses lifetime frequency)
    # Higher score for higher lifetime purchase frequency (capped)
    # Consider if CYTD frequency might be better here? For now, use lifetime.
    df_copy['frequency_component'] = df_copy['purchase_frequency'].apply(
        lambda freq: min(W_FREQUENCY, (freq / 5.0)) # Scale: Max points at 75 purchases? Adjust scale.
    ).fillna(0).round(1)

    # 3. Monetary Component (0-W_MONETARY) - Unchanged (uses lifetime total)
    # Higher score for higher lifetime spend (capped)
    # Consider if YEP Revenue might be better? For now, use lifetime.
    df_copy['monetary_component'] = df_copy['account_total'].apply(
        lambda total: min(W_MONETARY, (total / 5000.0)) # Scale: Max points at $50k total? Adjust scale.
    ).fillna(0).round(1)

    # 4. Cadence Component (0-W_CADENCE) - NEW
    # Score based on CYTD interval and comparison to PY interval
    def calculate_cadence_score(row):
        cytd = row['avg_interval_cytd']
        py = row['avg_interval_py']
        score = 0.0

        # Part 1: Score based on CYTD interval length (lower interval = better)
        # Scale: e.g., 7 days = max points, 90 days = 0 points
        if pd.notna(cytd):
            points_cytd = max(0.0, min(W_CADENCE * 0.6, W_CADENCE * 0.6 * (1 - (max(0, cytd - 7)) / (90 - 7)))) # Linear decay
            score += points_cytd
        else:
            score += W_CADENCE * 0.1 # Small base score if no CYTD interval yet

        # Part 2: Bonus/Penalty for change vs PY interval
        # Scale: e.g., improving by >7 days = max bonus, worsening by >30 days = max penalty
        if pd.notna(cytd) and pd.notna(py) and py > 0:
            lag = cytd - py
            # Bonus for shortening interval (negative lag)
            bonus = max(0.0, min(W_CADENCE * 0.4, W_CADENCE * 0.4 * (-lag / 7.0)))
            # Penalty for lengthening interval (positive lag)
            penalty = max(0.0, min(W_CADENCE * 0.4, W_CADENCE * 0.4 * (lag / 30.0)))
            score += bonus - penalty
        elif pd.notna(cytd) and pd.isna(py): # New this year with interval
             score += W_CADENCE * 0.2 # Bonus for establishing cadence
        # If only PY exists or neither, no bonus/penalty applied

        return max(0.0, min(W_CADENCE, score)) # Ensure score is within 0-W_CADENCE

    df_copy['cadence_component'] = df_copy.apply(calculate_cadence_score, axis=1).round(1)

    # 5. Pace Component (0-W_PACE) - NEW
    # Score based on projected performance vs last year
    def calculate_pace_score(row):
        pace_usd = row['pace_vs_ly']
        py_revenue = (row['yep_revenue'] - pace_usd) if pd.notna(row['yep_revenue']) and pd.notna(pace_usd) else 0

        if pd.isna(pace_usd) or py_revenue <= 0: # Cannot calculate pace % or no base last year
             if pd.notna(row['yep_revenue']) and row['yep_revenue'] > 0: # Pacing positive but no LY base
                  return W_PACE * 0.75 # Good score if generating revenue from zero
             else:
                  return W_PACE * 0.25 # Low score if no pace calculable and no YEP

        pace_pct = (pace_usd / py_revenue) * 100.0
        # Scale: e.g., +25% pace = max points, -50% pace = 0 points
        score = W_PACE * ( (pace_pct + 50) / (25 + 50) ) # Linear scale from -50% to +25% maps to 0-W_PACE
        return max(0.0, min(W_PACE, score)) # Clamp score

    df_copy['pace_component'] = df_copy.apply(calculate_pace_score, axis=1).round(1)

    # --- Calculate Overall Health Score ---
    component_cols = ['recency_component', 'frequency_component', 'monetary_component',
                      'cadence_component', 'pace_component']
    # Sum available components
    df_copy['health_score'] = df_copy[component_cols].sum(axis=1).round(1)

    # --- Assign Health Categories ---
    conditions = [
        df_copy['health_score'] >= 80, df_copy['health_score'] >= 60,
        df_copy['health_score'] >= 40, df_copy['health_score'] >= 20 ]
    choices = ['Excellent', 'Good', 'Average', 'Poor']
    df_copy['health_category'] = np.select(conditions, choices, default='Critical')

    logger.info(f"Calculated health scores (v2) for {len(df_copy)} accounts")
    return df_copy
    


def calculate_enhanced_priority_score(df):
    logger.info(f"Calculating enhanced priority scores (v2) for {len(df)} records...")
    if df.empty: return df.assign(enhanced_priority_score=pd.Series(dtype=float))
    df_copy = df.copy()

    req_cols = {
        'days_overdue':0,
        'avg_purchase_cycle_days':30, # *** CHANGED KEY NAME HERE ***
        'account_total':0.0,
        'rfm_segment':'Need Attention', 'health_score':50.0,
        'pace_vs_ly': 0.0,
        'yep_revenue': 0.0
    }
    for col, default in req_cols.items():
        if col not in df_copy.columns:
            logger.warning(f"Enhanced Priority: Missing input column '{col}'. Using default.")
            df_copy[col] = default
        df_copy[col] = df_copy[col].fillna(default)

    numeric_cols = ['days_overdue', 'avg_purchase_cycle_days', 'account_total', 'health_score', 'pace_vs_ly', 'yep_revenue'] # *** CHANGED HERE ***
    for col in numeric_cols:
         df_copy[col] = pd.to_numeric(df_copy[col], errors='coerce').fillna(0)

    # Urgency component
    df_copy['urgency_component'] = df_copy.apply(
        lambda r: min(30, 30 * min(1, r['days_overdue'] / max(1, r['avg_purchase_cycle_days']))), # *** CHANGED HERE ***
        axis=1
    ).fillna(0)
    # ... rest of the function remains the same using other columns ...
    df_copy['value_component'] = df_copy['account_total'].apply( lambda total: min(20, total / 5000) ).fillna(0)
    rfm_priority_map = { 'Champions': 1, 'Loyal Customers': 2, 'Potential Loyalists': 4, 'New Customers': 3, 'Promising': 5, 'Need Attention': 6, 'At Risk': 8, "Can't Lose": 10, 'Hibernating': 7, 'Lost': 2 }
    df_copy['rfm_component'] = df_copy['rfm_segment'].map(rfm_priority_map).fillna(6).clip(0, 10)
    df_copy['health_component'] = ((100 - df_copy['health_score']) * 0.10).clip(0, 10).fillna(5)
    def calculate_pace_priority(row):
        pace_usd = row['pace_vs_ly']; py_revenue = (row['yep_revenue'] - pace_usd) if pd.notna(row['yep_revenue']) and pd.notna(pace_usd) else 0
        if pd.isna(pace_usd) or py_revenue <= 0: return 3
        pace_pct = (pace_usd / py_revenue) * 100.0; score = 10 * ( (25 - pace_pct) / (25 - (-25)) )
        return max(0.0, min(10, score))
    df_copy['pace_component'] = df_copy.apply(calculate_pace_priority, axis=1).round(1)
    df_copy['enhanced_priority_score'] = df_copy[['urgency_component', 'value_component', 'rfm_component', 'health_component', 'pace_component']].sum(axis=1).round(1)
    logger.info("Finished enhanced priority score (v2) calculation.")
    return df_copy



def generate_snapshots(account_predictions_df: pd.DataFrame, yearly_historical_revenue_df: pd.DataFrame) -> pd.DataFrame:
    """
    Generate YEARLY account snapshots. Corrected fillna logic.

    Parameters:
    -----------
    account_predictions_df : pd.DataFrame
        The main dataframe containing current account prediction data (health score etc.)
    yearly_historical_revenue_df : pd.DataFrame
        DataFrame with historical revenue data aggregated ONLY by year.

    Returns:
    --------
    pd.DataFrame
        DataFrame with YEARLY snapshot data ready for database insertion.
    """
    logger.info("Generating YEARLY account snapshots")

    # --- Input Validation ---
    if 'card_code' not in account_predictions_df.columns:
         # Try to map from CARD_CODE if available in predictions
         if 'CARD_CODE' in account_predictions_df.columns:
              account_predictions_df = account_predictions_df.rename(columns={'CARD_CODE': 'card_code'})
              logger.warning("Renamed 'CARD_CODE' to 'card_code' in account_predictions_df.")
         else:
              logger.error("Missing 'card_code' column in account_predictions_df for snapshots.")
              return pd.DataFrame() # Return empty if key missing

    if 'CARD_CODE' not in yearly_historical_revenue_df.columns:
         logger.error("Missing 'CARD_CODE' column in yearly_historical_revenue_df for snapshots.")
         return pd.DataFrame() # Return empty if key missing

    # --- Merge Data ---
    logger.info("Merging yearly historical data with current predictions for snapshots.")
    # Ensure the key columns are of the same type if possible (usually string)
    account_predictions_df['card_code'] = account_predictions_df['card_code'].astype(str)
    yearly_historical_revenue_df['CARD_CODE'] = yearly_historical_revenue_df['CARD_CODE'].astype(str)

    # Select only necessary columns from predictions to avoid duplicate columns after merge
    prediction_cols_for_snapshot = [
        'card_code', 'account_total', 'health_score'
    ]
    # Check if columns exist before selecting
    prediction_cols_for_snapshot = [
        col for col in prediction_cols_for_snapshot if col in account_predictions_df.columns
    ]
    missing_pred_cols = set(['card_code', 'account_total', 'health_score', 'churn_risk_score']) - set(prediction_cols_for_snapshot)
    if missing_pred_cols:
        logger.warning(f"Missing prediction columns needed for snapshots: {missing_pred_cols}. Snapshots might lack these fields.")


    merged_df = pd.merge(
        yearly_historical_revenue_df,
        account_predictions_df[prediction_cols_for_snapshot],
        left_on='CARD_CODE',
        right_on='card_code',
        how='left' # Keep all historical rows, add prediction data if available
    )
    logger.info(f"Merged DataFrame shape for snapshots: {merged_df.shape}")

    # --- Create Snapshots List ---
    snapshots = []
    required_hist_cols = ['CARD_CODE', 'year', 'total_revenue', 'transaction_count']
    missing_hist_cols = [col for col in required_hist_cols if col not in merged_df.columns]
    if missing_hist_cols:
        logger.error(f"Critical error: Missing required historical columns after merge: {missing_hist_cols}")
        return pd.DataFrame()

    logger.info("Iterating through merged data to create snapshot dictionaries.")
    for _, row in merged_df.iterrows():
        try:
            # Basic validation for essential fields
            if pd.isna(row['CARD_CODE']) or pd.isna(row['year']):
                 logger.warning(f"Skipping snapshot row due to missing CARD_CODE or year.")
                 continue

            year = int(row['year'])
            # Create snapshot date (e.g., last day of year)
            snapshot_date = pd.Timestamp(f"{year}-12-31")

            snapshot = {
                'card_code': str(row['CARD_CODE']), # Ensure string
                'snapshot_date': snapshot_date,
                'year': year,
                'yearly_revenue': row.get('total_revenue'),       # Revenue IN that year (from historical)
                'yearly_purchases': row.get('transaction_count'), # Purchases IN that year (from historical)
                'account_total': row.get('account_total'),        # Overall total (from prediction)
                'health_score': row.get('health_score')
            }
            snapshots.append(snapshot)
        except Exception as e:
            logger.error(f"Error creating snapshot dict for card_code={row.get('CARD_CODE', 'N/A')}, year={row.get('year', 'N/A')}: {str(e)}")
            continue

    # --- Create DataFrame and Handle Missing Values ---
    if snapshots:
        snapshots_df = pd.DataFrame(snapshots)
        logger.info(f"Created initial snapshots DataFrame with {len(snapshots_df)} records.")

        # Define columns and their fill values
        fill_values = {
            'yearly_revenue': 0.0,
            'yearly_purchases': 0,
            'account_total': 0.0,
             # For scores, filling with 0.0 might be okay, or use a specific indicator like -1, or keep NaN
             # Let's fill with 0.0 for now, assuming missing score means 0 contribution
            'health_score': 0.0
        }

        for col, fill_val in fill_values.items():
            if col in snapshots_df.columns:
                 #logger.debug(f"Filling NaNs in column '{col}' with {fill_val}")
                 # Ensure numeric type before filling for numeric fills
                 if isinstance(fill_val, (int, float)):
                     # Coerce to numeric, setting errors='coerce' turns unparseable things into NaN
                     snapshots_df[col] = pd.to_numeric(snapshots_df[col], errors='coerce')
                 # Now apply fillna
                 snapshots_df[col] = snapshots_df[col].fillna(fill_val)
                 # Optionally ensure integer type if fill_val is int
                 if isinstance(fill_val, int):
                      snapshots_df[col] = snapshots_df[col].astype(int)
            else:
                 logger.warning(f"Column '{col}' not found in snapshots DataFrame for fillna.")

        # Final check for NaNs in key columns
        check_cols = ['card_code', 'snapshot_date', 'year', 'yearly_revenue', 'yearly_purchases']
        nan_counts = snapshots_df[check_cols].isna().sum()
        if nan_counts.sum() > 0:
             logger.warning(f"NaN values still present in key snapshot columns after fillna:\n{nan_counts[nan_counts > 0]}")
             # Consider dropping rows with NaN in critical columns like card_code, year, snapshot_date
             # snapshots_df = snapshots_df.dropna(subset=['card_code', 'year', 'snapshot_date'])

        logger.info(f"Generated final {len(snapshots_df)} yearly account snapshots after handling NaNs.")
        return snapshots_df
    else:
        logger.warning("No yearly snapshots were generated (list was empty).")
        return pd.DataFrame() # Return empty DataFrame consistent with schema if possible


def filter_by_period(historical_df, period_type='yearly'):
    """
    Filter historical revenue data to a single period type to avoid double-counting.
    
    Parameters
    ----------
    historical_df : pd.DataFrame
        Historical revenue DataFrame with multiple period types
    period_type : str, optional
        Period type to filter by ('yearly', 'quarterly', 'monthly', 'weekly')
        
    Returns
    -------
    pd.DataFrame
        Filtered DataFrame with only the specified period type
    """
    # IMPORTANT: Historical revenue data contains the same revenue reflected across
    # multiple period types (yearly, quarterly, monthly, weekly). Always filter to
    # a single period type before calculating totals or account-level metrics to
    # avoid double-counting.

    return historical_df[historical_df['period_type'] == period_type]


def verify_no_double_counting(historical_df):
    """Verify that historical data is correctly filtered to avoid double-counting."""
    if 'period_type' in historical_df.columns and historical_df['period_type'].nunique() > 1:
        logger.warning("DOUBLE-COUNTING RISK: Multiple period types detected in historical data")
        yearly_total = historical_df[historical_df['period_type'] == 'yearly']['total_revenue'].sum()
        total = historical_df['total_revenue'].sum()
        logger.warning(f"Yearly total: ${yearly_total:.2f}, All periods: ${total:.2f}")
        logger.warning(f"Ratio: {total/yearly_total:.2f}x")
        return False
    return True



def calculate_correct_revenue(df):
    """
    A simple helper function to calculate total revenue directly from the raw data.
    This provides a sanity check against the more complex aggregation methods.
    """
    logger.info("Calculating direct revenue from raw data")
    
    # Ensure numeric types
    df['AMOUNT'] = pd.to_numeric(df['AMOUNT'], errors='coerce').fillna(0)
    df['QUANTITY'] = pd.to_numeric(df['QUANTITY'], errors='coerce').fillna(0)
    
    # Calculate revenue directly
    revenue = (df['AMOUNT'] * df['QUANTITY']).sum()
    
    logger.info(f"Direct calculation of total revenue: ${revenue:.2f}")
    
    # If you want a breakdown by year
    df['POSTINGDATE'] = pd.to_datetime(df['POSTINGDATE'], errors='coerce')
    df['year'] = df['POSTINGDATE'].dt.year
    
    # Group by year and calculate revenue
    yearly_revenue = df.groupby('year').apply(
        lambda group: (group['AMOUNT'] * group['QUANTITY']).sum()
    ).reset_index(name='yearly_revenue')
    
    #for _, row in yearly_revenue.iterrows():
    #    logger.info(f"Year {row['year']}: ${row['yearly_revenue']:.2f}")
    
    return revenue, yearly_revenue



# --- Main Recalculation Function (Refactored for SQLAlchemy 2.x & New Metrics) ---
def recalculate_predictions_and_metrics(session=None):
    """
    Main function to recalculate all account metrics and predictions from the database.
    This version includes fixes for YEP calculation, Growth Engine logic, data handling, 
    last_purchase_date recalculation from actual transaction data, and rolling SKU analysis.
    """
    logger.info("--- Starting recalculation from DB (v10 - YEP/Growth/DataFrame/LastPurchaseDate/RollingSKU Fixes) ---")

    if session is None:
        session = db.session

    start_time = time.time()
    try:
        # === 1. Get All Unique Canonical Codes from Predictions ===
        logger.info("Querying all existing canonical codes from predictions...")
        stmt_codes = select(AccountPrediction.canonical_code).distinct()
        all_canonical_codes_results = db.session.execute(stmt_codes).scalars().all()
        all_canonical_codes = list(all_canonical_codes_results)
        if not all_canonical_codes:
            logger.warning("No existing accounts found in AccountPrediction table to recalculate.")
            return pd.DataFrame()
        logger.info(f"Found {len(all_canonical_codes)} unique canonical codes to process.")

        current_year = datetime.now().year
        prev_year = current_year - 1
        start_of_year_dt = datetime(current_year, 1, 1)
        now_dt = datetime.now()

        # === 2. Pre-fetch data that can be looked up ===
        logger.info("Pre-fetching YoY metrics for all accounts...")
        all_yoy_metrics_df = calculate_yoy_metrics_from_db(current_year, db.session)
        yoy_dict = all_yoy_metrics_df.set_index('canonical_code').to_dict('index') if not all_yoy_metrics_df.empty else {}

        logger.info("Pre-fetching SKU-Based Product Coverage (based on latest year) for all accounts...")
        all_coverage_df = calculate_product_coverage_from_db(db.session)
        coverage_dict = {}
        if not all_coverage_df.empty:
            # Use .to_dict('index') for efficiency
            coverage_dict = all_coverage_df.set_index('canonical_code').to_dict('index')
        
        # --- Pre-fetch the NEW rolling SKU analysis for ALL accounts ---
        logger.info("Pre-fetching rolling 12-month SKU analysis...")
        all_sku_analysis_data = calculate_rolling_sku_analysis(all_canonical_codes, db.session)
        logger.info(f"Pre-fetched SKU analysis for {len(all_sku_analysis_data)} accounts.")
        
        logger.info(f"Pre-fetching complete - YoY data for {len(yoy_dict)} accounts, SKU Coverage data for {len(coverage_dict)} accounts, Rolling SKU Analysis for {len(all_sku_analysis_data)} accounts")

        # === 3. Process accounts in batches ===
        batch_size = 100
        all_results_df = pd.DataFrame()

        for i in range(0, len(all_canonical_codes), batch_size): # LOOP 1: Iterate through BATCHES
            batch_codes = all_canonical_codes[i:i + batch_size]
            logger.info(f"Processing batch {i//batch_size + 1} of {math.ceil(len(all_canonical_codes)/batch_size)}, {len(batch_codes)} accounts...")
            batch_start_time = time.time()

            # Fetch base prediction data for the current batch
            pred_base_cols = [
                AccountPrediction.id, AccountPrediction.canonical_code, AccountPrediction.name,
                AccountPrediction.full_address, AccountPrediction.customer_id, AccountPrediction.sales_rep,
                AccountPrediction.sales_rep_name, AccountPrediction.distributor, AccountPrediction.last_purchase_date,
                AccountPrediction.last_purchase_amount, AccountPrediction.avg_interval_py,
                AccountPrediction.base_card_code, AccountPrediction.ship_to_code
            ]
            pred_base_stmt = select(*pred_base_cols).where(AccountPrediction.canonical_code.in_(batch_codes))
            predictions_base_results = db.session.execute(pred_base_stmt).all()
            
            # --- FIX: Create empty DataFrame with correct columns if results are empty ---
            pred_base_cols_names = [col.name for col in pred_base_cols]
            if predictions_base_results:
                predictions_base_df = pd.DataFrame(predictions_base_results, columns=pred_base_cols_names)
            else:
                predictions_base_df = pd.DataFrame(columns=pred_base_cols_names)

            if predictions_base_df.empty:
                logger.info(f"Batch {i//batch_size + 1} had no base prediction data. Skipping.")
                continue

            # Fetch transaction data for CYTD for this batch
            cytd_trans_cols_selected = [Transaction.canonical_code, Transaction.posting_date, Transaction.revenue]
            cytd_trans_stmt = select(*cytd_trans_cols_selected).where(
                Transaction.canonical_code.in_(batch_codes), Transaction.year == current_year
            ).order_by(Transaction.canonical_code, Transaction.posting_date)
            cytd_transactions_batch_results = db.session.execute(cytd_trans_stmt).all()
            cytd_trans_cols_names = ['canonical_code', 'posting_date', 'revenue']
            if cytd_transactions_batch_results:
                cytd_transactions_df = pd.DataFrame(cytd_transactions_batch_results, columns=cytd_trans_cols_names)
                cytd_transactions_df['posting_date'] = pd.to_datetime(cytd_transactions_df['posting_date'])
            else:
                cytd_transactions_df = pd.DataFrame(columns=cytd_trans_cols_names)

            # --- FIX: Fetch ALL transaction data for this batch INCLUDING REVENUE ---
            all_trans_cols_selected = [Transaction.canonical_code, Transaction.posting_date, Transaction.revenue]
            all_trans_stmt = select(*all_trans_cols_selected).where(
                Transaction.canonical_code.in_(batch_codes)
            ).order_by(Transaction.canonical_code, Transaction.posting_date)
            all_transactions_batch_results = db.session.execute(all_trans_stmt).all()
            all_trans_cols_names = ['canonical_code', 'posting_date', 'revenue']
            if all_transactions_batch_results:
                all_transactions_df = pd.DataFrame(all_transactions_batch_results, columns=all_trans_cols_names)
                all_transactions_df['posting_date'] = pd.to_datetime(all_transactions_df['posting_date'])
            else:
                all_transactions_df = pd.DataFrame(columns=all_trans_cols_names)

            # Fetch ALL historical yearly revenue data for this batch
            hist_cols_selected = [
                AccountHistoricalRevenue.canonical_code, AccountHistoricalRevenue.year,
                AccountHistoricalRevenue.total_revenue, AccountHistoricalRevenue.transaction_count,
                AccountHistoricalRevenue.yearly_products_json
            ]
            hist_stmt = select(*hist_cols_selected).where(AccountHistoricalRevenue.canonical_code.in_(batch_codes))
            historical_results_batch = db.session.execute(hist_stmt).all()
            hist_cols_names = ['canonical_code', 'year', 'total_revenue', 'transaction_count', 'yearly_products_json']
            if historical_results_batch:
                historical_df_batch = pd.DataFrame(historical_results_batch, columns=hist_cols_names)
            else:
                historical_df_batch = pd.DataFrame(columns=hist_cols_names)

            current_batch_processed_metrics = []

            for _, pred_row_current_account in predictions_base_df.iterrows(): # LOOP 2: Iterate through ACCOUNTS IN THIS BATCH
                code = pred_row_current_account['canonical_code']
                
                # --- FIX: Initialize metric_row with the ID from the predictions table ---
                metric_row = {'id': pred_row_current_account['id'], 'canonical_code': code}
                
                # --- Start of calculations for this account ---
                acc_cytd_trans = cytd_transactions_df[cytd_transactions_df['canonical_code'] == code].copy()
                acc_all_trans = all_transactions_df[all_transactions_df['canonical_code'] == code].copy()
                acc_hist_all_years_df = historical_df_batch[historical_df_batch['canonical_code'] == code].copy()

                # --- FIX: Explicitly find the NEW last_purchase_date and amount from all transactions ---
                last_purchase_datetime = None
                last_purchase_amount = 0.0
                if not acc_all_trans.empty:
                    # Ensure date column is datetime
                    acc_all_trans.loc[:, 'posting_date'] = pd.to_datetime(acc_all_trans['posting_date'], errors='coerce')
                    # Find the absolute latest date
                    last_purchase_datetime = acc_all_trans['posting_date'].max()
                    
                    if pd.notna(last_purchase_datetime):
                        # Filter for transactions on that specific day and sum their revenue
                        last_day_trans = acc_all_trans[acc_all_trans['posting_date'] == last_purchase_datetime]
                        last_purchase_amount = last_day_trans['revenue'].sum()
                # --- END FIX ---

                cytd_revenue = acc_cytd_trans["revenue"].sum()

                cytd_order_count = 0
                avg_order_amount_cytd = None
                if not acc_cytd_trans.empty:
                    # Use .loc to explicitly signal modification on the DataFrame.
                    # This avoids the SettingWithCopyWarning.
                    acc_cytd_trans.loc[:, 'posting_date'] = pd.to_datetime(acc_cytd_trans['posting_date'], errors='coerce')
                    
                    # It's also safer to drop any rows where the date conversion failed (became NaT)
                    # before trying to use the .dt accessor.
                    valid_dates = acc_cytd_trans.dropna(subset=['posting_date'])
                    
                    cytd_order_count = valid_dates['posting_date'].dt.normalize().nunique()
                    
                    if cytd_order_count > 0:
                        avg_order_amount_cytd = cytd_revenue / cytd_order_count
                
                median_interval_days = 30 
                if not acc_all_trans.empty:
                    unique_all_dates = pd.Series(acc_all_trans['posting_date'].dt.normalize().sort_values().unique())
                    if len(unique_all_dates) > 1:
                        all_intervals = unique_all_dates.diff().dt.days.dropna()
                        if not all_intervals.empty:
                            median_interval_days = int(max(1, all_intervals.median()))

                avg_interval_cytd = None
                if not acc_cytd_trans.empty:
                    unique_cytd_dates = pd.Series(acc_cytd_trans['posting_date'].dt.normalize().sort_values().unique())
                    if len(unique_cytd_dates) > 1:
                        cytd_intervals = unique_cytd_dates.diff().dt.days.dropna()
                        if not cytd_intervals.empty:
                            avg_interval_cytd = float(cytd_intervals.mean())
                
                next_expected_purchase_date = last_purchase_datetime + timedelta(days=median_interval_days) if pd.notna(last_purchase_datetime) else None
                days_overdue = max(0, (now_dt.date() - next_expected_purchase_date.date()).days) if pd.notna(next_expected_purchase_date) and next_expected_purchase_date < now_dt else 0
                avg_interval_py = safe_float(pred_row_current_account.get("avg_interval_py"), None)

                # --- FIX: YEP Calculation Adjusted for Data Lag ---
                yep_revenue = None
                if cytd_revenue > 0 and not acc_cytd_trans.empty:
                    # Use the last transaction date for the CYTD revenue as the reference point
                    '''
                    last_cytd_date = acc_cytd_trans['posting_date'].max().date()
                    # Ensure date is valid and in the current year
                    if last_cytd_date >= start_of_year_dt.date():
                        # Calculate days from start of year to the last transaction date
                        days_for_ytd_accumulation = (last_cytd_date - start_of_year_dt.date()).days + 1
                        yep_revenue = (cytd_revenue / days_for_ytd_accumulation) * 365
                    '''
                    days_for_ytd_accumulation = (now_dt.date() - start_of_year_dt.date()).days + 1
                    if days_for_ytd_accumulation < 30:  # No meaningful run-rate has been established yet
                        yep_revenue = cytd_revenue  # Set YEP equal to CYTD (i.e., no projection)
                    else:
                        # Otherwise, calculate the projection as intended
                        yep_revenue = (cytd_revenue / days_for_ytd_accumulation) * 365

                
                py_hist_row_df = acc_hist_all_years_df[acc_hist_all_years_df['year'] == prev_year]
                py_total_revenue = float(py_hist_row_df['total_revenue'].sum()) if not py_hist_row_df.empty else 0.0
                #pace_vs_ly = (yep_revenue - py_total_revenue) if yep_revenue is not None else None
                
                # --- CORRECTED AND ROBUST PACE CALCULATION ---
                if yep_revenue is not None and py_total_revenue is not None:
                    if py_total_revenue > 0:
                        # Calculates the percentage and assigns it to pace_vs_ly
                        pace_vs_ly = ((yep_revenue - py_total_revenue) / py_total_revenue) * 100
                    elif yep_revenue > 0:
                        # Handles "New Growth" case
                        pace_vs_ly = None 
                    else:
                        # Handles 0 vs 0 case
                        pace_vs_ly = 0.0
                else:
                    # Handles cases where pace can't be calculated
                    pace_vs_ly = None
                
                account_total = acc_hist_all_years_df['total_revenue'].sum()
                purchase_frequency = acc_hist_all_years_df['transaction_count'].sum()
                days_since_last_purchase = (now_dt - last_purchase_datetime).days if pd.notna(last_purchase_datetime) else 9999

                all_historical_skus_for_this_account = set()
                if not acc_hist_all_years_df.empty:
                    for yearly_skus_json_str in acc_hist_all_years_df['yearly_products_json'].dropna():
                        try:
                            skus_list_from_json = json.loads(yearly_skus_json_str)
                            if isinstance(skus_list_from_json, list):
                                all_historical_skus_for_this_account.update(str(s) for s in skus_list_from_json if s)
                        except json.JSONDecodeError: pass
                products_purchased_json = safe_json_dumps(sorted(list(all_historical_skus_for_this_account)))

                trend_results = None
                if not acc_hist_all_years_df.empty:
                    yearly_revenues_for_trend_list = acc_hist_all_years_df[['year', 'total_revenue']].rename(
                        columns={'total_revenue': 'revenue'}
                    ).to_dict('records')
                    trend_results = calculate_yearly_revenue_trend(yearly_revenues_for_trend_list)

                # --- FIX: Growth Engine Robustness ---
                target_yep_plus_1_pct = None
                additional_revenue_needed_eoy = None
                suggested_next_purchase_amount = None
                recommended_products_next_purchase_json = json.dumps([])
                growth_engine_message = "Data insufficient for growth suggestion."

                def format_currency_py_helper(value):
                    return f"${value:,.0f}" if value is not None and value >= 1000 else f"${value:.0f}" if value is not None else "$0"
                
                baseline_for_target = py_total_revenue if py_total_revenue > 0 else (yep_revenue if yep_revenue and yep_revenue > 0 else 0)
                
                if baseline_for_target > 0 and cytd_revenue is not None:
                    # Set a more aggressive target if pacing well vs LY
                    is_pacing_well = pace_vs_ly is not None and pace_vs_ly >= 0
                    growth_target_pct = 0.10 if is_pacing_well else 0.01

                    target_total_for_calc = baseline_for_target * (1 + growth_target_pct)
                    additional_needed = target_total_for_calc - cytd_revenue

                    target_yep_plus_1_pct = round(target_total_for_calc, 2)
                    additional_revenue_needed_eoy = round(additional_needed, 2)

                    if additional_needed <= 0:
                        growth_engine_message = f"Excellent! On track or has exceeded the +{growth_target_pct*100:.0f}% target (Target: {format_currency_py_helper(target_yep_plus_1_pct)})."
                    else:
                        days_left_in_year = max(1, (date(current_year, 12, 31) - now_dt.date()).days)
                        remaining_purchases_est = max(1.0, days_left_in_year / median_interval_days if median_interval_days > 0 else 1.0)
                        
                        amount_per_purchase = additional_needed / remaining_purchases_est
                        suggested_next_purchase_amount = round(max(50.0, min(amount_per_purchase, additional_needed)), 2)
                        
                        growth_engine_message = f"To reach {format_currency_py_helper(target_yep_plus_1_pct)} (+{growth_target_pct*100:.0f}% vs baseline), aim for orders around ~{format_currency_py_helper(suggested_next_purchase_amount)}."

                    # Populate recommended products from coverage data
                    cov_data_account = coverage_dict.get(code, {})
                    missing_skus_json = cov_data_account.get('missing_top_products_json')
                    if missing_skus_json:
                        try:
                             recommended_products_next_purchase_json = safe_json_dumps(json.loads(missing_skus_json)[:3])
                        except Exception: pass
                
                # --- Update metric_row with all calculated values ---
                yoy_data_for_acc = yoy_dict.get(code, {})
                cov_data_for_acc = coverage_dict.get(code, {})
                
                # +++ Add Rolling SKU Analysis to metric_row +++
                account_sku_analysis = all_sku_analysis_data.get(code, [])

                clean_sku_analysis = []
                if account_sku_analysis:
                    for sku_dict in account_sku_analysis:
                        clean_dict = {}
                        for key, value in sku_dict.items():
                            if isinstance(value, float) and math.isnan(value):
                                clean_dict[key] = None
                            else:
                                clean_dict[key] = value
                        clean_sku_analysis.append(clean_dict)
                # --- END NEW SAFETY CHECK ---

                purchased_skus_12m = set()
                for sku_data in account_sku_analysis:
                    if sku_data.get('current_12m_rev', 0) > 0:  # Has revenue in last 12 months
                        item_code = str(sku_data.get('item_code', '')).strip()
                        if item_code:
                            purchased_skus_12m.add(item_code)

                # Find which Top 30 SKUs they ARE carrying
                carried_top30_products = []
                for sku_data in account_sku_analysis:
                    if sku_data.get('is_top_30') and sku_data.get('current_12m_rev', 0) > 0:
                        carried_top30_products.append(str(sku_data.get('item_code', '')))

                coverage_12m = (len(purchased_skus_12m & TOP_30_SET) / len(TOP_30_SET) * 100) if TOP_30_SET else 0
           
                # Find which Top 30 SKUs are missing
                missing_top30_products = []
                for top30_sku in TOP_30_SET:
                    if top30_sku not in purchased_skus_12m:
                        missing_top30_products.append({
                            "sku": top30_sku,
                            "description": "Top 30 Product",  # Could enhance with actual descriptions
                            "reason": "Not purchased in last 12 months"
                        })
                
                metric_row.update({
                    'name': pred_row_current_account.get('name'), 'full_address': pred_row_current_account.get('full_address'),
                    'customer_id': pred_row_current_account.get('customer_id'), 'sales_rep': pred_row_current_account.get('sales_rep'),
                    'sales_rep_name': pred_row_current_account.get('sales_rep_name'), 'distributor': pred_row_current_account.get('distributor'),
                    'base_card_code': pred_row_current_account.get('base_card_code'), 'ship_to_code': pred_row_current_account.get('ship_to_code'),
                    'last_purchase_date': last_purchase_datetime, 'last_purchase_amount': last_purchase_amount,
                    'account_total': account_total, 'purchase_frequency': purchase_frequency, 'days_since_last_purchase': days_since_last_purchase,
                    'median_interval_days': int(median_interval_days), 'avg_purchase_cycle_days': float(median_interval_days),
                    'next_expected_purchase_date': next_expected_purchase_date, 'days_overdue': days_overdue,
                    'avg_interval_py': avg_interval_py, 'avg_interval_cytd': avg_interval_cytd, 'cytd_revenue': cytd_revenue,
                    'yep_revenue': yep_revenue, 'pace_vs_ly': pace_vs_ly, 'py_total_revenue': py_total_revenue,
                    'products_purchased': products_purchased_json,
                    'yoy_revenue_growth': yoy_data_for_acc.get('yoy_revenue_growth'),
                    'yoy_purchase_count_growth': yoy_data_for_acc.get('yoy_purchase_count_growth'),
                    #'product_coverage_percentage': cov_data_for_acc.get('product_coverage_percentage'),
                    'product_coverage_percentage': coverage_12m,
                    #'carried_top_products_json': cov_data_for_acc.get('carried_top_products_json'),
                    'carried_top_products_json': json.dumps(carried_top30_products),
                    #'missing_top_products_json': cov_data_for_acc.get('missing_top_products_json'),
                    'missing_top_products_json': json.dumps(missing_top30_products),
                    'revenue_trend_slope': trend_results['slope'] if trend_results else None,
                    'revenue_trend_r_squared': trend_results['r_squared'] if trend_results else None,
                    'revenue_trend_intercept': trend_results['intercept'] if trend_results else None,
                    'target_yep_plus_1_pct': target_yep_plus_1_pct,
                    'additional_revenue_needed_eoy': additional_revenue_needed_eoy,
                    'suggested_next_purchase_amount': suggested_next_purchase_amount,
                    'recommended_products_next_purchase_json': recommended_products_next_purchase_json,
                    'growth_engine_message': growth_engine_message,
                    'avg_order_amount_cytd': avg_order_amount_cytd,
                    'rolling_sku_analysis_json': json.dumps(clean_sku_analysis) if clean_sku_analysis is not None else None
                })

                current_batch_processed_metrics.append(metric_row)
            # --- END of LOOP 2 (accounts in batch) ---

            if current_batch_processed_metrics:
                batch_df_for_scoring = pd.DataFrame(current_batch_processed_metrics)
                if not batch_df_for_scoring.empty:
                    logger.info(f"Calculating scores for batch {i//batch_size + 1} ({len(batch_df_for_scoring)} accounts)...")
                    try:
                        batch_df_for_scoring = calculate_rfm_scores(batch_df_for_scoring)
                        batch_df_for_scoring = calculate_health_score(batch_df_for_scoring)
                        batch_df_for_scoring = calculate_enhanced_priority_score(batch_df_for_scoring)
                        if 'priority_score' not in batch_df_for_scoring.columns:
                            batch_df_for_scoring['priority_score'] = 0.0 
                        all_results_df = pd.concat([all_results_df, batch_df_for_scoring], ignore_index=True)
                    except Exception as score_err:
                        logger.error(f"Error calculating scores for batch: {score_err}", exc_info=True)
            
            logger.info(f"Batch {i//batch_size + 1} finished in {time.time() - batch_start_time:.2f}s. Total results processed: {len(all_results_df)}")
        # --- END of LOOP 1 (batches) ---

        if all_results_df.empty:
            logger.warning("No results generated after processing all batches.")
            return pd.DataFrame()

        # --- FIX: Validate final DataFrame has 'id' column ---
        if 'id' not in all_results_df.columns: 
            logger.error("CRITICAL: 'id' column missing from final calculated results. Bulk update would fail. Aborting.")
            return pd.DataFrame()
        
        # Filter out rows with invalid IDs before returning
        all_results_df = all_results_df[all_results_df['id'].notna()]
        
        logger.info(f"--- Recalculation complete (Duration: {time.time() - start_time:.2f}s). Returning {len(all_results_df)} prediction records with rolling SKU analysis. ---")
        return all_results_df

    except Exception as e:
        logger.error(f"CRITICAL Error during recalculate_predictions_and_metrics: {e}", exc_info=True)
        return pd.DataFrame()

# Keep the __main__ block for standalone testing if needed
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger.warning("Running pipeline.py as __main__. Ensure you have a Flask app context for DB operations if calling recalculate_predictions_and_metrics.")
