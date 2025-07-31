#!/usr/bin/env python
"""
Store Normalization Script

This script normalizes store data by:
1. Connecting to your production database
2. Identifying duplicate stores with the same base card_code
3. Generating a mapping file to standardize card_codes
4. Optionally updating the database with corrected mappings

Usage:
    python store_normalization.py --analyze      # Just analyze and create mapping file
    python store_normalization.py --apply        # Apply mapping to database
"""

import argparse
import os
import re
import difflib
import pandas as pd
import logging
from sqlalchemy import create_engine, text
import numpy as np
from datetime import datetime
import json

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"store_normalization_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# --- Configuration ---
# Pull from environment or config file in production
DB_CONNECTION_STRING = "sqlite:///data/sales_reminder.db"  # Update this for your local testing
MAPPING_FILE = "card_code_mapping.csv"
SIMILARITY_THRESHOLD = 0.85  # Adjust based on your data
# --- End Configuration ---


def normalize_address(address):
    """Normalize address for better matching with improved error handling."""
    try:
        if not address or not isinstance(address, str):
            return ""
        
        # Convert to uppercase and remove extra spaces
        addr = address.upper().strip()
        
        # First, check if this is a PO Box - they should be handled specially
        if re.search(r'P\.?O\.?\s*BOX', addr):
            # Just extract the PO Box part
            po_match = re.search(r'P\.?O\.?\s*BOX\s*(\d+)', addr)
            if po_match:
                return f"PO BOX {po_match.group(1)}"
            return "PO BOX"  # If we can't parse the number
        
        # Remove commas and everything after them (usually city, state)
        addr = re.sub(r',.*$', '', addr)
        
        # Handle "ADDRESS NOT AVAILABLE"
        if "ADDRESS NOT AVAILABLE" in addr or "NOT AVAILABLE" in addr:
            return "NO ADDRESS"
        
        # Normalize AVE/AVENUE consistently to AVE
        addr = re.sub(r'AVEUE|AVENUE', 'AVE', addr)
        
        # Normalize common street type abbreviations
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
        
        for pattern, replacement in street_types.items():
            addr = re.sub(pattern, replacement, addr)
        
        # Standardize directionals
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
        
        for pattern, replacement in directions.items():
            addr = re.sub(pattern, replacement, addr)
        
        # Fix specific OCR errors and common misspellings
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
        
        for misspelled, correct in spelling_fixes.items():
            addr = addr.replace(misspelled, correct)
        
        # Remove unit/suite designations
        addr = re.sub(r'\s+(?:UNIT|STE|SUITE|APT)[.\s]*(?:[A-Z0-9]+)?', '', addr, flags=re.IGNORECASE)
        
        # Remove single letters at end of words (often OCR errors or unit designations)
        addr = re.sub(r'\b(\d+)\s+([A-Z]+)\s+([A-Z])\b', r'\1 \2', addr)
        
        # Clean up letter suffixes attached to numbers (123A → 123)
        addr = re.sub(r'(\d+)[A-Z](?=\s|$)', r'\1', addr)
        
        # Clean up numeric spelling variations (5147/5145)
        # Extract street number for potential fuzzy matching
        street_num_match = re.match(r'^(\d+)', addr)
        original_street_num = street_num_match.group(1) if street_num_match else ""
        
        # Remove all non-alphanumeric characters except spaces
        addr = re.sub(r'[^\w\s]', ' ', addr)
        
        # Normalize whitespace
        addr = re.sub(r'\s+', ' ', addr).strip()
        
        # For very short addresses that are just a street number and one word,
        # return the normalized format plus original street number for better matching
        if len(addr.split()) <= 2 and original_street_num:
            return f"{original_street_num} {' '.join(addr.split()[1:])}"
        
        # For addresses with just a street number, normalize to "NUM STREET"
        if re.match(r'^\d+\s*$', addr):
            return f"{addr.strip()} STREET"
        
        # Return the entire normalized address
        return addr
        
    except Exception as e:
        logger.warning(f"Error normalizing address '{address}': {e}")
        return ""  # Return empty string on error


def normalize_store_name(name):
    """Normalize store names for better matching."""
    if not name or not isinstance(name, str):
        return ""
    
    # Convert to uppercase and remove extra spaces
    name = name.upper().strip()
    
    # Remove common prefixes
    prefixes = ["THE "]
    for prefix in prefixes:
        if name.startswith(prefix):
            name = name[len(prefix):]
    
    # Replace common variations
    replacements = {
        " & ": " AND ", 
        "#": " ",
        "NO.": " ",
        "MRKT": "MARKET",
        "MKT": "MARKET",
        "HLTH": "HEALTH",
        "NATRL": "NATURAL",
        "NUTR": "NUTRITION",
        "NUTRITN": "NUTRITION",
        "CTR": "CENTER",
        "CNTR": "CENTER",
        "FARMS": "FARMERS",
        "PATCH": "PATCH",
        "WHEATERY": "WHEATERY",
        "'S": "S",
        "-": " ", 
        "_": " ",
        ",": " ",
        ".": " "
    }
    
    for old, new in replacements.items():
        name = name.replace(old, new)
    
    # Remove common suffixes
    suffixes = [" INC", " LLC", " CO", " MARKET", " FOODS", " 1", " 2"]
    for suffix in suffixes:
        if name.endswith(suffix):
            name = name[:-len(suffix)]
    
    # Remove numbers and store numbers
    name = re.sub(r' \d+$', '', name)  # Remove numbers at the end
    name = re.sub(r'#\s*\d+', '', name)  # Remove store numbers like "#047"
    
    # Normalize whitespace
    name = ' '.join(name.split())
    
    return name


def get_base_card_code(card_code):
    """Extract the base card_code from a potentially concatenated one."""
    if not card_code or not isinstance(card_code, str):
        return ""
    
    # If card_code contains underscore, take the first part
    parts = card_code.split('_')
    return parts[0]


def load_card_code_exceptions(exceptions_file='card_code_exceptions.csv'):
    """Load manual exceptions for card code mapping."""
    if not os.path.exists(exceptions_file):
        logger.info(f"No exceptions file found at {exceptions_file}")
        return {}
    
    try:
        exceptions_df = pd.read_csv(exceptions_file)
        exceptions = dict(zip(exceptions_df['card_code'], exceptions_df['canonical_card_code']))
        logger.info(f"Loaded {len(exceptions)} card code mapping exceptions")
        return exceptions
    except Exception as e:
        logger.error(f"Error loading exceptions: {e}")
        return {}


def fetch_store_data():
    """Fetch store data from the database."""
    try:
        # Create engine
        engine = create_engine(DB_CONNECTION_STRING)
        
        # Query all stores from AccountPrediction table
        logger.info("Fetching store data from database...")
        query = """
        SELECT 
            card_code, name, full_address, distributor
        FROM 
            account_predictions
        """
        
        # Execute query and load into DataFrame
        with engine.connect() as conn:
            df = pd.read_sql(query, conn)
        
        logger.info(f"Fetched {len(df)} store records from database")
        return df
    
    except Exception as e:
        logger.error(f"Error fetching store data: {e}")
        return pd.DataFrame()


def find_duplicate_stores(df, similarity_threshold=0.85):
    """
    Find potential duplicate stores with improved matching.
    """
    logger.info("Analyzing stores for potential duplicates...")
    
    # Skip if DataFrame is empty
    if df.empty:
        logger.warning("No store data to analyze")
        return []
    
    # Log columns for debugging
    logger.info(f"DataFrame columns: {df.columns.tolist()}")
    
    # Check if required columns exist
    if 'full_address' not in df.columns:
        logger.error("'full_address' column not found in DataFrame!")
        df['full_address'] = ""  # Add empty column to prevent errors
    
    if 'name' not in df.columns:
        logger.error("'name' column not found in DataFrame!")
        df['name'] = ""  # Add empty column to prevent errors
        
    # Add normalized columns
    df['base_code'] = df['card_code'].apply(get_base_card_code)
    
    # Safely apply normalization
    df['norm_address'] = df['full_address'].fillna('').apply(lambda x: normalize_address(x) if x else "")
    df['norm_name'] = df['name'].fillna('').apply(lambda x: normalize_store_name(x) if x else "")
    
    # Extract street number for fuzzy matching
    def extract_street_num(addr):
        num_match = re.match(r'^(\d+)', addr)
        if num_match:
            return num_match.group(1)
        return ""
    
    df['street_num'] = df['norm_address'].apply(extract_street_num)
    
    # Log sample of normalized data
    logger.debug("Sample of normalized data:")
    logger.debug(df[['card_code', 'base_code', 'name', 'norm_name', 'full_address', 'norm_address', 'street_num']].head(5))
    
    # Group by base_code and find potential duplicates
    duplicates = []
    
    # Track progress
    total_groups = df['base_code'].nunique()
    processed = 0
    
    for base_code, group in df.groupby('base_code'):
        processed += 1
        if processed % 100 == 0:
            logger.info(f"Processing group {processed}/{total_groups}")
        
        # Skip if only one record with this base_code
        if len(group) <= 1:
            continue
        
        # Special case for PO Box vs street address
        has_po_box = group['norm_address'].str.contains('PO BOX', case=False).any()
        has_street = group['norm_address'].str.contains(r'^\d+\s+', regex=True).any()
        
        if has_po_box and has_street:
            logger.info(f"Base code {base_code} has both PO BOX and street addresses - treating as duplicates")
            card_codes = group['card_code'].tolist()
            for i in range(len(card_codes)):
                for j in range(i+1, len(card_codes)):
                    if card_codes[i] != base_code and card_codes[j] != base_code:
                        duplicates.append({
                            'base_code': base_code,
                            'card_code1': card_codes[i],
                            'card_code2': card_codes[j],
                            'combined_similarity': 1.0,
                            'addr_similarity': 1.0,
                            'name_similarity': 1.0,
                            'addr1': group['full_address'].iloc[i],
                            'addr2': group['full_address'].iloc[j],
                            'name1': group['name'].iloc[i],
                            'name2': group['name'].iloc[j],
                            'reason': 'po_box_and_street'
                        })
            continue
        
        # Compare every pair of addresses in this group
        card_codes = group['card_code'].tolist()
        addresses = group['norm_address'].tolist()
        names = group['norm_name'].tolist()
        street_nums = group['street_num'].tolist()
        original_addrs = group['full_address'].tolist()
        
        for i in range(len(card_codes)):
            for j in range(i+1, len(card_codes)):
                # Skip if already canonical
                if card_codes[i] == base_code or card_codes[j] == base_code:
                    continue
                
                # Calculate base similarity scores
                addr_similarity = difflib.SequenceMatcher(None, addresses[i], addresses[j]).ratio()
                name_similarity = difflib.SequenceMatcher(None, names[i], names[j]).ratio()
                
                # Check for special cases
                
                # Case 1: Same street number, highly similar rest of address
                street_num_match = False
                if street_nums[i] and street_nums[j]:
                    if street_nums[i] == street_nums[j]:
                        # Exact street number match - compare rest of address
                        rest_i = addresses[i][len(street_nums[i]):].strip()
                        rest_j = addresses[j][len(street_nums[j]):].strip()
                        
                        # If the rest of the address is similar enough, boost score
                        rest_similarity = difflib.SequenceMatcher(None, rest_i, rest_j).ratio()
                        if rest_similarity > 0.7:
                            addr_similarity = max(addr_similarity, 0.9)
                            street_num_match = True
                    else:
                        # Check for close street numbers
                        try:
                            num_i = int(street_nums[i])
                            num_j = int(street_nums[j])
                            # If within 5 and the rest is similar, likely same place
                            if abs(num_i - num_j) <= 5:
                                # Check rest of address
                                rest_i = addresses[i][len(street_nums[i]):].strip()
                                rest_j = addresses[j][len(street_nums[j]):].strip()
                                rest_similarity = difflib.SequenceMatcher(None, rest_i, rest_j).ratio()
                                
                                if rest_similarity > 0.8:
                                    addr_similarity = max(addr_similarity, 0.85)
                                    street_num_match = True
                        except ValueError:
                            pass
                
                # Case 2: Address patterns that often match despite different formatting
                # Common patterns in conflicts: "X MAIN" vs "X MAIN ST", etc.
                if not street_num_match and street_nums[i] and street_nums[j]:
                    # If addresses start with same number but one has "ST" or other suffix
                    words_i = addresses[i].split()
                    words_j = addresses[j].split()
                    
                    # Check if the first word (street number) is the same
                    if words_i[0] == words_j[0]:
                        # Remove common endings that cause false differences
                        common_suffixes = ['ST', 'RD', 'AVE', 'DR', 'BLVD', 'LN', 'CT', 'CIR', 'TER', 'PL']
                        
                        # Create sets of words minus these endings
                        set_i = set(w for w in words_i if w not in common_suffixes)
                        set_j = set(w for w in words_j if w not in common_suffixes)
                        
                        # If the core parts match well
                        overlap = len(set_i.intersection(set_j))
                        if overlap >= 2:  # They share at least 2 substantive words
                            addr_similarity = max(addr_similarity, 0.85)
                
                # Case 3: Original address patterns that suggest the same store
                # Look for patterns like "X STREET" vs "X STREET UNIT Y"
                orig_i = original_addrs[i] if i < len(original_addrs) else ""
                orig_j = original_addrs[j] if j < len(original_addrs) else ""
                
                # Check if one address is a substring of the other
                if orig_i and orig_j:
                    if orig_i in orig_j or orig_j in orig_i:
                        addr_similarity = max(addr_similarity, 0.9)
                
                # Calculate combined score
                combined_similarity = (0.7 * addr_similarity) + (0.3 * name_similarity)
                
                # Determine reason for match
                reason = 'similarity'
                if street_num_match:
                    reason = 'street_number_match'
                
                # Add to duplicates if similar enough
                if combined_similarity >= similarity_threshold:
                    duplicates.append({
                        'base_code': base_code,
                        'card_code1': card_codes[i],
                        'card_code2': card_codes[j],
                        'combined_similarity': combined_similarity,
                        'addr_similarity': addr_similarity,
                        'name_similarity': name_similarity,
                        'addr1': group['full_address'].iloc[i],
                        'addr2': group['full_address'].iloc[j],
                        'name1': group['name'].iloc[i],
                        'name2': group['name'].iloc[j],
                        'reason': reason
                    })
    
    logger.info(f"Found {len(duplicates)} potential duplicate pairs")
    return duplicates


def generate_mapping(duplicates, df, exceptions_file='card_code_exceptions.csv'):
    """
    Generate a mapping from duplicate card_codes to canonical card_codes.
    The canonical form is usually the base card_code.
    Incorporates manual exceptions from a CSV file.
    """
    logger.info("Generating card_code mapping...")
    
    # Load exceptions
    exceptions = load_card_code_exceptions(exceptions_file)
    logger.info(f"Loaded {len(exceptions)} manual exceptions")
    
    if not duplicates and not exceptions:
        logger.warning("No duplicates or exceptions found, generating empty mapping file")
        mapping_df = pd.DataFrame(columns=['original_card_code', 'canonical_card_code', 'reason'])
        mapping_df.to_csv(MAPPING_FILE, index=False)
        return {}
    
    # Create a graph of duplicate relationships to merge groups
    duplicate_graph = {}
    for dup in duplicates:
        code1, code2 = dup['card_code1'], dup['card_code2']
        base_code = dup['base_code']
        
        if code1 not in duplicate_graph:
            duplicate_graph[code1] = []
        if code2 not in duplicate_graph:
            duplicate_graph[code2] = []
        
        duplicate_graph[code1].append(code2)
        duplicate_graph[code2].append(code1)
    
    # Use connected components to find groups
    def find_connected_component(node, visited=None, component=None):
        if visited is None:
            visited = set()
        if component is None:
            component = []
        
        visited.add(node)
        component.append(node)
        
        for neighbor in duplicate_graph.get(node, []):
            if neighbor not in visited:
                find_connected_component(neighbor, visited, component)
        
        return component
    
    # Find all connected components
    all_nodes = set(duplicate_graph.keys())
    connected_components = []
    
    while all_nodes:
        node = next(iter(all_nodes))
        component = find_connected_component(node)
        connected_components.append(component)
        all_nodes -= set(component)
    
    logger.info(f"Found {len(connected_components)} distinct duplicate groups")
    
    # Generate mapping from connected components
    mapping = {}
    mapping_list = []
    
    # First, apply all exceptions
    for code, canonical in exceptions.items():
        mapping[code] = canonical
        mapping_list.append({
            'original_card_code': code,
            'canonical_card_code': canonical,
            'reason': 'manual_exception'
        })
        logger.debug(f"Applied exception mapping {code} -> {canonical}")
    
    # Then process connected components
    for component in connected_components:
        # Exclude codes that already have exceptions
        component = [code for code in component if code not in exceptions]
        if not component:
            continue
            
        # Find the base code for this group
        base_codes = set()
        for code in component:
            base_code = get_base_card_code(code)
            base_codes.add(base_code)
        
        # If there's only one base code, use it as canonical
        # Otherwise find the most common code in the original data
        if len(base_codes) == 1:
            canonical = next(iter(base_codes))
            reason = "single_base_code"
        else:
            # Use the most common base code in the dataset
            base_counts = {}
            for base in base_codes:
                base_counts[base] = len(df[df['base_code'] == base])
            
            canonical = max(base_counts, key=base_counts.get)
            reason = "most_common_base"
        
        # Add each component member to the mapping
        for code in component:
            # Skip if code is already the canonical form
            if code == canonical:
                continue
            
            mapping[code] = canonical
            mapping_list.append({
                'original_card_code': code,
                'canonical_card_code': canonical,
                'reason': reason
            })
            logger.debug(f"Mapping {code} -> {canonical}")
    
    # Create the mapping file
    mapping_df = pd.DataFrame(mapping_list)
    
    # Add duplicate detection reason
    reason_map = {}
    for dup in duplicates:
        reason_map[(dup['card_code1'], dup['card_code2'])] = dup['reason']
        reason_map[(dup['card_code2'], dup['card_code1'])] = dup['reason']
    
    # Add detailed reason to mapping file
    def get_detailed_reason(row):
        if row['reason'] == 'manual_exception':
            return 'manual_exception'
            
        for component in connected_components:
            if row['original_card_code'] in component:
                canonical = row['canonical_card_code']
                # Check if direct connection exists
                if (row['original_card_code'], canonical) in reason_map:
                    return reason_map[(row['original_card_code'], canonical)]
                elif (canonical, row['original_card_code']) in reason_map:
                    return reason_map[(canonical, row['original_card_code'])]
                else:
                    # Indirect connection through component
                    return f"connected_component_{row['reason']}"
        return row['reason']
    
    mapping_df['detailed_reason'] = mapping_df.apply(get_detailed_reason, axis=1)
    
    # Add similarity information for non-exception mappings
    for i, row in mapping_df.iterrows():
        orig = row['original_card_code']
        canon = row['canonical_card_code']
        
        if row['reason'] == 'manual_exception':
            # For manual exceptions, don't try to calculate similarity
            mapping_df.at[i, 'similarity'] = 1.0  # Use 1.0 to indicate manual override
            continue
            
        # Find similarity info for regular mappings
        similarity_info = next((dup for dup in duplicates 
                               if (dup['card_code1'] == orig and dup['card_code2'] == canon) or 
                                  (dup['card_code1'] == canon and dup['card_code2'] == orig)), None)
        
        if similarity_info:
            mapping_df.at[i, 'similarity'] = similarity_info['combined_similarity']
            mapping_df.at[i, 'addr_similarity'] = similarity_info['addr_similarity']
            mapping_df.at[i, 'name_similarity'] = similarity_info['name_similarity']
    
    # Save mapping to CSV
    mapping_df.to_csv(MAPPING_FILE, index=False)
    logger.info(f"Generated mapping for {len(mapping)} card_codes")
    
    return mapping


def update_product_coverage():
    """Update consolidated records with combined product coverage data."""
    logger.info("Updating consolidated records with combined product coverage data...")
    
    try:
        import json
        # Create engine
        engine = create_engine(DB_CONNECTION_STRING)
        
        with engine.begin() as conn:
            # First, get the base code for each card_code
            get_bases = text("""
                SELECT 
                    card_code,
                    substr(card_code, 1, instr(card_code || '_', '_') - 1) as base_code
                FROM account_predictions
            """)
            
            bases_result = conn.execute(get_bases)
            card_to_base = {row[0]: row[1] for row in bases_result}
            
            # Group by base code
            base_groups = {}
            for card, base in card_to_base.items():
                if base not in base_groups:
                    base_groups[base] = []
                base_groups[base].append(card)
            
            # Process each base group with multiple entries
            for base, cards in base_groups.items():
                if len(cards) > 1:
                    # Check if base exists as a card code
                    check_base = text("""
                        SELECT 1 FROM account_predictions WHERE card_code = :base
                    """)
                    base_exists = conn.execute(check_base, {"base": base}).fetchone() is not None
                    
                    if base_exists:
                        cards_json = json.dumps(cards)
                        
                        # Get all product data from cards in this group
                        get_products = text("""
                            SELECT 
                                card_code,
                                carried_top_products_json,
                                missing_top_products_json,
                                product_coverage_percentage
                            FROM account_predictions
                            WHERE card_code IN (SELECT value FROM json_each(:cards))
                        """)
                        
                        products_result = conn.execute(get_products, {"cards": cards_json})
                        all_products = []
                        all_coverage = []
                        
                        # Combine all product lists
                        for row in products_result:
                            card_code = row[0]
                            carried_json = row[1]
                            missing_json = row[2]
                            coverage = row[3]
                            
                            if carried_json:
                                try:
                                    carried_products = json.loads(carried_json)
                                    if isinstance(carried_products, list):
                                        all_products.extend(carried_products)
                                        all_coverage.append(coverage)
                                except Exception as e:
                                    logger.warning(f"Error parsing carried products for {card_code}: {e}")
                        
                        # Remove duplicates and sort
                        unique_products = sorted(list(set(all_products)))
                        
                        # Calculate new coverage percentage
                        if unique_products:
                            # If we loaded TOP_30_SET here, we could calculate exactly
                            # Instead, use the highest coverage percentage as an approximation
                            new_coverage = max(all_coverage) if all_coverage else 0
                        else:
                            new_coverage = 0
                        
                        # Update the base record with combined product data
                        update_base_products = text("""
                            UPDATE account_predictions
                            SET 
                                carried_top_products_json = :carried_json,
                                product_coverage_percentage = :coverage
                            WHERE card_code = :base
                        """)
                        
                        conn.execute(update_base_products, {
                            "base": base,
                            "carried_json": json.dumps(unique_products),
                            "coverage": new_coverage
                        })
                        
                        logger.info(f"Updated {base} with combined product list ({len(unique_products)} products, {new_coverage:.2f}% coverage)")
            
            logger.info("Finished updating consolidated records with combined product coverage.")
            
    except Exception as e:
        logger.error(f"Error updating product coverage: {e}")
        logger.error(f"Error details: {str(e)}")


def update_canonical_with_latest_dates():
    """Update all canonical records with the most recent purchase dates from their group."""
    logger.info("Updating canonical records with most recent purchase dates...")
    
    try:
        # Create engine
        engine = create_engine(DB_CONNECTION_STRING)
        
        with engine.begin() as conn:
            # First, get the base code for each card_code
            get_bases = text("""
                SELECT 
                    card_code,
                    substr(card_code, 1, instr(card_code || '_', '_') - 1) as base_code
                FROM account_predictions
            """)
            
            bases_result = conn.execute(get_bases)
            card_to_base = {row[0]: row[1] for row in bases_result}
            
            # Group by base code
            base_groups = {}
            for card, base in card_to_base.items():
                if base not in base_groups:
                    base_groups[base] = []
                base_groups[base].append(card)
            
            # For each base group with multiple entries, find the most recent purchase
            for base, cards in base_groups.items():
                if len(cards) > 1:
                    cards_json = json.dumps(cards)
                    
                    # Find the most recent purchase date and its associated card code
                    get_most_recent = text("""
                        SELECT 
                            card_code, 
                            last_purchase_date,
                            median_interval_days,
                            days_overdue
                        FROM account_predictions
                        WHERE card_code IN (SELECT value FROM json_each(:cards))
                        ORDER BY datetime(last_purchase_date) DESC
                        LIMIT 1
                    """)
                    
                    most_recent_result = conn.execute(get_most_recent, {"cards": cards_json})
                    most_recent = next((row for row in most_recent_result), None)
                    
                    if most_recent:
                        source_card = most_recent[0]
                        recent_date = most_recent[1]
                        interval_days = most_recent[2]
                        
                        # If the base code itself exists as a card_code
                        check_base = text("""
                            SELECT 1 FROM account_predictions WHERE card_code = :base
                        """)
                        base_exists = conn.execute(check_base, {"base": base}).fetchone() is not None
                        
                        if base_exists and base != source_card:
                            # Update the base record with the most recent dates
                            update_base = text("""
                                UPDATE account_predictions
                                SET 
                                    last_purchase_date = :last_date,
                                    median_interval_days = :interval,
                                    next_expected_purchase_date = date(:last_date, '+' || :interval || ' days'),
                                    days_overdue = :overdue
                                WHERE card_code = :base
                            """)
                            
                            conn.execute(update_base, {
                                "base": base,
                                "last_date": recent_date,
                                "interval": interval_days,
                                "overdue": most_recent[3]
                            })
                            
                            logger.info(f"Updated {base} with most recent dates from {source_card} (purchase date: {recent_date})")
            
            logger.info("Finished updating canonical records with most recent purchase dates.")
            
    except Exception as e:
        logger.error(f"Error updating canonical records with latest dates: {e}")
        logger.error(f"Error details: {str(e)}")


def validate_mapping(mapping, df, exceptions_file='card_code_exceptions.csv'):
    """Validate the mapping by checking if it would create conflicts."""
    logger.info("Validating mapping...")
    
    # Load exceptions
    exceptions = load_card_code_exceptions(exceptions_file)
    
    # Check if all exceptions were applied correctly
    missed_exceptions = [code for code in exceptions if code not in mapping or mapping[code] != exceptions[code]]
    
    if missed_exceptions:
        logger.warning(f"Found {len(missed_exceptions)} exceptions that were not properly applied")
        for code in missed_exceptions[:5]:  # Show first 5 for brevity
            logger.warning(f"Exception not applied: {code} -> {exceptions[code]} (mapped to {mapping.get(code, 'not mapped')})")
    
    # Create a copy of the DataFrame
    df_copy = df.copy()
    
    # Apply the mapping
    df_copy['mapped_card_code'] = df_copy['card_code'].map(lambda x: mapping.get(x, x))
    
    # Function to determine if addresses are truly different
    def are_addresses_truly_different(addr1, addr2):
        # Basic normalization for comparison
        norm1 = normalize_address(addr1) if addr1 else ""
        norm2 = normalize_address(addr2) if addr2 else ""
        
        # Calculate similarity
        similarity = difflib.SequenceMatcher(None, norm1, norm2).ratio()
        return similarity < 0.7  # Truly different if similarity is low
    
    # Check for conflicts within canonical groups
    conflicts = []
    for canonical, group in df_copy[df_copy['mapped_card_code'] != df_copy['card_code']].groupby('mapped_card_code'):
        # Skip validation for exception-defined canonicals
        exception_values = set(exceptions.values())
        if canonical in exception_values:
            logger.debug(f"Skipping conflict validation for exception-defined canonical: {canonical}")
            continue
            
        # Get unique addresses
        unique_addresses = group['full_address'].unique()
        
        if len(unique_addresses) > 1:
            # Determine if there are truly conflicting addresses
            truly_different = False
            
            for i in range(len(unique_addresses)):
                for j in range(i+1, len(unique_addresses)):
                    if are_addresses_truly_different(unique_addresses[i], unique_addresses[j]):
                        truly_different = True
                        break
                if truly_different:
                    break
            
            if truly_different:
                conflicts.append({
                    'canonical_card_code': canonical,
                    'original_codes': group['card_code'].tolist(),
                    'addresses': unique_addresses.tolist(),
                    'normalized_addresses': [normalize_address(addr) for addr in unique_addresses]
                })
    
    if conflicts:
        logger.warning(f"Found {len(conflicts)} potential conflicts in mapping")
        for i, conflict in enumerate(conflicts[:5]):  # Show first 5 conflicts
            logger.warning(f"Conflict {i+1}: {conflict['canonical_card_code']} would map records with addresses: {conflict['addresses']}")
        
        # Write conflicts to file
        pd.DataFrame(conflicts).to_csv("mapping_conflicts.csv", index=False)
        logger.warning("Wrote all conflicts to mapping_conflicts.csv")
    else:
        logger.info("No conflicts found in mapping")
    
    return conflicts

def apply_mapping_to_database(mapping):
    """Apply the mapping to update the database with proper data consolidation."""
    import json
    import pandas as pd  # Add pandas import for date handling
    
    if not mapping:
        logger.warning("No mappings to apply to database")
        return
    
    # Filter out invalid mappings
    valid_mapping = {}
    for original, canonical in mapping.items():
        if (original is None or canonical is None or 
            pd.isna(original) or pd.isna(canonical) or
            str(original).strip() == "" or str(canonical).strip() == "" or
            "nan" == str(canonical).lower()):
            logger.warning(f"Skipping invalid mapping: {original} → {canonical}")
            continue
        valid_mapping[original] = canonical
    
    if not valid_mapping:
        logger.warning("No valid mappings to apply after filtering")
        return
    
    logger.info(f"Applying {len(valid_mapping)} valid mappings (filtered out {len(mapping) - len(valid_mapping)} invalid ones)")
    
    try:
        # Create engine
        engine = create_engine(DB_CONNECTION_STRING)
        
        # Group mappings by canonical form to identify which need consolidation
        canonical_groups = {}
        for original, canonical in valid_mapping.items():
            if canonical not in canonical_groups:
                canonical_groups[canonical] = []
            canonical_groups[canonical].append(original)
        
        with engine.begin() as conn:
            # Drop any existing temporary tables from previous runs
            try:
                conn.execute(text("DROP TABLE IF EXISTS temp_historical_backup"))
                conn.execute(text("DROP TABLE IF EXISTS temp_snapshot_backup"))
                conn.execute(text("DROP TABLE IF EXISTS temp_predictions_backup"))
                logger.info("Cleaned up any existing temporary tables")
            except Exception as e:
                logger.warning(f"Error dropping temp tables: {e}")
                
            # First, handle account_predictions with proper consolidation
            logger.info("Consolidating account_predictions entries...")
            for canonical, originals in canonical_groups.items():
                if len(originals) > 1:
                    logger.info(f"Consolidating prediction data for {canonical} from {len(originals)} sources")
                    
                    # Check if canonical entry already exists
                    check_canonical = text("""
                        SELECT id, name, full_address FROM account_predictions
                        WHERE card_code = :canonical
                        LIMIT 1
                    """)
                    canonical_result = conn.execute(check_canonical, {"canonical": canonical})
                    canonical_record = next((row for row in canonical_result), None)
                    
                    if canonical_record:
                        # Just delete other records that will map to this canonical
                        logger.info(f"Canonical record {canonical} already exists, deleting originals")
                        # Delete all the mappings that point to this canonical 
                        # (except ones that already have this code)
                        for original in originals:
                            if original != canonical:
                                delete_pred = text("""
                                    DELETE FROM account_predictions
                                    WHERE card_code = :original
                                """)
                                conn.execute(delete_pred, {"original": original})
                                logger.info(f"Deleted prediction record for {original}")
                    else:
                        # Pick the most appropriate original as the canonical - use the most recent purchase date
                        best_match = None
                        most_recent_date = None
                        
                        for original in originals:
                            get_pred = text("""
                                SELECT id, name, full_address, last_purchase_date, 
                                       median_interval_days, next_expected_purchase_date
                                FROM account_predictions
                                WHERE card_code = :original
                                LIMIT 1
                            """)
                            pred_result = conn.execute(get_pred, {"original": original})
                            pred_record = next((row for row in pred_result), None)
                            
                            if pred_record:
                                logger.info(f"Found record for {original}: purchase_date={pred_record[3]}, interval={pred_record[4]}")
                                # Parse the date and compare
                                try:
                                    purchase_date = pd.to_datetime(pred_record[3]) if pred_record[3] else None
                                    logger.info(f"Parsed date for {original}: {purchase_date}")

                                    # Log current best
                                    if best_match:
                                        logger.info(f"Current best is {best_match[0]} with date {most_recent_date}")
                                    
                                    # If this is our first record or it has a more recent date than what we've seen
                                    if best_match is None or (purchase_date is not None and 
                                        (most_recent_date is None or purchase_date > most_recent_date)):
                                        logger.info(f"New best match: {original} (newer date)")
                                        best_match = (original, pred_record)
                                        most_recent_date = purchase_date
                                        
                                    # Still prioritize exact canonical match if dates are equal
                                    if original == canonical and (most_recent_date is None or 
                                       (purchase_date is not None and purchase_date == most_recent_date)):
                                        logger.info(f"New best match: {original} (canonical with equal date)")
                                        best_match = (original, pred_record)
                                        most_recent_date = purchase_date
                                except Exception as date_err:
                                    logger.warning(f"Could not parse date for {original}: {date_err}")
                                    # If we can't parse the date, still consider this record
                                    if best_match is None or original == canonical:
                                        logger.info(f"New best match: {original} (date parsing failed)")
                                        best_match = (original, pred_record)
                        
                        if best_match:
                            best_original, best_record = best_match
                            # Update this to be our canonical record
                            update_to_canonical = text("""
                                UPDATE account_predictions
                                SET card_code = :canonical
                                WHERE card_code = :original
                            """)
                            conn.execute(update_to_canonical, {"original": best_original, "canonical": canonical})
                            logger.info(f"Updated {best_original} to be the canonical {canonical} record (last purchase: {most_recent_date})")
                            
                            # Delete other records that would map to this canonical
                            for original in originals:
                                if original != best_original:
                                    delete_pred = text("""
                                        DELETE FROM account_predictions
                                        WHERE card_code = :original
                                    """)
                                    conn.execute(delete_pred, {"original": original})
                                    logger.info(f"Deleted prediction record for {original}")
                else:
                    # Simple case - just one original maps to the canonical
                    original = originals[0]
                    logger.info(f"Simple update for {original} to {canonical}")
                    update_prediction = text("""
                        UPDATE account_predictions 
                        SET card_code = :canonical 
                        WHERE card_code = :original
                    """)
                    conn.execute(update_prediction, {"original": original, "canonical": canonical})
            
            # Now handle historical revenue with proper consolidation
            logger.info("Processing historical revenue with consolidation...")
            
            # For each canonical form that has multiple source mappings
            for canonical, originals in canonical_groups.items():
                if len(originals) > 1:
                    logger.info(f"Consolidating historical data for {canonical} from {len(originals)} sources")
                    
                    # Step 1: First back up the data we'll be working with
                    conn.execute(text("DROP TABLE IF EXISTS temp_historical_backup"))
                    create_backup = text("""
                        CREATE TEMPORARY TABLE temp_historical_backup AS
                        SELECT * FROM account_historical_revenues
                        WHERE card_code IN (SELECT value FROM json_each(:originals))
                    """)
                    conn.execute(create_backup, {"originals": json.dumps(originals)})
                    
                    # Step 2: Identify all years that need consolidation
                    get_years = text("""
                        SELECT DISTINCT year 
                        FROM account_historical_revenues
                        WHERE card_code IN (SELECT value FROM json_each(:originals))
                    """)
                    years_result = conn.execute(get_years, {"originals": json.dumps(originals)})
                    years = [row[0] for row in years_result]
                    
                    # Step 3: For each year, consolidate all data
                    for year in years:
                        # Check if data exists for this canonical code and year
                        check_existing = text("""
                            SELECT id FROM account_historical_revenues
                            WHERE card_code = :canonical AND year = :year
                        """)
                        existing_result = conn.execute(check_existing, {"canonical": canonical, "year": year})
                        existing_id = next((row[0] for row in existing_result), None)
                        
                        # Get aggregated data for this year across all original codes
                        get_aggregates = text("""
                            SELECT 
                                SUM(total_revenue) AS total_revenue,
                                SUM(transaction_count) AS transaction_count,
                                (SELECT name FROM temp_historical_backup 
                                 WHERE card_code IN (SELECT value FROM json_each(:originals))
                                 AND year = :year 
                                 ORDER BY CASE WHEN card_code = :canonical THEN 0 ELSE 1 END
                                 LIMIT 1) AS name,
                                (SELECT sales_rep FROM temp_historical_backup 
                                 WHERE card_code IN (SELECT value FROM json_each(:originals))
                                 AND year = :year
                                 ORDER BY CASE WHEN card_code = :canonical THEN 0 ELSE 1 END
                                 LIMIT 1) AS sales_rep,
                                (SELECT distributor FROM temp_historical_backup 
                                 WHERE card_code IN (SELECT value FROM json_each(:originals))
                                 AND year = :year
                                 ORDER BY CASE WHEN card_code = :canonical THEN 0 ELSE 1 END
                                 LIMIT 1) AS distributor,
                                (SELECT yearly_products_json FROM temp_historical_backup 
                                 WHERE card_code IN (SELECT value FROM json_each(:originals))
                                 AND year = :year
                                 ORDER BY CASE WHEN card_code = :canonical THEN 0 ELSE 1 END
                                 LIMIT 1) AS yearly_products_json
                            FROM temp_historical_backup
                            WHERE card_code IN (SELECT value FROM json_each(:originals))
                            AND year = :year
                        """)
                        agg_result = conn.execute(get_aggregates, {"originals": json.dumps(originals), "year": year, "canonical": canonical})
                        agg_data = next(agg_result, None)
                        
                        if not agg_data:
                            logger.warning(f"No data found for {canonical}, year {year} - skipping")
                            continue
                        
                        # Before we update or insert, delete any duplicate records for this canonical/year
                        # This ensures we don't end up with multiple records
                        clean_duplicates = text("""
                            DELETE FROM account_historical_revenues
                            WHERE card_code = :canonical AND year = :year
                        """)
                        conn.execute(clean_duplicates, {"canonical": canonical, "year": year})
                        logger.info(f"Cleaned any duplicate records for {canonical}, year {year}")
                            
                        # Insert new consolidated record
                        insert_historical = text("""
                            INSERT INTO account_historical_revenues
                            (card_code, year, total_revenue, transaction_count, name, sales_rep, distributor, yearly_products_json)
                            VALUES
                            (:card_code, :year, :total_revenue, :transaction_count, :name, :sales_rep, :distributor, :yearly_products_json)
                        """)
                        
                        conn.execute(insert_historical, {
                            "card_code": canonical,
                            "year": year,
                            "total_revenue": agg_data[0],
                            "transaction_count": agg_data[1],
                            "name": agg_data[2],
                            "sales_rep": agg_data[3],
                            "distributor": agg_data[4],
                            "yearly_products_json": agg_data[5]
                        })
                        logger.info(f"Inserted consolidated record for {canonical}, year {year}")
                    
                    # Step 4: Delete original records that aren't the canonical
                    for original in originals:
                        if original != canonical:
                            delete_originals = text("""
                                DELETE FROM account_historical_revenues
                                WHERE card_code = :original
                            """)
                            conn.execute(delete_originals, {"original": original})
                            logger.info(f"Deleted original records for {original}")
                
                else:
                    # Simple case - just one original maps to the canonical, just update the code
                    original = originals[0]
                    logger.info(f"Simple update for {original} to {canonical}")
                    update_historical = text("""
                        UPDATE account_historical_revenues 
                        SET card_code = :canonical 
                        WHERE card_code = :original
                    """)
                    conn.execute(update_historical, {"original": original, "canonical": canonical})
            
            # Handle account_snapshots table if it exists
            try:
                logger.info("Updating account_snapshots table...")
                # Similar approach as historical revenue, with consolidation
                for canonical, originals in canonical_groups.items():
                    if len(originals) > 1:
                        logger.info(f"Consolidating snapshot data for {canonical} from {len(originals)} sources")
                        
                        # Back up the data
                        conn.execute(text("DROP TABLE IF EXISTS temp_snapshot_backup"))
                        create_snapshot_backup = text("""
                            CREATE TEMPORARY TABLE temp_snapshot_backup AS
                            SELECT * FROM account_snapshots
                            WHERE card_code IN (SELECT value FROM json_each(:originals))
                        """)
                        conn.execute(create_snapshot_backup, {"originals": json.dumps(originals)})
                        
                        # Get all years
                        get_snapshot_years = text("""
                            SELECT DISTINCT year 
                            FROM account_snapshots
                            WHERE card_code IN (SELECT value FROM json_each(:originals))
                        """)
                        snapshot_years_result = conn.execute(get_snapshot_years, {"originals": json.dumps(originals)})
                        snapshot_years = [row[0] for row in snapshot_years_result]
                        
                        # Process each year
                        for year in snapshot_years:
                            # Check if canonical exists for this year
                            check_existing_snapshot = text("""
                                SELECT id FROM account_snapshots
                                WHERE card_code = :canonical AND year = :year
                            """)
                            existing_snapshot = conn.execute(check_existing_snapshot, {"canonical": canonical, "year": year})
                            existing_snapshot_id = next((row[0] for row in existing_snapshot), None)
                            
                            # Get aggregated data using SQLite-compatible syntax
                            get_snapshot_agg = text("""
                                SELECT 
                                    (SELECT snapshot_date FROM temp_snapshot_backup 
                                     WHERE card_code IN (SELECT value FROM json_each(:originals)) 
                                     AND year = :year LIMIT 1) AS snapshot_date,
                                    SUM(yearly_revenue) AS yearly_revenue,
                                    SUM(yearly_purchases) AS yearly_purchases,
                                    AVG(account_total) AS account_total,
                                    AVG(health_score) AS health_score,
                                    AVG(churn_risk_score) AS churn_risk_score
                                FROM temp_snapshot_backup
                                WHERE card_code IN (SELECT value FROM json_each(:originals))
                                AND year = :year
                            """)
                            snapshot_agg = conn.execute(get_snapshot_agg, {"originals": json.dumps(originals), "year": year})
                            snapshot_data = next(snapshot_agg, None)
                            
                            if not snapshot_data:
                                continue
                            
                            # Clean up duplicates first
                            clean_snapshot_duplicates = text("""
                                DELETE FROM account_snapshots
                                WHERE card_code = :canonical AND year = :year
                            """)
                            conn.execute(clean_snapshot_duplicates, {"canonical": canonical, "year": year})
                                
                            # Insert new record
                            insert_snapshot = text("""
                                INSERT INTO account_snapshots
                                (card_code, snapshot_date, year, yearly_revenue, yearly_purchases, account_total, health_score, churn_risk_score)
                                VALUES
                                (:card_code, :snapshot_date, :year, :yearly_revenue, :yearly_purchases, :account_total, :health_score, :churn_risk_score)
                            """)
                            
                            conn.execute(insert_snapshot, {
                                "card_code": canonical,
                                "snapshot_date": snapshot_data[0],
                                "year": year,
                                "yearly_revenue": snapshot_data[1],
                                "yearly_purchases": snapshot_data[2],
                                "account_total": snapshot_data[3],
                                "health_score": snapshot_data[4],
                                "churn_risk_score": snapshot_data[5]
                            })
                        
                        # Delete originals that aren't the canonical
                        for original in originals:
                            if original != canonical:
                                delete_snapshot_originals = text("""
                                    DELETE FROM account_snapshots
                                    WHERE card_code = :original
                                """)
                                conn.execute(delete_snapshot_originals, {"original": original})
                    else:
                        # Simple update for single mapping
                        original = originals[0]
                        update_snapshot = text("""
                            UPDATE account_snapshots 
                            SET card_code = :canonical 
                            WHERE card_code = :original
                        """)
                        conn.execute(update_snapshot, {"original": original, "canonical": canonical})
                
            except Exception as e:
                logger.warning(f"Error updating account_snapshots: {e}")
            
        logger.info(f"Successfully applied mappings with proper data consolidation")
        
    except Exception as e:
        logger.error(f"Error applying mapping to database: {e}")
        logger.error(f"Error details: {str(e)}")


def update_yoy_metrics():
    """Update YoY growth metrics for consolidated records based on historical data."""
    logger.info("Recalculating YoY metrics for consolidated records...")
    
    try:
        # Create engine
        engine = create_engine(DB_CONNECTION_STRING)
        
        with engine.begin() as conn:
            # Get the current year to compare with previous
            get_max_year = text("SELECT MAX(year) FROM account_historical_revenues")
            max_year_result = conn.execute(get_max_year)
            current_year = next(max_year_result)[0]
            prev_year = current_year - 1
            
            # For each canonical code, calculate YoY metrics from historical data
            get_canonical_codes = text("SELECT DISTINCT card_code FROM account_predictions")
            codes_result = conn.execute(get_canonical_codes)
            canonical_codes = [row[0] for row in codes_result]
            
            updates = 0
            for code in canonical_codes:
                # Get current and previous year data
                get_revenue_data = text("""
                    SELECT 
                        year, 
                        SUM(total_revenue) as total_revenue, 
                        SUM(transaction_count) as transaction_count
                    FROM account_historical_revenues
                    WHERE card_code = :code AND year IN (:current_year, :prev_year)
                    GROUP BY year
                """)
                
                rev_result = conn.execute(get_revenue_data, {
                    "code": code, 
                    "current_year": current_year,
                    "prev_year": prev_year
                })
                
                # Extract data for both years
                year_data = {}
                for row in rev_result:
                    year = row[0]
                    revenue = row[1] or 0
                    count = row[2] or 0
                    year_data[year] = {"revenue": revenue, "count": count}
                
                # Calculate growth rates if both years have data
                yoy_revenue_growth = 0.0
                yoy_count_growth = 0.0
                
                if current_year in year_data and prev_year in year_data:
                    curr_rev = year_data[current_year]["revenue"]
                    prev_rev = year_data[prev_year]["revenue"]
                    
                    curr_count = year_data[current_year]["count"]
                    prev_count = year_data[prev_year]["count"]
                    
                    # Calculate revenue growth
                    if prev_rev > 0:
                        yoy_revenue_growth = ((curr_rev - prev_rev) / prev_rev) * 100
                    elif curr_rev > 0:
                        yoy_revenue_growth = 100.0  # If previous was 0, treat as 100% growth
                        
                    # Calculate transaction count growth
                    if prev_count > 0:
                        yoy_count_growth = ((curr_count - prev_count) / prev_count) * 100
                    elif curr_count > 0:
                        yoy_count_growth = 100.0  # If previous was 0, treat as 100% growth
                
                # Update the prediction record with recalculated YoY metrics
                update_yoy = text("""
                    UPDATE account_predictions
                    SET 
                        yoy_revenue_growth = :rev_growth,
                        yoy_purchase_count_growth = :count_growth
                    WHERE card_code = :code
                """)
                
                conn.execute(update_yoy, {
                    "code": code,
                    "rev_growth": yoy_revenue_growth,
                    "count_growth": yoy_count_growth
                })
                
                updates += 1
                if updates % 100 == 0:
                    logger.info(f"Updated YoY metrics for {updates} records")
            
            logger.info(f"Recalculated YoY metrics for {updates} records")
            
    except Exception as e:
        logger.error(f"Error updating YoY metrics: {e}")
        logger.error(f"Error details: {str(e)}")


def main():
    """Main function to run the script."""
    parser = argparse.ArgumentParser(description="Store normalization script")
    parser.add_argument("--analyze", action="store_true", help="Analyze stores and generate mapping")
    parser.add_argument("--apply", action="store_true", help="Apply mapping to database")
    parser.add_argument("--threshold", type=float, default=0.85, 
                        help="Similarity threshold (default: 0.85)")
    parser.add_argument("--csv", type=str, help="Path to CSV file for analysis instead of database")
    parser.add_argument("--exceptions", type=str, default="card_code_exceptions.csv",
                        help="Path to exceptions file (default: card_code_exceptions.csv)")
    args = parser.parse_args()
    
    # Use the threshold from args
    threshold = args.threshold
    exceptions_file = args.exceptions
    
    if args.analyze:
        logger.info("Starting store analysis...")
        
        # Fetch store data
        if args.csv:
            df = load_data_from_csv(args.csv)
        else:
            df = fetch_store_data()
            
        if df.empty:
            logger.error("No store data found. Exiting.")
            return
        
        # Find duplicates
        duplicates = find_duplicate_stores(df, threshold)
        
        # Generate mapping with exceptions
        mapping = generate_mapping(duplicates, df, exceptions_file)
        
        # Validate mapping
        if mapping:
            conflicts = validate_mapping(mapping, df, exceptions_file)
            if conflicts:
                logger.warning("Please review mapping_conflicts.csv before applying")
        
        logger.info("Analysis completed. Results saved to CSV files.")
    
    if args.apply:
        logger.info("Applying mapping to database...")
        
        # Load mapping from file
        if not os.path.exists(MAPPING_FILE):
            logger.error(f"Mapping file {MAPPING_FILE} not found. Run --analyze first.")
            return
        
        mapping_df = pd.read_csv(MAPPING_FILE)
        mapping = dict(zip(mapping_df['original_card_code'], mapping_df['canonical_card_code']))
        
        # Apply mapping
        apply_mapping_to_database(mapping)

        # After mapping, update canonical records with the most recent purchase dates
        update_canonical_with_latest_dates()


        # Also update product coverage data
        update_product_coverage()

        # Recalculate YoY metrics
        update_yoy_metrics()
        
        logger.info("Mapping applied to database.")

    
    # If no action specified, show help
    if not args.analyze and not args.apply:
        parser.print_help()


if __name__ == "__main__":
    main()