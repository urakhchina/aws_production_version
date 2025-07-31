# check_hash_collisions.py
import pandas as pd
import hashlib
import sys
from datetime import datetime
from collections import defaultdict
import os

print("--- Script Started! ---") # Add this line

# --- Configuration ---
# Hashing columns WITHOUT ShipTo (matches current webhook/backfill before proposed change)
HASH_COLS_WITHOUT_SHIPTO = [
    "base_card_code", # Assumes this column exists in the processed data
    "posting_date",   # Use lowercase model attribute names expected after cleaning
    "description",
    "quantity",
    "amount"
]

# Hashing columns WITH ShipTo (proposed new logic)
HASH_COLS_WITH_SHIPTO = [
    "base_card_code", # Assumes this column exists
    "posting_date",
    "description",
    "quantity",
    "amount",
    "ship_to_code"     # Use lowercase model attribute name
]

DELIMITER = "|"
# --- End Configuration ---

def calculate_hash(row, columns_to_hash):
    """Calculates SHA1 hash based on specified columns in a pandas Series."""
    try:
        hash_input_parts = []
        for col in columns_to_hash:
            value = row.get(col) # Use .get() for safety
            if value is None:
                part = ''
            # Handle datetime specifically if it might be a datetime object
            elif isinstance(value, datetime):
                part = value.isoformat()
            # Handle potential pandas NA types
            elif pd.isna(value):
                 part = ''
            else:
                part = str(value) # Convert rest to string
            hash_input_parts.append(part)

        hash_input = DELIMITER.join(hash_input_parts)
        return hashlib.sha1(hash_input.encode('utf-8')).hexdigest()[:32]
    except Exception as e:
        print(f"Error hashing row {row.name}: {e}") # Print index on error
        return None

def check_collisions(df, columns_to_hash):
    """Checks for duplicate hashes generated using the specified columns."""
    print(f"\n--- Checking collisions using columns: {columns_to_hash} ---")
    df['temp_hash'] = df.apply(lambda row: calculate_hash(row, columns_to_hash), axis=1)

    # Find hashes that appear more than once
    duplicates = df[df.duplicated(subset=['temp_hash'], keep=False)].copy()

    if duplicates.empty:
        print("No hash collisions found with this method.")
        df.drop(columns=['temp_hash'], inplace=True)
        return

    print(f"Found {duplicates['temp_hash'].nunique()} unique hash values that caused collisions involving {len(duplicates)} rows.")

    # Group by the duplicate hash to show colliding rows
    grouped = duplicates.sort_values(by=['temp_hash', 'id'] if 'id' in duplicates.columns else ['temp_hash']).groupby('temp_hash')

    collision_count = 0
    for hash_value, group in grouped:
        if len(group) > 1:
            collision_count += 1
            print(f"\nCollision #{collision_count} - Hash: {hash_value}")
            # Define columns to display for context - adjust as needed
            display_cols = ['id'] if 'id' in group.columns else [] # Use id if available
            display_cols += [col for col in columns_to_hash if col in group.columns] # Show hash input columns
            # Add ShipTo if not in hash columns, for comparison
            if 'ShipTo' in group.columns and 'ShipTo' not in display_cols: display_cols.append('ShipTo')
            if 'ship_to_code' in group.columns and 'ship_to_code' not in display_cols: display_cols.append('ship_to_code')
            if 'CardCode' in group.columns and 'CardCode' not in display_cols: display_cols.append('CardCode') # Show original code

            print(group[display_cols].to_string(index=False))

    df.drop(columns=['temp_hash'], inplace=True)
    print(f"\n--- Finished checking for {columns_to_hash}. Found {collision_count} distinct hash collisions. ---")
    return collision_count


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python check_hash_collisions.py <path_to_csv_file>")
        sys.exit(1)

    csv_filepath = sys.argv[1]

    if not os.path.exists(csv_filepath):
        print(f"Error: File not found at {csv_filepath}")
        sys.exit(1)

    print(f"Loading data from: {csv_filepath}")
    try:
        # Load CSV - use similar dtypes as webhook
        dtypes = { # Use source CSV column names
            'CardCode': str, 'CARD_CODE': str, 'POSTINGDATE': str, 'AMOUNT': str, 'QUANTITY': str,
            'DESCRIPTION': str, 'NAME': str, 'SalesRep': str, 'Distributor': str, 'CUSTOMERID': str,
            'ADDRESS': str, 'CITY': str, 'STATE': str, 'ZIPCODE': str, 'ITEMDESC': str, 'SlpName': str, 'ShipTo': str,
            'base_card_code': str, # Add if generated separately
            'ship_to_code': str   # Add if generated separately
         }
         # Be flexible with reading, try different encodings if needed
        try:
            df = pd.read_csv(csv_filepath, dtype=str, keep_default_na=False, na_values=[''], low_memory=False)
        except UnicodeDecodeError:
            print("UTF-8 failed, trying latin-1...")
            df = pd.read_csv(csv_filepath, dtype=str, keep_default_na=False, na_values=[''], low_memory=False, encoding='latin-1')

        print(f"Loaded {len(df)} rows.")

        # --- Data Preprocessing ---
        # 1. Standardize CardCode column name (use 'CardCode' initially)
        if 'CardCode' not in df.columns and 'CARD_CODE' in df.columns:
             df.rename(columns={'CARD_CODE': 'CardCode'}, inplace=True)
        if 'CardCode' not in df.columns:
             print("Error: Missing 'CardCode' or 'CARD_CODE' column in CSV.")
             sys.exit(1)

        # 2. Basic Cleaning (handle potential NaN strings explicitly after loading as str)
        df.replace({'nan': None, 'None': None, '': None}, inplace=True) # Replace common null strings

        # 3. Convert relevant columns to appropriate types FOR HASHING CONSISTENCY
        #    This should match the types *before* they are stringified in the hash function
        try:
            # Convert date - use errors='coerce' to handle bad dates
            df['posting_date'] = pd.to_datetime(df['POSTINGDATE'], errors='coerce')
            # Convert numeric - use errors='coerce'
            df['quantity'] = pd.to_numeric(df['QUANTITY'], errors='coerce')
            df['amount'] = pd.to_numeric(df['AMOUNT'], errors='coerce')
        except KeyError as e:
            print(f"Error converting types: Missing column {e}")
            sys.exit(1)

        # 4. Add 'description' column (lowercase from DESCRIPTION) if needed by hash list
        if 'DESCRIPTION' in df.columns:
            df['description'] = df['DESCRIPTION'].str.lower()
        else:
            print("Warning: DESCRIPTION column missing, hash results might differ.")
            df['description'] = None

        # 5. Add 'base_card_code' column (using your pipeline logic)
        # Important: Need get_base_card_code function accessible
        try:
            from pipeline import get_base_card_code
            df['base_card_code'] = df['CardCode'].apply(get_base_card_code)
        except ImportError:
            print("Warning: Could not import get_base_card_code from pipeline. Using CardCode as base_card_code.")
            df['base_card_code'] = df['CardCode']
        except Exception as e:
             print(f"Warning: Error generating base_card_code: {e}. Using CardCode as base_card_code.")
             df['base_card_code'] = df['CardCode']

        # 6. Add 'ship_to_code' column (lowercase from ShipTo)
        if 'ShipTo' in df.columns:
            df['ship_to_code'] = df['ShipTo'].str.lower().str.strip()
        else:
             print("Warning: ShipTo column missing.")
             df['ship_to_code'] = None

        # --- Run Collision Checks ---
        # Note: The DataFrame passed to check_collisions now uses lowercase names
        # matching the model attributes used in HASH_COLS lists.
        collisions_without_shipto = check_collisions(df, HASH_COLS_WITHOUT_SHIPTO)
        collisions_with_shipto = check_collisions(df, HASH_COLS_WITH_SHIPTO)

        # --- Final Verdict ---
        print("\n--- FINAL VERDICT ---")
        if collisions_without_shipto > 0 and collisions_with_shipto == 0:
             print("✅ YES, including 'ship_to_code' RESOLVES all hash collisions found.")
             print("   Recommendation: Update hashing logic in webhook and backfill to include 'ship_to_code'.")
        elif collisions_without_shipto > 0 and collisions_with_shipto > 0:
             print("⚠️ Including 'ship_to_code' REDUCES collisions, but some still remain.")
             print(f"   ({collisions_without_shipto} collisions without -> {collisions_with_shipto} collisions with)")
             print("   Investigate the remaining collisions shown above. Are they true duplicates?")
             print("   You might need to add even more fields to the hash if these are distinct transactions.")
        elif collisions_without_shipto == 0:
             print("✅ No hash collisions were found even WITHOUT including 'ship_to_code'.")
             print("   Adding 'ship_to_code' to the hash is still safer but not strictly required based on this data sample.")
        else: # collisions_without_shipto > 0 and collisions_with_shipto > collisions_without_shipto (shouldn't happen)
             print("❓ Unexpected result: Including 'ship_to_code' increased collisions? Check logic/data.")

        print("--------------------")

    except FileNotFoundError:
        print(f"Error: Input file not found at {csv_filepath}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        import traceback
        traceback.print_exc()