import pandas as pd
import os
import sys

pd.set_option('display.max_rows', 500)  # Show more rows
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)

output_dir = 'analysis_reports'

# Check for both old and new output files
file_paths = {
    # Old analysis outputs
    'shipto': os.path.join(output_dir, 'shipto_merge_candidates.parquet'),
    'name': os.path.join(output_dir, 'name_merge_candidates.parquet'),
    
    # New canonical analysis outputs
    'canonical': os.path.join(output_dir, 'canonical_code_conflicts.parquet'),
    'raw': os.path.join(output_dir, 'raw_code_conflicts.parquet'),
    
    # CSV files
    'base_csv': os.path.join(output_dir, 'base_code_conflicts.csv'),
    'raw_csv': os.path.join(output_dir, 'raw_code_conflicts.csv')
}

# Check which files exist
existing_files = {name: path for name, path in file_paths.items() if os.path.exists(path)}

if not existing_files:
    print("No analysis files found. Have you run any analysis scripts yet?")
    print("Try running one of the following:")
    print("- python analyze_codes_streaming.py your_data.csv")
    print("- python analyze_codes_canonical.py your_data.csv")
    sys.exit(1)

# Print which files were found
print(f"Found {len(existing_files)} analysis files:")
for name, path in existing_files.items():
    print(f"- {name}: {path}")
print()

# Check and load old analysis files
if 'shipto' in existing_files:
    try:
        shipto_df = pd.read_parquet(existing_files['shipto'])
        print("--- ShipTo Based Candidates (Original Analysis) ---")
        print(f"Found {len(shipto_df)} ShipTo merge candidates")
        if len(shipto_df) > 0:
            print(shipto_df.head(10))
        else:
            print("(No ShipTo merge candidates found)")
        print()
    except Exception as e:
        print(f"Error loading ShipTo candidates: {e}")

if 'name' in existing_files:
    try:
        name_df = pd.read_parquet(existing_files['name'])
        print("--- Name/City/State Based Candidates (Original Analysis) ---")
        print(f"Found {len(name_df)} Name/City/State merge candidates")
        if len(name_df) > 0:
            print(name_df.head(10))
        else:
            print("(No Name/City/State merge candidates found)")
        print()
    except Exception as e:
        print(f"Error loading Name candidates: {e}")

# Check and load new canonical analysis files
if 'canonical' in existing_files:
    try:
        canonical_df = pd.read_parquet(existing_files['canonical'])
        print("--- Canonical Code Conflicts (New Analysis) ---")
        print(f"Found {len(canonical_df)} canonical code conflicts")
        if len(canonical_df) > 0:
            # Convert list column to string for display
            display_df = canonical_df.copy()
            if 'base_codes_list' in display_df.columns:
                display_df['base_codes'] = display_df['base_codes_list'].apply(lambda x: ', '.join(x) if isinstance(x, list) else str(x))
                display_df = display_df.drop('base_codes_list', axis=1)
            
            # Show the top conflicts
            print("\nTop conflicts by number of distinct base codes:")
            columns_to_show = ['canonical_code', 'distinct_base_codes', 'base_codes', 
                               'sample_name', 'sample_shipto']
            columns_to_show = [col for col in columns_to_show if col in display_df.columns]
            print(display_df.sort_values('distinct_base_codes', ascending=False)[columns_to_show].head(10))
        else:
            print("(No canonical code conflicts found)")
        print()
    except Exception as e:
        print(f"Error loading Canonical conflicts: {e}")

if 'raw' in existing_files:
    try:
        raw_df = pd.read_parquet(existing_files['raw'])
        print("--- Raw CARD_CODE Conflicts (New Analysis) ---")
        print(f"Found {len(raw_df)} raw CARD_CODE conflicts")
        if len(raw_df) > 0:
            # Convert list column to string for display
            display_df = raw_df.copy()
            if 'raw_codes_list' in display_df.columns:
                display_df['raw_codes'] = display_df['raw_codes_list'].apply(lambda x: ', '.join(x) if isinstance(x, list) else str(x))
                display_df = display_df.drop('raw_codes_list', axis=1)
            
            # Show the top conflicts
            print("\nTop conflicts by number of distinct raw codes:")
            columns_to_show = ['canonical_code', 'distinct_raw_codes', 'raw_codes', 
                              'sample_name', 'sample_shipto']
            columns_to_show = [col for col in columns_to_show if col in display_df.columns]
            print(display_df.sort_values('distinct_raw_codes', ascending=False)[columns_to_show].head(10))
        else:
            print("(No raw CARD_CODE conflicts found)")
        print()
    except Exception as e:
        print(f"Error loading Raw conflicts: {e}")

# Check if CSV versions exist - for cases where Parquet might be corrupted
if 'base_csv' in existing_files and 'canonical' not in existing_files:
    try:
        print("Loading CSV version of canonical code conflicts (backup)...")
        csv_df = pd.read_csv(existing_files['base_csv'])
        print(f"Found {len(csv_df)} canonical code conflicts in CSV")
        if len(csv_df) > 0:
            print(csv_df.head(10))
        else:
            print("(No canonical code conflicts found in CSV)")
        print()
    except Exception as e:
        print(f"Error loading CSV: {e}")

print("\n=== DEBUGGING INFO ===")
print("If you're not seeing the expected results, there might be issues with:")
print("1. The analysis scripts not being run correctly")
print("2. The input data not containing conflicts")
print("3. Problems with the canonical code generation logic")
print("\nTry running the canonical analysis with debugging:")
print("python -u analyze_codes_canonical.py your_data.csv")