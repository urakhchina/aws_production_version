# analyze_codes_canonical.py
import os, sys, re, json, argparse, textwrap
import polars as pl
from datetime import datetime

# --- helper imports from your pipeline ---------------------------------
# You MUST use the exact same versions of these functions
try:
    from pipeline import (
        normalize_store_name, normalize_address, get_base_card_code, generate_canonical_code
    )
    print("Successfully imported pipeline functions.")
except ImportError as e:
    print(f"ERROR: Could not import pipeline functions: {e}")
    sys.exit(1)
# -----------------------------------------------------------------------

def apply_generate_canonical_code_polars(struct_row: dict) -> str | None:
    """Wrapper to apply row-based generate_canonical_code to Polars struct."""
    # Polars passes each row as a dictionary when using map_elements on a struct
    try:
        # Call your existing function
        return generate_canonical_code(struct_row)
    except Exception as e:
        # Log or print errors during apply if needed, return None on failure
        print(f"Error applying generate_canonical_code to row {struct_row.get('CARD_CODE', 'N/A')}: {e}", file=sys.stderr)
        return None

def main(csv_path: str, out_dir: str):
    t0 = datetime.now()
    os.makedirs(out_dir, exist_ok=True)

    print(f"→ Scanning {csv_path} …")

    # Define columns needed for generate_canonical_code accurately
    # These MUST match the keys expected by your generate_canonical_code function
    cols_needed_for_canonical = ["CARD_CODE", "ShipTo", "NAME", "ADDRESS", "CITY", "STATE", "ZIPCODE"]

    scan = (
        pl.scan_csv(
            csv_path,
            null_values=["", "NaN", "nan"],
            encoding="utf8-lossy",
            infer_schema_length=0,
        )
        .rename({"CardCode": "CARD_CODE"}) # Ensure consistent naming early
        # Select only needed columns early to reduce memory/processing
        .select(cols_needed_for_canonical)
         # Ensure string type and fill nulls appropriately BEFORE normalization
        .with_columns(
            [pl.col(c).cast(str).fill_null("").str.strip_chars() for c in cols_needed_for_canonical]
        )
        .filter(pl.col("CARD_CODE") != "")
        # Add base_card_code first, as generate_canonical_code needs it
        .with_columns(
            base_card_code=pl.col("CARD_CODE").map_elements(
                get_base_card_code, return_dtype=pl.Utf8, skip_nulls=False
            )
        )
        # Generate the canonical code using YOUR logic
        .with_columns(
            # Pass all needed columns as a struct to the apply function
            canonical_code=pl.struct(
                # Include base_card_code AND original raw columns needed by generate_canonical_code
                 ["base_card_code"] + cols_needed_for_canonical
            ).map_elements(
                 apply_generate_canonical_code_polars, return_dtype=pl.Utf8, skip_nulls=False
            )
        )
        .filter(pl.col("canonical_code").is_not_null()) # Remove rows where canonical failed
    )

    # -------- Collect results after initial processing --------
    print("Collecting processed data with canonical codes...")
    try:
        processed_df = scan.collect() # Bring into memory for grouping
        print(f"Collected {len(processed_df)} rows with canonical codes.")
        
        # Show some sample data for verification
        print("\nSample canonical codes (first 5):")
        sample = processed_df.select(["CARD_CODE", "base_card_code", "canonical_code"]).head(5)
        print(sample)
    except Exception as e:
        print(f"Error collecting data: {e}")
        sys.exit(1)

    # -------- Analyze Conflicts --------
    print("Analyzing canonical code conflicts...")
    # Group by the generated canonical_code and see which ones map to multiple base_card_codes
    conflicts = (
        processed_df
        .group_by("canonical_code")
        .agg(
            pl.col("base_card_code").n_unique().alias("distinct_base_codes"),
            pl.col("base_card_code")
            .unique()
            .sort()
            .alias("base_codes_list"),
             # Include representative NAME/ShipTo for context
            pl.col("NAME").first().alias("sample_name"),
            pl.col("ShipTo").first().alias("sample_shipto"),
            # Include address fields for additional context
            pl.col("ADDRESS").first().alias("sample_address"),
            pl.col("CITY").first().alias("sample_city"),
            pl.col("STATE").first().alias("sample_state"),
        )
        .filter(pl.col("distinct_base_codes") > 1)
        .sort("distinct_base_codes", descending=True)
    )

    # -------- Also find raw CARD_CODE conflicts --------
    raw_code_conflicts = (
        processed_df
        .group_by("canonical_code")
        .agg(
            pl.col("CARD_CODE").n_unique().alias("distinct_raw_codes"),
            pl.col("CARD_CODE")
            .unique()
            .sort()
            .alias("raw_codes_list"),
            # Include representative NAME/ShipTo for context
            pl.col("NAME").first().alias("sample_name"),
            pl.col("ShipTo").first().alias("sample_shipto"),
        )
        .filter(pl.col("distinct_raw_codes") > 1)
        .sort("distinct_raw_codes", descending=True)
    )

    # -------- Write Results --------
    conflict_out = os.path.join(out_dir, "canonical_code_conflicts.parquet")
    conflicts.write_parquet(conflict_out)
    
    raw_conflict_out = os.path.join(out_dir, "raw_code_conflicts.parquet")
    raw_code_conflicts.write_parquet(raw_conflict_out)

    # -------- Create CSV exports for easier viewing --------
    try:
        # Base code conflicts CSV
        base_csv_out = os.path.join(out_dir, "base_code_conflicts.csv")
        base_csv_ready = conflicts.with_columns([
            pl.col("base_codes_list").map_elements(lambda x: "|".join(x) if x else "", return_dtype=pl.Utf8).alias("base_codes_str")
        ]).drop("base_codes_list")
        base_csv_ready.write_csv(base_csv_out)
        
        # Raw code conflicts CSV
        raw_csv_out = os.path.join(out_dir, "raw_code_conflicts.csv")
        raw_csv_ready = raw_code_conflicts.with_columns([
            pl.col("raw_codes_list").map_elements(lambda x: "|".join(x) if x else "", return_dtype=pl.Utf8).alias("raw_codes_str")
        ]).drop("raw_codes_list")
        raw_csv_ready.write_csv(raw_csv_out)
        
        print(f"✔  CSV exports created successfully")
    except Exception as e:
        print(f"Warning: Could not create CSV exports: {e}")
        print("The Parquet files were still created successfully.")

    print(
        textwrap.dedent(
            f"""
            ✔  Canonical Codes with multiple Base Codes : {conflicts.height:,}
               → {conflict_out}
            
            ✔  Canonical Codes with multiple Raw Codes  : {raw_code_conflicts.height:,}
               → {raw_conflict_out}
            
            ✔  CSV exports for easier viewing:
               → {os.path.join(out_dir, "base_code_conflicts.csv")}
               → {os.path.join(out_dir, "raw_code_conflicts.csv")}

            Done in {(datetime.now() - t0).total_seconds():.1f} s
            
            Next steps:
            1. Examine the Parquet files using a tool like DuckDB, Python, or R
            2. Review the CSV files to identify patterns in conflicts
            3. Add explicit mappings to your card_code_mapping.csv for conflicting codes
            """
        )
    )

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Analyse potential CardCode merges by applying canonical logic.")
    parser.add_argument("csv", help="Path to SAP CSV export")
    parser.add_argument(
        "-o",
        "--out-dir",
        default="analysis_reports",
        help="Directory to write candidate Parquet files",
    )
    args = parser.parse_args()
    main(args.csv, args.out_dir)