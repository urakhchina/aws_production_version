"""
Store mapping module for pipeline integration.

This module:
1. Loads the mapping file created by store_normalization.py
2. Provides functions to normalize card codes during data processing
3. Can be imported into webhook_routes.py or pipeline.py
"""

import os
import pandas as pd
import logging
from typing import Dict, Optional

logger = logging.getLogger(__name__)

# Path to the mapping file (change as needed)
MAPPING_FILE = os.path.join(os.path.dirname(__file__), "data", "card_code_mapping.csv")

# Cache the mapping to avoid repeated file loading
_card_code_mapping: Optional[Dict[str, str]] = None


def load_card_code_mapping() -> Dict[str, str]:
    """
    Load the card code mapping from the CSV file.
    Returns a dictionary mapping from original to canonical card_codes.
    """
    global _card_code_mapping
    
    # Return cached mapping if available
    if _card_code_mapping is not None:
        return _card_code_mapping
    
    # Check if mapping file exists
    if not os.path.exists(MAPPING_FILE):
        logger.warning(f"Mapping file not found at {MAPPING_FILE}")
        return {}
    
    try:
        # Load mapping from CSV
        mapping_df = pd.read_csv(MAPPING_FILE)
        
        # Convert to dictionary for faster lookups
        _card_code_mapping = dict(zip(
            mapping_df['original_card_code'],
            mapping_df['canonical_card_code']
        ))
        
        logger.info(f"Loaded {len(_card_code_mapping)} card code mappings")
        return _card_code_mapping
    
    except Exception as e:
        logger.error(f"Error loading card code mapping: {e}")
        return {}


def normalize_card_code(card_code: str) -> str:
    """
    Normalize a card code using the loaded mapping.
    
    Args:
        card_code: The original card code to normalize.
        
    Returns:
        The canonical card code if a mapping exists, otherwise the original card code.
    """
    if not card_code:
        return card_code
    
    # Load mapping if not already loaded
    mapping = load_card_code_mapping()
    
    # Return mapped value if exists, otherwise return original
    return mapping.get(card_code, card_code)


def apply_card_code_mapping(df: pd.DataFrame, card_code_column: str = 'CARD_CODE') -> pd.DataFrame:
    """
    Apply card code normalization to a DataFrame.
    
    Args:
        df: The DataFrame containing card codes.
        card_code_column: The name of the column containing card codes.
        
    Returns:
        The DataFrame with normalized card codes.
    """
    if df.empty or card_code_column not in df.columns:
        return df
    
    # Load mapping if not already loaded
    mapping = load_card_code_mapping()
    if not mapping:
        logger.warning("No card code mapping loaded. Returning original DataFrame.")
        return df
    
    # Count original card codes for logging
    original_counts = df[card_code_column].value_counts()
    
    # Apply mapping
    df[card_code_column] = df[card_code_column].apply(lambda x: mapping.get(x, x))
    
    # Count mapped card codes for logging
    mapped_counts = df[card_code_column].value_counts()
    
    # Calculate and log changes
    reduced_codes = len(original_counts) - len(mapped_counts)
    if reduced_codes > 0:
        logger.info(f"Card code mapping reduced unique codes from {len(original_counts)} to {len(mapped_counts)} ({reduced_codes} consolidated)")
    
    return df


# --- Example Usage in pipeline.py ---
"""
from store_mapper import apply_card_code_mapping

def process_file_async(app_instance_config, filepath):
    # ... existing code ...
    
    # Load weekly data
    weekly_df = pd.read_csv(filepath)
    
    # Apply store mapping for consistent card codes (BEFORE your existing mapping logic)
    weekly_df = apply_card_code_mapping(weekly_df)
    
    # ... continue with your existing code ...
"""