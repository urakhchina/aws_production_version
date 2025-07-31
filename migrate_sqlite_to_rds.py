# migrate_sqlite_to_rds.py
import os
import sys
import pandas as pd
import logging
from sqlalchemy import create_engine, inspect
from sqlalchemy.types import Integer, String, Float, DateTime, Text, Boolean
from dotenv import load_dotenv
import urllib.parse

# --- Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - Migration - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load credentials from .env file in the current directory
load_dotenv()

# Source SQLite configuration (assuming config.py is in the same root)
try:
    import config
    SQLITE_URI = config.DEFAULT_SQLALCHEMY_DATABASE_URI
    if not SQLITE_URI or not SQLITE_URI.startswith('sqlite:///'):
         raise ValueError("Default URI in config.py is not a valid SQLite URI.")
    logger.info(f"Source SQLite URI from config.py: {SQLITE_URI}")
except AttributeError:
     logger.error("Could not find DEFAULT_SQLALCHEMY_DATABASE_URI in config.py.")
     sys.exit(1)
except ValueError as ve:
     logger.error(f"Configuration Error: {ve}")
     sys.exit(1)
except Exception as e:
    logger.error(f"Could not load SQLite config from config.py: {e}")
    sys.exit(1)

# Target RDS PostgreSQL configuration from environment variables loaded by dotenv
RDS_USER = os.getenv('RDS_USER')
RDS_PASSWORD = os.getenv('RDS_PASSWORD')
RDS_HOST = os.getenv('RDS_HOST')
RDS_PORT = os.getenv('RDS_PORT', '5432')  # Default port if not set
RDS_DBNAME = os.getenv('RDS_DBNAME')

if not all([RDS_USER, RDS_PASSWORD, RDS_HOST, RDS_DBNAME]):
    logger.error("Missing RDS connection details in .env file (RDS_USER, RDS_PASSWORD, RDS_HOST, RDS_DBNAME)")
    sys.exit(1)

# URL-encode password if it contains special characters
encoded_password = urllib.parse.quote_plus(RDS_PASSWORD)
RDS_URI = f"postgresql://{RDS_USER}:{encoded_password}@{RDS_HOST}:{RDS_PORT}/{RDS_DBNAME}"
logger.info(f"Target RDS URI: postgresql://{RDS_USER}:***@{RDS_HOST}:{RDS_PORT}/{RDS_DBNAME}")

# Function to automatically discover all tables in SQLite
def get_all_sqlite_tables(sqlite_engine):
    """Get all table names from SQLite database."""
    inspector = inspect(sqlite_engine)
    all_tables = inspector.get_table_names()
    logger.info(f"Found {len(all_tables)} tables in SQLite database: {all_tables}")
    return all_tables

# Define specific data types for PostgreSQL (helps pandas.to_sql)
# Add mappings for columns in ALL your tables that need specific types
POSTGRES_DTYPES = {
    # Common Types
    'id': Integer,
    'year': Integer,
    'transaction_count': Integer,
    'median_interval_days': Integer,
    'days_overdue': Integer,
    'purchase_frequency': Integer,
    'days_since_last_purchase': Integer,
    'recency_score': Integer,
    'frequency_score': Integer,
    'monetary_score': Integer,
    'total_revenue': Float,
    'last_purchase_amount': Float,
    'account_total': Float,
    'priority_score': Float,
    'enhanced_priority_score': Float,
    'churn_risk_score': Float,
    'purchase_trend': Float,
    'purchase_consistency': Float,
    'avg_purchase_interval': Float,
    'purchase_interval_trend': Float,
    'expected_purchase_likelihood': Float,
    'rfm_score': Float,
    'health_score': Float,
    'yoy_revenue_growth': Float,
    'yoy_purchase_count_growth': Float,
    'product_coverage_percentage': Float,
    'yearly_revenue': Float,
    'yearly_purchases': Integer,
    'duration_minutes': Integer,
    'accounts_count': Integer,
    'active_accounts': Integer,
    'at_risk_accounts': Integer,
    'yoy_accounts_growth': Float,
    'month': Integer,
    'quarter': Integer,
    'card_code': String(255),
    'name': String(255),
    'full_address': String(500),
    'customer_id': String(255),
    'sales_rep': String(255),
    'sales_rep_name': String(255),
    'distributor': String(255),
    'sales_rep_id': String(50),
    'rfm_segment': String(50),
    'health_category': String(50),
    'activity_type': String(20),
    'outcome': String(50),
    'account_name': String(100),
    'last_purchase_date': DateTime,
    'next_expected_purchase_date': DateTime,
    'created_at': DateTime,
    'updated_at': DateTime,
    'snapshot_date': DateTime,
    'activity_datetime': DateTime,
    # Text/JSON columns
    'products_purchased': Text,
    'yearly_products_json': Text,
    'carried_top_products_json': Text,
    'missing_top_products_json': Text,
    'notes': Text,
    'version_num': String(32),  # For alembic_version table
    # Boolean columns
    'active': Boolean
}

# --- Main Migration Logic ---
try:
    logger.info("Creating database engines...")
    sqlite_engine = create_engine(SQLITE_URI)
    rds_engine = create_engine(RDS_URI)

    # Verify connection to RDS
    logger.info("Testing connection to RDS...")
    with rds_engine.connect() as rds_conn_test:
        logger.info("RDS connection successful.")
    logger.info("Testing connection to SQLite...")
    with sqlite_engine.connect() as sqlite_conn_test:
        logger.info("SQLite connection successful.")

    # Get inspector to check tables
    sqlite_inspector = inspect(sqlite_engine)
    rds_inspector = inspect(rds_engine)
    rds_tables_existing = rds_inspector.get_table_names()
    logger.info(f"Tables found in RDS target: {rds_tables_existing}")

    # Automatically discover all tables in SQLite
    TABLES_TO_MIGRATE = get_all_sqlite_tables(sqlite_engine)
    # Optionally exclude system tables if needed
    TABLES_TO_MIGRATE = [t for t in TABLES_TO_MIGRATE if not t.startswith('sqlite_')]
    
    logger.info(f"Will attempt to migrate the following tables: {TABLES_TO_MIGRATE}")

    # Track migration statistics
    migration_stats = {
        "successful_tables": [],
        "failed_tables": [],
        "empty_tables": [],
        "missing_target_tables": [],
        "total_rows_migrated": 0
    }

    for table_name in TABLES_TO_MIGRATE:
        logger.info(f"--- Processing table: {table_name} ---")

        # Check if table exists in source SQLite
        if not sqlite_inspector.has_table(table_name):
            logger.warning(f"Source table '{table_name}' not found in SQLite DB. Skipping.")
            continue

        # Check if table exists in target RDS (should have been created by flask db upgrade)
        if table_name not in rds_tables_existing:
            logger.error(f"Target table '{table_name}' does not exist in RDS! Run 'flask db upgrade' on EB first. Skipping.")
            migration_stats["missing_target_tables"].append(table_name)
            continue

        try:
            # Read data from SQLite table
            logger.info(f"Reading data from SQLite table '{table_name}'...")
            query = f"SELECT * FROM {table_name}"
            df = pd.read_sql(query, sqlite_engine)
            logger.info(f"Read {len(df)} rows from SQLite '{table_name}'.")

            if df.empty:
                logger.info(f"Source table '{table_name}' is empty. Nothing to write.")
                migration_stats["empty_tables"].append(table_name)
                continue

            # Get specific dtype mapping for this table's columns
            table_dtypes = {col: POSTGRES_DTYPES[col] for col in df.columns if col in POSTGRES_DTYPES}
            
            # Log columns that don't have explicit type mappings
            unmapped_columns = [col for col in df.columns if col not in POSTGRES_DTYPES]
            if unmapped_columns:
                logger.warning(f"Table '{table_name}' has columns without explicit type mappings: {unmapped_columns}")
                logger.warning("These will use pandas' default type inference which may not be optimal.")

            # Start a transaction
            with rds_engine.begin() as conn:
                # Write data to PostgreSQL table within the transaction
                logger.info(f"Writing {len(df)} rows to RDS table '{table_name}'...")
                df.to_sql(
                    name=table_name,
                    con=conn,  # Use the transaction connection
                    if_exists='append',
                    index=False,
                    dtype=table_dtypes,
                    chunksize=1000
                )
                # Record success
                migration_stats["successful_tables"].append(table_name)
                migration_stats["total_rows_migrated"] += len(df)
                logger.info(f"Successfully wrote {len(df)} rows to RDS '{table_name}'.")
        except Exception as table_err:
            logger.error(f"Error migrating table '{table_name}': {table_err}", exc_info=True)
            logger.warning(f"Migration for table {table_name} failed. Continuing with next table.")
            migration_stats["failed_tables"].append(table_name)
            # The transaction will automatically roll back due to the exception

    # Print migration summary
    logger.info("\n=== MIGRATION SUMMARY ===")
    logger.info(f"Total rows migrated: {migration_stats['total_rows_migrated']}")
    logger.info(f"Successful tables ({len(migration_stats['successful_tables'])}): {migration_stats['successful_tables']}")
    
    if migration_stats["empty_tables"]:
        logger.info(f"Empty tables ({len(migration_stats['empty_tables'])}): {migration_stats['empty_tables']}")
    
    if migration_stats["missing_target_tables"]:
        logger.warning(f"Missing target tables ({len(migration_stats['missing_target_tables'])}): {migration_stats['missing_target_tables']}")
        logger.warning("These tables exist in SQLite but not in RDS. Run 'flask db upgrade' to create them.")
    
    if migration_stats["failed_tables"]:
        logger.error(f"Failed tables ({len(migration_stats['failed_tables'])}): {migration_stats['failed_tables']}")
        logger.error("Review the log for specific errors related to these tables.")
    else:
        logger.info("No migration failures! All tables were processed successfully.")

    logger.info("--- Data Migration Script Completed ---")

except Exception as e:
    logger.error(f"An error occurred during the migration process: {e}", exc_info=True)
    sys.exit(1)
finally:
    # Dispose engines (optional, but good practice)
    if 'sqlite_engine' in locals(): 
        sqlite_engine.dispose()
        logger.info("SQLite engine disposed.")
    if 'rds_engine' in locals(): 
        rds_engine.dispose()
        logger.info("RDS engine disposed.")

if migration_stats.get("failed_tables"):
    logger.warning("Migration completed with errors. Some tables failed to migrate.")
    sys.exit(1)
else:
    logger.info("Migration completed successfully!")
    sys.exit(0)