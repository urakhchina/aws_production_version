# migrations/env.py
import os
import sys
from logging.config import fileConfig

from sqlalchemy import create_engine
from sqlalchemy import pool

from alembic import context

# --- Add project root to Python path ---
# This ensures 'app' and 'models' can be imported
project_root = os.path.join(os.path.dirname(__file__), '..')
if project_root not in sys.path:
    sys.path.insert(0, project_root)
# --- End Path Addition ---

# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# Interpret the config file for Python logging.
# This line sets up loggers basically.
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# --- Import target_metadata directly from models ---
# This is often more reliable in env.py than importing the full app
try:
    from models import db # Import your Flask-SQLAlchemy db instance
    target_metadata = db.metadata
    print("DEBUG env.py: Successfully imported target_metadata from models.db")
except ImportError as e:
    print(f"ERROR env.py: Failed to import db from models: {e}")
    print("Ensure models.py exists and defines 'db = SQLAlchemy()'")
    target_metadata = None
except Exception as e_meta:
    print(f"ERROR env.py: Failed to get metadata from models.db: {e_meta}")
    target_metadata = None
# --- End Metadata Import ---


# other values from the config, defined by the needs of env.py,
# can be acquired:
# my_important_option = config.get_main_option("my_important_option")
# ... etc.


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode."""
    # We still need the correct URL even for offline mode if generating SQL
    # against a specific backend dialect potentially.
    url = os.environ.get('SQLALCHEMY_DATABASE_URI') # Prioritize env var
    if not url:
        try:
            from app import app # Fallback to app config
            url = app.config.get('SQLALCHEMY_DATABASE_URI')
            print("DEBUG env.py (offline): Using URL from app config")
        except Exception:
             url = config.get_main_option("sqlalchemy.url") # Final fallback to alembic.ini
             print("DEBUG env.py (offline): Using URL from alembic.ini")
    if not url:
        raise ValueError("Offline mode needs database URL via SQLALCHEMY_DATABASE_URI env var, Flask config, or alembic.ini")

    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode."""

    # --- Determine SQLAlchemy URL (same logic as before) ---
    sqlalchemy_url = os.environ.get('SQLALCHEMY_DATABASE_URI')
    if sqlalchemy_url:
        print(f"DEBUG env.py (online): Using SQLALCHEMY_DATABASE_URI from environment variable.")
    else:
        try:
             from app import app # Fallback to app config
             sqlalchemy_url = app.config.get('SQLALCHEMY_DATABASE_URI')
             if sqlalchemy_url:
                 print(f"DEBUG env.py (online): Using SQLALCHEMY_DATABASE_URI from Flask app config.")
             else:
                 raise ValueError("SQLALCHEMY_DATABASE_URI not found in environment or Flask config.")
        except Exception as e:
             print(f"ERROR env.py (online): Could not retrieve SQLALCHEMY_DATABASE_URI: {e}")
             raise

    # --- Create Engine ---
    try:
        connectable = create_engine(sqlalchemy_url)
        print(f"DEBUG env.py (online): Engine created for URL: {connectable.url.render_as_string(hide_password=True)}") # Log URL without password
    except Exception as e_engine:
        print(f"ERROR env.py (online): Failed to create engine: {e_engine}")
        raise

    # --- Connect and Run Migrations ---
    if target_metadata is None:
        raise ValueError("target_metadata could not be loaded. Cannot run migrations.")

    try:
        with connectable.connect() as connection:
            print("DEBUG env.py (online): Database connection successful.")
            context.configure(
                connection=connection, target_metadata=target_metadata
            )
            print("DEBUG env.py (online): Alembic context configured. Beginning transaction...")
            with context.begin_transaction():
                print("DEBUG env.py (online): Running migrations...")
                context.run_migrations()
                print("DEBUG env.py (online): Migrations finished.")
    except Exception as e_connect:
        print(f"ERROR env.py (online): Failed during connection or migration execution: {e_connect}")
        raise
    finally:
        if connectable:
            connectable.dispose()
            print("DEBUG env.py (online): Engine disposed.")


if context.is_offline_mode():
    print("DEBUG env.py: Running migrations offline...")
    run_migrations_offline()
else:
    print("DEBUG env.py: Running migrations online...")
    run_migrations_online()