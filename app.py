# app.py (Corrected App Factory Pattern)

import os
import time
import click
from flask import Flask, current_app, jsonify
from flask.cli import with_appcontext
from sqlalchemy import func, or_, select, distinct, and_
import logging
import sys
from datetime import datetime, date, timedelta
import pandas as pd
import numpy as np

# --- Step 1: Initialize Extensions (but don't configure them yet) ---
from models import db, AccountPrediction
from flask_migrate import Migrate
from dotenv import load_dotenv
from pipeline import recalculate_predictions_and_metrics, safe_float
from services.email_service import send_email

# NOTE: Blueprint imports have been REMOVED from here and moved inside create_app()
# NOTE: "import config" has been REMOVED

load_dotenv()
migrate = Migrate()


def create_app():
    """
    Application Factory Function.
    This creates and configures the Flask application.
    """
    app = Flask(__name__, static_folder='static', template_folder='templates')
    load_dotenv()
    logger = logging.getLogger('flask.app')

    # --- Step 2: Configure the App from config.py ---
    # Configuration is loaded cleanly from config.py
    app.config.from_object('config')
    
    # Configure logging
    log_level_name = os.environ.get('LOG_LEVEL', 'INFO').upper()
    log_level = getattr(logging, log_level_name, logging.INFO)
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        stream=sys.stdout
    )
    app.logger.info(f"Root logger configured with level {log_level_name}")

    # --- Step 3: Initialize Extensions with the App Context ---
    # From this point on, 'db' is LIVE and correctly configured.
    db.init_app(app)
    migrate.init_app(app, db)

    # --- Step 4: Import and Register Blueprints ---
    # Because we do this *after* db.init_app(), they get the live, working db object.
    from routes.webhook_routes import webhook_bp
    from routes.api_routes import api_bp
    from routes.dashboard_routes import dashboard_bp
    from routes.api_routes_historical import api_historical_bp
    from routes.compatibility_routes import compatibility_bp
    from routes.api_routes_strategic import api_strategic_bp
    from routes.api_routes_strategic_v2 import api_strategic_v2_bp
    from routes.tasks_routes import tasks_bp

    app.register_blueprint(webhook_bp)
    app.register_blueprint(api_bp)
    app.register_blueprint(dashboard_bp)
    app.register_blueprint(api_historical_bp)
    app.register_blueprint(compatibility_bp)
    app.register_blueprint(api_strategic_bp)
    app.register_blueprint(api_strategic_v2_bp)
    app.register_blueprint(tasks_bp)

    # --- Step 5: Register Core Routes, CLI Commands, and other App-level logic ---

    @app.route('/')
    def index():
        return jsonify({"status": "healthy", "message": "Mariano on the Road service is running"})

    @app.route('/health', methods=['GET'])
    def health_check():
        return "OK", 200

    # CLI Commands
    @app.cli.command("send-reminders")
    @click.option('--test-email', default=None, help='Send only to this specific email address for testing.')
    @with_appcontext
    def send_reminders_command(test_email):
        logger = current_app.logger # Use current_app.logger
        logger.info("Starting daily customer reminder task...")
        print("Starting daily customer reminder task...")

        today = date.today()
        # Optional: Define a grace period threshold
        # reminder_grace_period = datetime.utcnow() - timedelta(hours=20) # Don't resend if sent within X hours

        try:
            logger.info(f"Querying for accounts due on {today} with NULL reminder_state or needing re-check...")

            # Construct the query using SQLAlchemy ORM
            accounts_to_remind_query = db.select(AccountPrediction).where(
                AccountPrediction.customer_email.isnot(None),
                AccountPrediction.customer_email != '',
                func.date(AccountPrediction.next_expected_purchase_date) == today,
                # Logic:
                # 1. Reminder state is NULL (never sent for this due date)
                # OR
                # 2. Reminder state is 'SENT', but the last_purchase_date recorded AT THE TIME OF SENDING
                #    is older than some threshold (e.g. 2 days before next_expected_purchase_date).
                #    This handles cases where a reminder was sent, but no purchase happened, and we want to send another.
                #    For simplicity now, let's stick to only reminder_state IS NULL, or SENT but last purchase date hasn't changed.
                #    If you want to resend, the logic becomes more complex (e.g., track number of reminders sent).
                #    Let's refine to: Send if state is NULL OR (state is SENT AND last_purchase_date is still the same as notified_last_purchase_date)
                or_(
                    AccountPrediction.reminder_state.is_(None),
                    and_( # Only consider resending if...
                        AccountPrediction.reminder_state == 'SENT',
                        AccountPrediction.last_purchase_date == AccountPrediction.notified_last_purchase_date, # No new purchase since last reminder
                        AccountPrediction.reminder_sent_at < (datetime.utcnow() - timedelta(days=3)) # And reminder was sent >3 days ago (avoid spam)
                    )
                )
                # Optional: Add grace period to avoid rapid resends
                # and_(
                #     AccountPrediction.reminder_sent_at.is_(None), # Not sent yet OR
                #     AccountPrediction.reminder_sent_at < reminder_grace_period # Sent long enough ago to resend
                # )
            )

            accounts_to_remind = db.session.execute(accounts_to_remind_query).scalars().all()

            if not accounts_to_remind:
                logger.info("No accounts found needing a reminder today based on criteria.")
                print("No accounts found needing a reminder today.")
                return

            logger.info(f"Found {len(accounts_to_remind)} accounts to remind.")
            print(f"Found {len(accounts_to_remind)} accounts to remind.")

            success_count = 0
            fail_count = 0
            processed_ids = [] 

            sales_rep_map = current_app.config.get('SALES_REP_MAPPING', {})
            if not sales_rep_map:
                logger.warning("SALES_REP_MAPPING not found or is empty. Sales reps will not be CC'd.")

            for account in accounts_to_remind:
                # Redundant check, query should handle this, but good for safety
                # if account.reminder_state == 'PURCHASED': # Already purchased after a reminder
                #     logger.info(f"Skipping {account.canonical_code} - reminder_state is already 'PURCHASED'.")
                #     continue
                
                # Additional check if we are "resending"
                if account.reminder_state == 'SENT' and account.last_purchase_date == account.notified_last_purchase_date:
                     logger.info(f"Account {account.canonical_code} was previously sent a reminder, no new purchase detected, due today. Preparing to send again.")
                elif account.reminder_state is not None and account.reminder_state != 'SENT': # e.g. PURCHASED or some other state
                     logger.warning(f"Skipping {account.canonical_code} - reminder_state is '{account.reminder_state}' (not NULL or eligible SENT).")
                     continue

                recipient = test_email if test_email else account.customer_email
                if not recipient:
                    logger.warning(f"Skipping {account.canonical_code} - No valid recipient email ({account.customer_email}).")
                    continue

                rep_email_cc = None
                if account.sales_rep_name:
                    if account.sales_rep_name in sales_rep_map:
                        rep_email_cc = sales_rep_map[account.sales_rep_name]
                    else:
                        rep_id_info = f"(ID: {getattr(account, 'sales_rep', 'N/A')})"
                        logger.warning(f"No email mapping for Rep '{account.sales_rep_name}' {rep_id_info} for {account.canonical_code}. Cannot CC.")
                
                store_name = account.name or "Valued Customer"
                subject = f"Friendly Reminder from Irwin Naturals"
                body = f"""Hi {store_name},

    Just a friendly reminder that your next order with Irwin Naturals might be due soon, based on your typical purchasing pattern.

    Ensuring you have the products your customers love is important!

    If you've already placed an order recently, please disregard this message.

    Best regards,
    The Irwin Naturals Team
    """
                logger.info(f"Sending reminder to {recipient} (CC: {rep_email_cc or 'None'}) for account {account.canonical_code}...")
                
                email_sent = send_email(
                    subject=subject,
                    body=body,
                    recipient=recipient,
                    cc_recipient=rep_email_cc
                )

                if email_sent:
                    logger.info(f"Email successfully sent to {recipient} (CC: {rep_email_cc or 'None'}).")
                    try:
                        account.reminder_state = 'SENT'
                        account.reminder_sent_at = datetime.utcnow()
                        # *** MODIFICATION: Store the last_purchase_date at the time of sending ***
                        account.notified_last_purchase_date = account.last_purchase_date 
                        processed_ids.append(account.id)
                        success_count += 1
                    except Exception as update_err:
                        logger.error(f"Failed to update reminder status for {account.canonical_code} (recipient: {recipient}, CC: {rep_email_cc or 'None'}) after successful send: {update_err}", exc_info=True)
                else:
                    logger.error(f"Failed to send email to {recipient} (CC: {rep_email_cc or 'None'}) for account {account.canonical_code}.")
                    fail_count += 1
                time.sleep(0.5) 

            if processed_ids: 
                try:
                    logger.info(f"Committing updates for {len(processed_ids)} accounts marked as SENT.")
                    print(f"Committing updates for {len(processed_ids)} accounts...")
                    db.session.commit()
                    logger.info("Database updates committed.")
                except Exception as commit_err:
                    logger.error(f"Database commit failed after processing reminders: {commit_err}", exc_info=True)
                    print(f"ERROR: Database commit failed: {commit_err}")
                    db.session.rollback()
            else:
                logger.info("No accounts processed that required DB commit for reminder status.")

            logger.info(f"Reminder task finished. Sent: {success_count}, Failed: {fail_count}.")
            print(f"Reminder task finished. Sent: {success_count}, Failed: {fail_count}.")

        except Exception as e:
            logger.error(f"An error occurred during the send-reminders command: {e}", exc_info=True)
            print(f"ERROR: An unexpected error occurred: {e}")
            if db.session.is_active:
                try:
                    db.session.rollback()
                    logger.info("Rolled back database session due to error.")
                except Exception as rollback_err:
                    logger.error(f"Error during rollback: {rollback_err}", exc_info=True)

    @app.cli.command("recalculate-predictions")
    @with_appcontext
    def recalculate_predictions_command():
        logger = logging.getLogger('flask.app') 
        logger.info("Starting prediction recalculation via CLI command...")
        print("Starting prediction recalculation...") 

        try:
            logger.info("Calling recalculate_predictions_and_metrics...")
            predictions_df = recalculate_predictions_and_metrics()

            if predictions_df is None or predictions_df.empty:
                logger.error("Prediction recalculation failed or returned no data.")
                print("ERROR: Prediction recalculation failed or returned no data.")
                return 

            if 'id' not in predictions_df.columns:
                 logger.error("CRITICAL ERROR: DataFrame from pipeline is missing 'id' column. Aborting update.")
                 print("ERROR: Recalculation function did not return the required 'id' column.")
                 return

            logger.info(f"Recalculated metrics for {len(predictions_df)} accounts. Preparing database update...")
            print(f"Recalculated metrics for {len(predictions_df)} accounts. Preparing database update...")
            
            # +++ THE FIX: More robust NaN/NaT handling +++
            # Replace numpy NaN with None for JSON/DB compatibility
            # Replace pandas NaT (Not a Time) with None
            predictions_df = predictions_df.replace({np.nan: None, pd.NaT: None})

            # Convert the entire DataFrame to a list of dictionaries.
            # This is simpler and less error-prone than iterating and building manually.
            update_mappings = predictions_df.to_dict(orient='records')
            
            # We must still filter out any records that have a null ID after conversion
            update_mappings = [m for m in update_mappings if m.get('id') is not None]

            logger.info(f"Prepared {len(update_mappings)} mappings for bulk update.")
            print(f"Prepared {len(update_mappings)} mappings for bulk update.")

            if update_mappings:
                try:
                    logger.info(f"Performing bulk update...")
                    print(f"Performing bulk update...")
                    
                    # We need to add updated_at timestamp manually to each mapping
                    now_utc = datetime.utcnow()
                    for mapping in update_mappings:
                        mapping['updated_at'] = now_utc
                    
                    db.session.bulk_update_mappings(AccountPrediction, update_mappings)
                    db.session.commit()
                    logger.info(f"Successfully updated {len(update_mappings)} accounts.")
                    print(f"Successfully updated {len(update_mappings)} accounts.")
                except Exception as update_err:
                    logger.error(f"Error during bulk update or commit: {update_err}", exc_info=True)
                    print(f"ERROR during bulk update: {update_err}")
                    db.session.rollback() 
                    print("ERROR: Database changes rolled back.")
            else:
                logger.info("No valid records prepared for update.")
                print("No valid records prepared for update.")

        except Exception as e:
            logger.error(f"An error occurred during recalculation command: {e}", exc_info=True)
            print(f"ERROR: An unexpected error occurred: {e}")
            if db.session.is_active : db.session.rollback()

        logger.info("Recalculation command finished.")
        print("Recalculation command finished.")

    # Register additional CLI commands
    try:
        from populate_transaction_item_codes_optimized import populate_item_codes_optimized_command
        app.cli.add_command(populate_item_codes_optimized_command)
        app.logger.info("Registered CLI command: populate-item-codes-optimized")
    except ImportError as e:
        app.logger.error(f"Failed to import or register CLI command 'populate-item-codes-optimized': {e}")
        
    try:
        from data_migration_sku_setup import migrate_historical_skus_command
        app.cli.add_command(migrate_historical_skus_command)
        app.logger.info("Registered CLI command: migrate-historical-skus")
    except ImportError as e:
        app.logger.error(f"Failed to import or register CLI command 'migrate-historical-skus': {e}")

    # Initial setup logic within app context
    with app.app_context():
        try:
            app.logger.info("Ensuring data directories exist...")
            os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
            # ... other setup logic ...
        except Exception as setup_err:
            app.logger.error(f"Error during initial setup: {setup_err}", exc_info=True)

    app.logger.info("Flask application created and configured.")
    return app


# --- Step 6: Create the App Instance ---
# This line is called when Python first executes this file.
app = create_app()