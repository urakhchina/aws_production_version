# routes/main_routes.py (or wherever your main app routes are)

from flask import Blueprint, render_template, request, redirect, url_for, flash
from flask_login import current_user, login_required # Assuming Flask-Login for user session
from sqlalchemy import select, distinct # Import select and distinct
from models import db, AccountPrediction, ActivityLog # Import your models
# Removed forms import - assumed not defined in this snippet, handle elsewhere
# from forms import LogActivityForm # Import your new form
from datetime import datetime
import logging # Make sure logging is imported

# --- Setup Logger ---
logger = logging.getLogger(__name__)

# --- Assume LogActivityForm is defined elsewhere ---
# Placeholder form class if not imported
class LogActivityForm:
    # Dummy attributes to avoid errors in the route code below
    # Replace with actual form definition using Flask-WTF
    account_card_code = type('obj', (object,), {'choices': [], 'data': None})()
    activity_type = type('obj', (object,), {'data': None})()
    notes = type('obj', (object,), {'data': None})()
    outcome = type('obj', (object,), {'data': None})()
    activity_datetime = type('obj', (object,), {'data': None})()
    errors = {} # To mimic form errors

    def validate_on_submit(self):
        # Dummy validation - replace with actual Flask-WTF validation
        logger.warning("Using dummy LogActivityForm.validate_on_submit()")
        # Basic check: ensure account is selected
        if not self.account_card_code.data:
            self.errors['account_card_code'] = ['Please select an account.']
            return False
        return True # Assume valid for now


main_bp = Blueprint('main', __name__)

# --- Route to display the logging form ---
@main_bp.route('/log-activity', methods=['GET'])
@login_required # Ensure user is logged in
def log_activity_form():
    """ Displays the activity logging form, populating account choices. """
    form = LogActivityForm() # Replace with your actual form class

    # --- Populate Account Choices Dynamically (SQLAlchemy 2.x) ---
    try:
        # Use current_user attributes directly if available and correct type
        # Ensure current_user.sales_rep_id exists and is the correct type (e.g., string)
        rep_id = str(current_user.sales_rep_id) # Example: Get ID and ensure string

        # --- Query using SQLAlchemy 2.x ---
        stmt = select(
                AccountPrediction.canonical_code, # Select canonical_code
                AccountPrediction.name
               ).where(
                   AccountPrediction.sales_rep == rep_id # Filter by rep ID
               ).distinct().order_by(
                   AccountPrediction.name
               )
        # Execute and get results as Row objects
        accounts_rows = db.session.execute(stmt).all()
        # --- End Query ---

        # Format choices for the SelectField: (value, label)
        # Value should be canonical_code
        form.account_card_code.choices = [('', '-- Select Account --')] + \
                                         [(acc.canonical_code, f"{acc.name} ({acc.canonical_code})") for acc in accounts_rows]
    except AttributeError:
         logger.error(f"User object (id={current_user.id}) missing required 'sales_rep_id' attribute.")
         flash("Could not determine your Sales Rep ID. Please contact support.", "danger")
         form.account_card_code.choices = [('', '-- Error Loading Accounts --')]
    except Exception as e:
        logger.error(f"Error fetching accounts for user {current_user.id}: {e}", exc_info=True)
        flash("Could not load accounts for selection.", "danger")
        form.account_card_code.choices = [('', '-- Error Loading Accounts --')]

    return render_template('log_activity.html', form=form) # Pass your actual template name

# --- Route to handle form submission ---
@main_bp.route('/log-activity', methods=['POST'])
@login_required
def log_activity_submit():
    """ Handles the submission of the activity logging form. """
    form = LogActivityForm() # Replace with your actual form class

    # --- Repopulate choices on POST in case of validation error (SQLAlchemy 2.x) ---
    try:
        rep_id = str(current_user.sales_rep_id) # Get rep ID again

        # --- Query using SQLAlchemy 2.x ---
        stmt = select(
                AccountPrediction.canonical_code, # Select canonical_code
                AccountPrediction.name
               ).where(
                   AccountPrediction.sales_rep == rep_id
               ).distinct().order_by(
                   AccountPrediction.name
               )
        accounts_rows = db.session.execute(stmt).all()
        # --- End Query ---

        # Repopulate choices using canonical_code
        form.account_card_code.choices = [('', '-- Select Account --')] + \
                                         [(acc.canonical_code, f"{acc.name} ({acc.canonical_code})") for acc in accounts_rows]
    except AttributeError:
         logger.error(f"User object (id={current_user.id}) missing 'sales_rep_id' on POST.")
         flash("An error occurred loading account list. Please contact support.", "danger")
         form.account_card_code.choices = [('', '-- Error Loading Accounts --')]
    except Exception as e:
         logger.error(f"Error repopulating accounts on POST for user {current_user.id}: {e}", exc_info=True)
         flash("An error occurred loading account list.", "danger")
         form.account_card_code.choices = [('', '-- Error Loading Accounts --')]
         # Let validation handle form re-rendering

    # --- Validate Form ---
    if form.validate_on_submit(): # Use your actual form's validation
        try:
            # Get data from form - use canonical_code now
            canonical_code = form.account_card_code.data # Value from choices is now canonical_code
            activity_type = form.activity_type.data
            notes = form.notes.data
            outcome = form.outcome.data if form.outcome.data else None
            activity_dt = form.activity_datetime.data or datetime.utcnow()

            # Get sales rep details from current_user
            sales_rep_id = str(current_user.sales_rep_id) if hasattr(current_user, 'sales_rep_id') else None
            sales_rep_name = current_user.sales_rep_name if hasattr(current_user, 'sales_rep_name') else 'Unknown Rep'

            # --- Get account details using canonical_code (SQLAlchemy 2.x) ---
            # Select name and potentially base_card_code for the log
            acc_details_stmt = select(AccountPrediction.name, AccountPrediction.base_card_code)\
                               .where(AccountPrediction.canonical_code == canonical_code)
            account = db.session.execute(acc_details_stmt).first() # Fetch first Row or None
            account_name = account.name if account else 'Unknown Account'
            base_code = account.base_card_code if account else None # Get base code if needed
            # --- End Get Account Details ---


            # Create new log entry using canonical_code
            new_log = ActivityLog(
                canonical_code=canonical_code, # Store canonical code
                base_card_code=base_code,      # Store base code for context (optional)
                account_name=account_name,
                sales_rep_id=sales_rep_id,
                sales_rep_name=sales_rep_name,
                activity_type=activity_type,
                activity_datetime=activity_dt,
                notes=notes,
                outcome=outcome
                # duration_minutes = form.duration_minutes.data # Add if form field exists
            )

            db.session.add(new_log)
            db.session.commit()

            flash(f'{activity_type} logged successfully for {account_name}!', 'success')
            return redirect(url_for('main.log_activity_form')) # Redirect back to form

        except Exception as e:
            db.session.rollback()
            logger.error(f"Error saving activity log for user {current_user.id}, account {canonical_code}: {e}", exc_info=True)
            flash('Error saving activity log. Please try again.', 'danger')

    else:
        # Form validation failed
        logger.warning(f"Activity log form validation failed: {form.errors}")
        flash('Please correct the errors below.', 'warning')

    # Re-render form if validation fails or exception occurs before redirect
    return render_template('log_activity.html', form=form) # Pass your actual template name

# --- Make sure blueprint is registered in app.py/factory ---
# from routes.main_routes import main_bp
# app.register_blueprint(main_bp)