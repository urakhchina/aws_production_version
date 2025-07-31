# models.py
import json
from flask_sqlalchemy import SQLAlchemy
from datetime import datetime

# Initialize SQLAlchemy
db = SQLAlchemy()

# --- Main Account Prediction Table ---
class AccountPrediction(db.Model):
    __tablename__ = 'account_predictions'

    # --- Core Columns ---
    id = db.Column(db.Integer, primary_key=True) # Internal primary key

    # --- Identifiers (NEW STRUCTURE) ---
    canonical_code = db.Column(db.String(350), unique=True, nullable=False, index=True) # Main business key, unique per store location
    base_card_code = db.Column(db.String(50), nullable=True, index=True) # Original base code (e.g., 02AZ3377)
    ship_to_code = db.Column(db.String(50), nullable=True, index=True) # ShipTo code from SAP, if available

    # --- Basic Account Info ---
    name = db.Column(db.String(100), nullable=False)
    full_address = db.Column(db.String(255), nullable=True)
    customer_id = db.Column(db.String(255)) # Comma-separated list of unique customer IDs? Review if needed.
    customer_email = db.Column(db.String(255), nullable=True) # NEW: Customer email for reminders


    # --- Sales Rep & Distributor ---
    sales_rep = db.Column(db.String(255), index=True) # Keep index
    sales_rep_name = db.Column(db.String(255))
    distributor = db.Column(db.String(255), index=True) # Keep index

    # --- Purchase History Summary ---
    products_purchased = db.Column(db.Text) # Comma-separated list? Consider JSON or separate table long-term.
    last_purchase_date = db.Column(db.DateTime)
    last_purchase_amount = db.Column(db.Float)

    account_total = db.Column(db.Float) # Consolidated total revenue
    purchase_frequency = db.Column(db.Integer, default=0) # Consolidated total purchase count
    days_since_last_purchase = db.Column(db.Integer, default=0)

    # --- Purchase Cadence & Prediction ---
    median_interval_days = db.Column(db.Integer) # Based on recent history of this canonical account
    next_expected_purchase_date = db.Column(db.DateTime)
    days_overdue = db.Column(db.Integer, default=0) # Based on next_expected_purchase_date

    # --- Cadence Metrics (NEW) ---
    avg_interval_py = db.Column(db.Float, nullable=True) # Avg interval in Previous Year
    avg_interval_cytd = db.Column(db.Float, nullable=True) # Avg interval Current Year To Date
    # cadence_lag_days = db.Column(db.Float, nullable=True) # Optional: Store calculated lag explicitly

    # --- Year End Pace (YEP) Metrics (NEW) ---
    cytd_revenue = db.Column(db.Float, default=0.0) # Accurate CYTD Revenue
    py_total_revenue = db.Column(db.Float, default=0.0) 
    yep_revenue = db.Column(db.Float, nullable=True) # Projected Year End Pace Revenue
    pace_vs_ly = db.Column(db.Float, nullable=True) # Projected Pace vs Last Year's Total Revenue
    avg_order_amount_cytd = db.Column(db.Float, nullable=True)

    # --- Churn Metrics ---
    #churn_risk_score = db.Column(db.Float, default=0.0)
    # purchase_trend = db.Column(db.Float, default=0.0) # Consider if still needed/calculable
    #purchase_consistency = db.Column(db.Float, default=0.0) # Consider if still needed/calculable
    #avg_purchase_interval = db.Column(db.Float, default=0.0) # Superseded by avg_interval_cytd/py? Keep if different calc.
    #purchase_interval_trend = db.Column(db.Float, default=0.0) # Consider if still needed/calculable
    #expected_purchase_likelihood = db.Column(db.Float, default=0.0) # Still useful

    # --- RFM Analysis ---
    recency_score = db.Column(db.Integer, default=0)
    frequency_score = db.Column(db.Integer, default=0)
    monetary_score = db.Column(db.Integer, default=0)
    rfm_score = db.Column(db.Float, default=0.0)
    rfm_segment = db.Column(db.String(30), default="")

    # --- Health Score ---
    health_score = db.Column(db.Float, default=0.0)
    health_category = db.Column(db.String(20), default="")

    # --- Priority Scores ---
    priority_score = db.Column(db.Float, default=0.0) # Original score
    enhanced_priority_score = db.Column(db.Float, default=0.0) # Enhanced score

    # --- YoY Metrics (Keep for reference, but YEP/Pace are primary now) ---
    yoy_revenue_growth = db.Column(db.Float, default=0.0)
    yoy_purchase_count_growth = db.Column(db.Float, default=0.0)

    # --- Product Coverage ---
    product_coverage_percentage = db.Column(db.Float, default=0.0, nullable=True)
    carried_top_products_json = db.Column(db.Text, nullable=True)
    missing_top_products_json = db.Column(db.Text, nullable=True)

    # +++ Add New Columns for Representative Last Order +++
    rep_last_order_date = db.Column(db.DateTime, nullable=True, index=True) # Date of the representative order
    rep_last_order_amount = db.Column(db.Float, nullable=True) # Amount of the representative order
    # +++ End New Columns +++

    historical_avg_daily_order = db.Column(db.Float, nullable=True) # Avg daily total over entire history

    revenue_trend_slope = db.Column(db.Float, nullable=True)
    revenue_trend_r_squared = db.Column(db.Float, nullable=True)
    revenue_trend_intercept = db.Column(db.Float, nullable=True)

    # --- Growth Opportunity Engine Fields (NEW) ---
    target_yep_plus_1_pct = db.Column(db.Float, nullable=True)
    additional_revenue_needed_eoy = db.Column(db.Float, nullable=True)
    suggested_next_purchase_amount = db.Column(db.Float, nullable=True)
    recommended_products_next_purchase_json = db.Column(db.Text, nullable=True) # For storing [{"sku": "...", "reason": "..."}, ...]
    growth_engine_message = db.Column(db.String(500), nullable=True) # Or db.Text if messages can be very long
    avg_purchase_cycle_days = db.Column(db.Float, nullable=True) # To store median_interval_days

    # --- Reminder System Fields (NEW) ---
    reminder_state = db.Column(db.String(20), nullable=True, index=True) # State of customer reminder (sent, pending, etc.)
    reminder_sent_at = db.Column(db.DateTime, nullable=True, index=True) # When the last reminder was sent
    notified_last_purchase_date = db.Column(db.DateTime, nullable=True)


    # --- Timestamps ---
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    rolling_sku_analysis_json = db.Column(db.Text, nullable=True)

    # --- Properties for Product Lists (Keep as is) ---
    @property
    def carried_top_products(self):
        if not self.carried_top_products_json: return []
        try: return json.loads(self.carried_top_products_json)
        except json.JSONDecodeError: return []

    @property
    def missing_top_products(self):
        if not self.missing_top_products_json: return []
        try: return json.loads(self.missing_top_products_json)
        except json.JSONDecodeError: return []

    # --- Table Arguments (Indexes) ---
    __table_args__ = (
        db.Index('idx_prediction_base_code', 'base_card_code'),
        db.Index('idx_prediction_ship_to', 'ship_to_code'),
        db.Index('idx_prediction_dist_rep', 'distributor', 'sales_rep'), # Keep useful index
        # Add other indexes as needed, e.g., on dates or scores for dashboard filtering
        db.Index('idx_prediction_next_due', 'next_expected_purchase_date'),
        db.Index('idx_prediction_rep_due', 'sales_rep', 'next_expected_purchase_date'),
    )

    def __repr__(self):
        # Updated representation
        return f"<AccountPrediction canonical_code={self.canonical_code} name={self.name}>"

# --- Historical Revenue Table ---
class AccountHistoricalRevenue(db.Model):
    """Stores historical revenue data aggregated BY YEAR for each canonical_code."""
    __tablename__ = 'account_historical_revenues'

    id = db.Column(db.Integer, primary_key=True)

    # --- Identifiers (NEW STRUCTURE) ---
    canonical_code = db.Column(db.String(350), nullable=False, index=True) # Renamed from card_code
    base_card_code = db.Column(db.String(50), nullable=True, index=True)
    ship_to_code = db.Column(db.String(50), nullable=True, index=True)

    # --- Aggregated Data ---
    year = db.Column(db.Integer, nullable=False, index=True)
    total_revenue = db.Column(db.Float, default=0.0)
    transaction_count = db.Column(db.Integer, default=0) # Total transactions in that year

    # --- Associated Info (Consider if needed here or just in Prediction) ---
    name = db.Column(db.String(100)) # Name associated with this canonical code in this year
    sales_rep = db.Column(db.String(255), index=True)
    distributor = db.Column(db.String(100), index=True)

    # --- Products ---
    yearly_products_json = db.Column(db.Text, nullable=True)

    # --- Table Arguments (Indexes & Constraints) ---
    __table_args__ = (
        # Ensure uniqueness based on the NEW canonical code and year
        db.UniqueConstraint('canonical_code', 'year', name='uix_canon_code_year'),
        # Update indexes to use canonical_code and add base_code index
        db.Index('idx_hist_canon_code_year', 'canonical_code', 'year'),
        db.Index('idx_hist_base_code_year', 'base_card_code', 'year'),
        db.Index('idx_hist_rep_year', 'sales_rep', 'year'), # Keep others if still relevant
        db.Index('idx_hist_distributor_year', 'distributor', 'year'),
    )

    def __repr__(self):
        return f"<AccountHistoricalRevenue canonical_code={self.canonical_code} year={self.year} revenue={self.total_revenue}>"

    # --- Properties for Product Lists (Keep as is) ---
    @property
    def yearly_products(self):
        if not self.yearly_products_json: return []
        try: return json.loads(self.yearly_products_json)
        except json.JSONDecodeError: return []

    def set_yearly_products(self, product_sku_list):
        if not product_sku_list: self.yearly_products_json = None
        else:
            unique_sorted_products = sorted(list(set(product_sku_list)))
            self.yearly_products_json = json.dumps(unique_sorted_products)

# --- Snapshot Table ---
class AccountSnapshot(db.Model):
    """Stores periodic YEARLY snapshots of account metrics using canonical_code."""
    __tablename__ = 'account_snapshots'

    id = db.Column(db.Integer, primary_key=True)

    # --- Identifiers (NEW STRUCTURE) ---
    canonical_code = db.Column(db.String(350), nullable=False, index=True) # Renamed from card_code
    base_card_code = db.Column(db.String(50), nullable=True, index=True)
    ship_to_code = db.Column(db.String(50), nullable=True, index=True)

    # --- Snapshot Info ---
    snapshot_date = db.Column(db.DateTime, nullable=False) # e.g., end of year
    year = db.Column(db.Integer, nullable=False, index=True)

    # --- Key Metrics Snapshotted ---
    account_total = db.Column(db.Float) # Overall total up to that point (from Prediction)
    yearly_revenue = db.Column(db.Float) # Revenue specifically IN that year (from Historical)
    yearly_purchases = db.Column(db.Integer) # Purchases specifically IN that year (from Historical)
    health_score = db.Column(db.Float) # (from Prediction)
    #churn_risk_score = db.Column(db.Float) # (from Prediction)

    # --- Table Arguments (Indexes & Constraints) ---
    __table_args__ = (
        # Ensure uniqueness based on the NEW canonical code and year
        db.UniqueConstraint('canonical_code', 'year', name='uix_snapshot_canon_year'),
        # Update indexes
        db.Index('idx_snapshot_canon_year', 'canonical_code', 'year'),
        db.Index('idx_snapshot_base_year', 'base_card_code', 'year'),
    )

    def __repr__(self):
        return f"<AccountSnapshot canonical_code={self.canonical_code} year={self.year}>"


# --- Sales Rep Performance Table ---
# (Likely no changes needed unless linking specifically to canonical_code)
class SalesRepPerformance(db.Model):
    __tablename__ = 'sales_rep_performance'
    id = db.Column(db.Integer, primary_key=True)
    sales_rep = db.Column(db.String(255), nullable=False, index=True)
    year = db.Column(db.Integer, nullable=False)
    quarter = db.Column(db.Integer, nullable=True)
    month = db.Column(db.Integer, nullable=True)
    total_revenue = db.Column(db.Float, default=0.0)
    accounts_count = db.Column(db.Integer, default=0)
    active_accounts = db.Column(db.Integer, default=0)
    at_risk_accounts = db.Column(db.Integer, default=0)
    yoy_revenue_growth = db.Column(db.Float, default=0.0)
    yoy_accounts_growth = db.Column(db.Float, default=0.0)
    __table_args__ = (
        db.Index('idx_salesrep_perf_rep_year', 'sales_rep', 'year'), # Renamed for clarity
        db.Index('idx_salesrep_perf_rep_year_quarter', 'sales_rep', 'year', 'quarter'),
        db.Index('idx_salesrep_perf_rep_year_month', 'sales_rep', 'year', 'month'),
        db.UniqueConstraint('sales_rep', 'year', 'month', 'quarter', name='uix_rep_time_period'),
    )
    def __repr__(self):
        return f"<SalesRepPerformance sales_rep={self.sales_rep} year={self.year} month={self.month}>"

# --- Activity Log Table ---
class ActivityLog(db.Model):
    """Tracks sales representative activity logs, linked via canonical_code."""
    __tablename__ = 'activity_log'

    id = db.Column(db.Integer, primary_key=True)

    # --- Link to Account (NEW STRUCTURE) ---
    canonical_code = db.Column(db.String(350), nullable=False, index=True) # Renamed from card_code
    base_card_code = db.Column(db.String(50), nullable=True, index=True) # Optional: for context/grouping

    # --- Activity Details (Keep as is) ---
    account_name = db.Column(db.String(100), nullable=True) # Store name at time of logging
    sales_rep_id = db.Column(db.String(50), nullable=True, index=True)
    sales_rep_name = db.Column(db.String(100), nullable=True)
    activity_type = db.Column(db.String(20), nullable=False, index=True)
    activity_datetime = db.Column(db.DateTime, nullable=False, default=datetime.utcnow, index=True)
    notes = db.Column(db.Text, nullable=True)
    outcome = db.Column(db.String(50), nullable=True)
    duration_minutes = db.Column(db.Integer, nullable=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    def __repr__(self):
        return f'<ActivityLog {self.id}: {self.activity_type} for {self.canonical_code} by {self.sales_rep_name} on {self.activity_datetime}>'
    

class Transaction(db.Model):
    __tablename__ = 'transactions' # Or your preferred name

    id = db.Column(db.Integer, primary_key=True) # Simple primary key

    # --- Link to Account (Using Canonical Code) ---
    canonical_code = db.Column(db.String(350), nullable=False, index=True)
    base_card_code = db.Column(db.String(50), nullable=True, index=True) # For easy grouping/lookup
    ship_to_code = db.Column(db.String(50), nullable=True, index=True)

    # --- Transaction Details (Match Raw Data) ---
    posting_date = db.Column(db.DateTime, nullable=False, index=True)
    year = db.Column(db.Integer, nullable=False, index=True) # Store year for partitioning/indexing
    amount = db.Column(db.Float, nullable=False)
    quantity = db.Column(db.Integer, nullable=False) # Use Integer if always whole numbers
    revenue = db.Column(db.Float) 
    item_code = db.Column(db.String(50), nullable=True, index=True) # Store ITEM code (SKU)
    description = db.Column(db.Text) # Product description
    # Add other relevant fields from raw data if needed for analysis (e.g., ITEM code?)
    distributor = db.Column(db.String(100), index=True) # Maybe store distributor here?
    sales_rep = db.Column(db.String(255), index=True) # Or store rep here?

    transaction_hash = db.Column(db.String(64), nullable=True, index=True) # For SHA-256 hash

    # Optional: Add created_at timestamp for when the record was added to this table
    # created_at = db.Column(db.DateTime, default=datetime.utcnow)

    # --- Table Arguments ---
    __table_args__ = (
        # Index for common queries
        db.Index('idx_transaction_canon_date', 'canonical_code', 'posting_date'),
        db.Index('idx_transaction_canon_year', 'canonical_code', 'year'),
        db.Index('idx_transaction_item_code', 'item_code'), # Index on new item_code

        db.UniqueConstraint('transaction_hash', name='uix_transaction_hash'),
    )

    def __repr__(self):
        #return f"<Transaction id={self.id} canonical_code={self.canonical_code} date={self.posting_date} revenue={self.revenue}>"
         return f"<Transaction id={self.id} canonical_code={self.canonical_code} item_code={self.item_code} date={self.posting_date} revenue={self.revenue}>"