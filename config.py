# config.py (Revised for Production Readiness - With DEBUG block moved)
import os
import logging

# It's good practice to get a logger here too if config does complex things
logger = logging.getLogger(__name__) # This is the module-level logger

# --- Core Application Settings ---
# Use environment variables FIRST, then provide a default for local dev
SECRET_KEY = os.environ.get('SECRET_KEY', 'change-this-in-production-to-a-strong-random-key!')
DEBUG = os.environ.get('FLASK_DEBUG', 'True').lower() in ('true', '1', 't')  # Default DEBUG=True locally
#TEST_MODE = os.environ.get('TEST_MODE', 'True').lower() in ('true', '1', 't')  # Default TEST_MODE=True locally
TEST_MODE = os.environ.get('TEST_MODE', 'False').lower() in ('true', '1', 't')  # Default TEST_MODE=True locally


# ==========================================
# Scoring and Threshold Configuration
# ==========================================
# HEALTH_POOR_THRESHOLD is defined later with env var override, ensure this isn't a duplicate if it's meant to be the same
# If these are different, ensure names are unique. Assuming HEALTH_POOR_THRESHOLD later is the definitive one.
# HEALTH_POOR_THRESHOLD_SCORING = 40 # Example if it's different
PRIORITY_PACE_DECLINE_PCT_THRESHOLD = -10 # Pacing below -10% contributes to priority score
GROWTH_PACE_INCREASE_PCT_THRESHOLD = 10   # Pacing above +10% flags growth opportunity
GROWTH_HEALTH_THRESHOLD = 60             # Min health score to be considered for Growth Opps section
GROWTH_MISSING_PRODUCTS_THRESHOLD = 3    # Min number of missing top products for Growth Opps section


CURRENT_YEAR_PARTNER_CODES = {
    "02MT0134", "02MT4923", "02MT5150", "02FL3026A", "02FL9500C", "02MT5823",
    "02FL9009", "02AL3952", "02FL0120", "02GA6922", "02NC8602", "02MO3608",
    "02OK1347", "02WY0862", "02VA7287", "02UT9164", "02WY9266A", "02AZ8182",
    "02IL0461", "02MA7097", "02CA3068", "02OR0252", "02OR3602", "02OR4251",
    "02WA0933", "02WA2400A", "02WA3830", "02WA4080", "02WA4840", "02WA5386",
    "02WA5940", "02WA9981", "02WA6056", "02AZ4120", "02IL0100", "02IL0422",
    "02IL0651", "02IL1445", "02IL2086", "02IL2553", "02IL2999", "02IL3304",
    "02IL3490A", "02IL5880", "02IL6166", "02IL6924", "02IL7614", "02IL8240",
    "02IL9103", "02IL9242", "02IN1984", "02SD3900", "02PA17770", "02CA1727",
    "02NC0224", "02IL8009", "02MI9459", "02AR7558", "02SC9295", "02UT39024",
    "02WA1500", "02WA8100", "02PA6100", "02PA6100A", "02CA5111", "02MT9259",
    "02MI1731", "02NJ1021", "02NJ9355", "02PA3825", "02PA7384A", "02CA3202",
    "02CA4565", "02CA4918", "02CA6789A", "02CA8330", "02GA3003", "02WA2565",
    "02FL3360", "02LA9910", "02MD2555", "02TX1371", "02CA5669", "02FL6040",
    "02FL8311", "02FL8392", "02FL9600", "02KS1088", "02KS4283", "02KS4365",
    "02MO0010", "02NE0869", "02NJ3900", "02NJ7045", "02NJ8337", "02NJ8555",
    "02NJ9084", "02NY5481", "02NY6214", "02NY9146", "02OK1533", "02OK6060",
    "02VA7525", "02AZ5014", "02MT9412B", "02NJ5804", "02WA9006", "02AL4260",
    "02AL8458", "02CA6411", "02ID8135", "02ID9730", "02IN1555", "02WA3655",
    "02WA6413", "02WV1024", "02TN6682", "02FL6869", "02MI8744", "02MI0852",
    "02MI2307", "02MI3026B", "02WV5801", "02GA3670", "02IN3151", "02MI4942",
    "02MI6268", "02MI6388", "02MI6630A", "02MI7555", "02MT4668", "02TN3663",
    "02AZ8224", "02CO3525", "02FL3790", "02IL1023", "02MI5250", "02UT0305",
    "02UT1118", "02UT4500", "02CA7873", "02GA1566", "02IL3409A", "02IL9114",
    "02CA8787", "02FL7047", "02IN9525", "02MO3341", "02NJ3555A", "02PR7800",
    "02AK8910", "02SC3222", "02AK5433", "02MS0211", "02OK8609", "02UT1389",
    "02CA6321", "02LA6080", "02CA2452", "02IN6540", "02OH2878", "02FL21962",
    "02IL1555", "02MI9684", "02MO0909", "02GA8465", "02IN6382", "02NV8818",
    "02PA7707", "02VA0100", "02MO0990A", "02NY2463A", "02FL2818", "02IN4717",
    "02MO1650A", "02MT5260", "02OH0431", "02LA9037", "02CO1200", "02CO4139",
    "02GA9999", "02NC4315", "02UT2253", "02FL7553", "02MI5908", "02WI5681",
    "02WI8543", "02AL2050", "02AK1444", "02MO7852", "09MS06979", "02MI5030",
    "02NC0300", "02VA6711", "02TN1148", "02AZ6494", "02AR5263", "02PA1299",
    "02TX1993", "02TX5237", "02TX9483", "02FL6825", "02OR7756", "02ID7238",
    "02MD0770", "02TX5165", "02GA6083", "02AZ9284"
}


# --- Base Directory ---
# Determines paths based on execution environment
# --- Base Directory ---
IN_AWS = os.environ.get('AWS_EXECUTION_ENV') is not None
if IN_AWS:
    BASE_DIR = '/var/app/current'
else:
    BASE_DIR = os.path.abspath(os.path.dirname(__file__))

# --- Database Configuration ---
DEFAULT_DB_PATH = os.path.join(BASE_DIR, 'data', 'sales_reminder.db')
DEFAULT_SQLALCHEMY_DATABASE_URI = f'sqlite:///{DEFAULT_DB_PATH}'

# ####################################################################
# ### TEMPORARY CHANGE FOR LOCAL POSTGRESQL DEVELOPMENT ###
# ####################################################################
# We are temporarily overriding the logic to FORCE the use of the local PostgreSQL DB.
# Remember to change this back before deploying to AWS.

# Original line is commented out:
# SQLALCHEMY_DATABASE_URI = os.environ.get('SQLALCHEMY_DATABASE_URI', DEFAULT_SQLALCHEMY_DATABASE_URI)

# New hardcoded line for local development:
SQLALCHEMY_DATABASE_URI = os.environ.get('SQLALCHEMY_DATABASE_URI')

if not SQLALCHEMY_DATABASE_URI:
    # If the environment variable is not set AT ALL, the app will refuse to start.
    # This prevents it from accidentally connecting to the wrong database.
    logger.critical("FATAL: SQLALCHEMY_DATABASE_URI environment variable is not set. Application cannot start.")
    raise ValueError("FATAL: SQLALCHEMY_DATABASE_URI environment variable is not set.")

SQLALCHEMY_TRACK_MODIFICATIONS = False
# --- Email Configuration (SMTP) ---
# Use environment variables first, fall back to Mailtrap for local dev
SMTP_SERVER = os.environ.get('SMTP_SERVER', "sandbox.smtp.mailtrap.io")
try:  # Ensure port is an integer
    SMTP_PORT = int(os.environ.get('SMTP_PORT', 2525))
except ValueError:
    logger.warning("Invalid SMTP_PORT env var, using default 2525.")
    SMTP_PORT = 2525
EMAIL_USERNAME = os.environ.get('EMAIL_USERNAME', "fb2d6d5997399d")  # Use placeholder
EMAIL_PASSWORD = os.environ.get('EMAIL_PASSWORD', "b5edb794f6782a")  # Use placeholder
FROM_EMAIL = os.environ.get('FROM_EMAIL', "salesreminders@irwinnaturals.com'") # Note: extra ' at the end, might be a typo

# --- Webhook Security ---
# Should ONLY come from environment variable
HMAC_SECRET_KEY = os.environ.get('HMAC_SECRET_KEY')
# Log warning if missing (especially important if not in DEBUG mode)
if not HMAC_SECRET_KEY:
    if not DEBUG: # DEBUG is defined at the top
        logger.warning("PRODUCTION WARNING: HMAC_SECRET_KEY environment variable is not set! Webhook security compromised!")
    else:
        logger.info("DEBUG INFO: HMAC_SECRET_KEY environment variable is not set (webhook auth may fail).")

# --- File Uploads ---
DEFAULT_UPLOAD_FOLDER = os.path.join(BASE_DIR, 'data', 'uploads')
UPLOAD_FOLDER = os.environ.get('UPLOAD_FOLDER', DEFAULT_UPLOAD_FOLDER)

# --- Application Specific Settings ---
SALES_REP_MAPPING = {
    "Andy Chasen": "Andy@irwinnaturals.com",
    "Ashley Bolanos": "ashleyB@irwinnaturals.com",
    "Christina Antrim": "christina@irwinnaturals.com",
    "Lisa Clarke": "Lisa@irwinnaturals.com",
    "Liz Vasquez": "LizP@irwinnaturals.com",
    "Mariano Cruz": "mariano@irwinnaturals.com",
    "Marshall McClean": "marshall@irwinnaturals.com",
    "Mohit Kumar": "mohit@irwinnaturals.com",
    "Trina Hilley": "Trina@irwinnaturals.com",
    "Scott Hendren": "scotth@irwinnaturals.com",
    "SPROUTS": "marshall@irwinnaturals.com",
    "Customer Care": "LizP@irwinnaturals.com",
    "Donald Corgill": "Donald@irwinnaturals.com",
    "NGVC": "marshall@irwinnaturals.com",
    "Mid-West Territory": "LizP@irwinnaturals.com",
    "EARTH FARE": "mariano@irwinnaturals.com",
    "CLOS": "LizP@irwinnaturals.com"
}

'''
TOP_30_PRODUCTS = [
    "Milk Thistle Liver Detox", "Berberine-Body + Fat Burner", "Concentrated Maca Root",
    "Steel-Libido RED", "Testosterone UP RED", "Stored-Fat Belly Burner",
    "Kidney & Liver Super Cleanse", "Testosterone UP Men Over 40", "Magnesium + Ashwagandha",
    "Steel-Libido v2", "L-Arginine + Horny Goat Weed", "Liver Detox & Blood Refresh",
    "Testosterone UP for Men 60+", "Testosterone-Extra Fat Burner", "Testosterone Mega-Boost RED",
    "L-Citrulline + L-Arginine Booster for Men", "Extra Strength Testosterone UP v2",
    "Sleep Fit + PM Fat Burner", "Healthy Brain ALL-DAY Focus", "Testosterone UP PRO-Growth",
    "Libido-Max for Active Men", "Testosterone UP ThermoBurn-MAX",
    "Stored-Fat Belly Burner Plus Calorie-BURN", "Extra Strength Ashwagandha Mind & Body",
    "Testosterone UP Strength & Size", "5-HTP Extra Mood & Relaxation",
    "Nitric Oxide Pre-Sport with L-Citrulline", "Power-Male Horny Goat Weed with Nitric Oxide Booster",
    "Beets-4-Cardio Peak Performance", "Hair Follicle Stimulator"
]
'''

'''
TOP_30_PRODUCTS = [
    "IN058549", "IN051044", "IN059205", "IN057860", "IN058598", "IN058638", 
    "IN059457", "IN050607", "IN050432", "IN050560", "IN050329", "IN059461", 
    "IN051142", "IN059676", "IN050522", "IN050789", "IN051138", "IN050617", 
    "IN050822", "IN050711", "IN050255", "IN051040", "IN051163", "IN059449", 
    "IN050587", "IN058537", "IN058486", "IN050598", "IN051155", "IN051159"
]


TOP_30_SET = {
    "NS1197", "NS1195", "NS1263", "NS1251", "NS1215", "NS1257", "NS1224",
    "NS1166", "NS1245", "NS1172", "NS1196", "NS1188", "NS1279", "NS1117",
    "NS1173", "NS1269", "NS1114", "NS1187", "NS1244", "NS1240", "NS1216",
    "NS1159", "NS050967", "NS1254", "NS1243", "NS1207", "NS1168", "NS1107",
    "NS1198", "NS1249"
}
'''
'''
TOP_30_SET = {
"0071036358549.0", "84008141044.0", "710363592059.0", "710363578602.0", "710363585983.0", "710363586386.0",
"710363594572.0", "840081406076.0", "840081404324.0", "840081405604.0", "840081403297.0", "710363594619.0",
"0084008141142.0", "710363596767.0", "710363594404.0", "840081407899.0", "840081411384.0", "840081410684.0",
"840081408223.0", "840081407110.0", "0084008140255.0", "840081410400.0", "840081411636.0", "710363594497.0",
"0084008140587.0", "710363585372.0", "710363584863.0", "840081405987.0", "84008141155.0", "840081411599.0"
}
'''
'''
TOP_30_SET = {
    "0071036358549", "840081410448", "710363592059", "710363578602",
    "710363585983", "710363586386", "710363594572", "840081406076",
    "840081404324", "840081405604", "840081403297", "710363594619",
    "0084008141142", "710363596767", "710363594404", "840081407899",
    "840081411384", "840081410684", "840081408223", "840081407110",
    "0084008140255", "840081410400", "840081411636", "710363594497",
    "0084008140587", "710363585372", "710363584863", "840081405987",
    "840081411551", "840081411599"
}

#TOP_30_SET = set(TOP_30_PRODUCTS)
TOP_30_MATCH_SET = TOP_30_SET | {f"{s}.0" for s in TOP_30_SET}
'''

TOP_30_CLEAN = {
    "0071036358549", "840081410448", "710363592059", "710363578602",
    "710363585983", "710363586386", "710363594572", "840081406076",
    "840081404324", "840081405604", "840081403297", "710363594619",
    "0084008141142", "710363596767", "710363594404", "840081407899",
    "840081411384", "840081410684", "840081408223", "840081407110",
    "0084008140255", "840081410400", "840081411636", "710363594497",
    "0084008140587", "710363585372", "710363584863", "840081405987",
    "840081411551", "840081411599"
}

# Version with .0 suffix (what's actually in your database)
TOP_30_WITH_DECIMAL = {f"{s}.0" for s in TOP_30_CLEAN}

# IMPORTANT: Use the decimal version as primary since that's what's in the DB
TOP_30_SET = TOP_30_WITH_DECIMAL

# Include both for comprehensive matching
TOP_30_MATCH_SET = TOP_30_CLEAN | TOP_30_WITH_DECIMAL

def normalize_upc_for_matching(upc_str):
    """Normalize UPC for TOP_30 matching - handles decimal formats"""
    if not upc_str:
        return ""
    
    # Handle pandas NaN
    if pd.isna(upc_str):
        return ""
    
    upc_str = str(upc_str).strip()
    
    # If it's already in one of our sets, return as-is
    if upc_str in TOP_30_MATCH_SET:
        return upc_str
    
    # If it doesn't have .0, try adding it
    if not upc_str.endswith('.0'):
        with_decimal = f"{upc_str}.0"
        if with_decimal in TOP_30_MATCH_SET:
            return with_decimal
    
    # If it has .0, try removing it
    if upc_str.endswith('.0'):
        without_decimal = upc_str[:-2]
        if without_decimal in TOP_30_MATCH_SET:
            return without_decimal
    
    return upc_str

def is_top_30_product(upc):
    """Check if a UPC is in the TOP_30 list - handles all formats"""
    if not upc:
        return False
    
    # Handle pandas NaN
    try:
        if pd.isna(upc):
            return False
    except:
        pass  # If pd isn't imported or upc isn't a pandas type, continue
    
    upc_str = str(upc).strip()
    
    # Check all possible formats
    return (upc_str in TOP_30_MATCH_SET or 
            f"{upc_str}.0" in TOP_30_MATCH_SET or
            (upc_str.endswith('.0') and upc_str[:-2] in TOP_30_MATCH_SET))


# --- Dashboard URL Section (MODIFIED FOR DEBUGGING) ---
# This section MUST be defined before the final DEBUG logging block if DASHBOARD_URL is logged there.
local_port = os.environ.get("PORT", 5000) # Used for DEFAULT_DASHBOARD_URL

_dashboard_url_from_env = os.environ.get('DASHBOARD_URL')
logger.info(f"CONFIG.PY (DASHBOARD_URL section): os.environ.get('DASHBOARD_URL') resolved to: '{_dashboard_url_from_env}'")

DEFAULT_DASHBOARD_URL = f'http://localhost:{local_port}/dashboard'

if _dashboard_url_from_env:
    DASHBOARD_URL = _dashboard_url_from_env
    logger.info(f"CONFIG.PY (DASHBOARD_URL section): Using DASHBOARD_URL from environment: '{DASHBOARD_URL}'")
else:
    DASHBOARD_URL = DEFAULT_DASHBOARD_URL
    logger.info(f"CONFIG.PY (DASHBOARD_URL section): DASHBOARD_URL from environment NOT FOUND. Using DEFAULT: '{DASHBOARD_URL}'")


# --- Thresholds (Allow override) ---
# These must also be defined before they are logged in the final DEBUG block.
try:
    CHURN_HIGH_RISK_THRESHOLD = int(os.environ.get('CHURN_HIGH_RISK_THRESHOLD', 70))
except ValueError:
    logger.warning("Invalid CHURN_HIGH_RISK_THRESHOLD, using default 70.")
    CHURN_HIGH_RISK_THRESHOLD = 70

# HEALTH_POOR_THRESHOLD was defined near the top. If this is the one meant to be configurable via env var,
# ensure the earlier one is removed or renamed to avoid confusion or redefinition.
# Assuming this is the definitive one:
try:
    HEALTH_POOR_THRESHOLD = int(os.environ.get('HEALTH_POOR_THRESHOLD', 40)) # Overwrites earlier static definition
except ValueError:
    logger.warning("Invalid HEALTH_POOR_THRESHOLD (env var), using default 40.")
    HEALTH_POOR_THRESHOLD = 40 # Fallback if earlier one was removed

try:
    EMAIL_DUE_SOON_DAYS = int(os.environ.get('EMAIL_DUE_SOON_DAYS', 7))
except ValueError:
    logger.warning("Invalid EMAIL_DUE_SOON_DAYS, using default 7.")
    EMAIL_DUE_SOON_DAYS = 7


# --- Log loaded settings in DEBUG mode ---
# THIS BLOCK IS NOW AT THE VERY END OF THE FILE
if DEBUG: # DEBUG is defined at the top of this file
    # Using the module-level 'logger' defined at the top of this file for consistency.
    logger.debug("--- Configuration Loaded (DEBUG Mode) ---")
    logger.debug(f"SECRET_KEY Set: {bool(SECRET_KEY and SECRET_KEY != 'change-this-in-production-to-a-strong-random-key!')}")
    logger.debug(f"DEBUG: {DEBUG}")
    logger.debug(f"TEST_MODE: {TEST_MODE}")
    logger.debug(f"BASE_DIR: {BASE_DIR}")
    logger.debug(f"SQLALCHEMY_DATABASE_URI: {SQLALCHEMY_DATABASE_URI}")  # CAUTION: May log credentials
    logger.debug(f"SMTP_SERVER: {SMTP_SERVER}:{SMTP_PORT}")
    logger.debug(f"FROM_EMAIL: {FROM_EMAIL}")
    logger.debug(f"HMAC_SECRET_KEY Set: {bool(HMAC_SECRET_KEY)}")
    logger.debug(f"UPLOAD_FOLDER: {UPLOAD_FOLDER}")
    logger.debug(f"DASHBOARD_URL: {DASHBOARD_URL}") # Now DASHBOARD_URL will be defined
    logger.debug(f"CHURN_HIGH_RISK_THRESHOLD: {CHURN_HIGH_RISK_THRESHOLD}")
    logger.debug(f"HEALTH_POOR_THRESHOLD: {HEALTH_POOR_THRESHOLD}") # Logs the final value
    logger.debug(f"EMAIL_DUE_SOON_DAYS: {EMAIL_DUE_SOON_DAYS}")
    # Log other important variables as needed
    logger.debug(f"PRIORITY_PACE_DECLINE_PCT_THRESHOLD: {PRIORITY_PACE_DECLINE_PCT_THRESHOLD}")
    logger.debug(f"GROWTH_PACE_INCREASE_PCT_THRESHOLD: {GROWTH_PACE_INCREASE_PCT_THRESHOLD}")
    logger.debug(f"GROWTH_HEALTH_THRESHOLD: {GROWTH_HEALTH_THRESHOLD}")
    logger.debug(f"GROWTH_MISSING_PRODUCTS_THRESHOLD: {GROWTH_MISSING_PRODUCTS_THRESHOLD}")
    logger.debug("---------------------------------------")