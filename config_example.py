"""
Volatility Scanner Configuration
================================
Copy this file to config.py and fill in your credentials:
    cp config_example.py config.py
"""

import os

# =============================================================================
# DATABASE
# =============================================================================
DATABASE_PATH = os.path.join(os.path.dirname(__file__), 'volatility_data.db')

# =============================================================================
# iVOLATILITY API
# =============================================================================
IVOLATILITY_API_KEY = os.environ.get('IVOLATILITY_API_KEY', 'your_api_key_here')
IVOLATILITY_BASE_URL = 'https://restapi.ivolatility.com/v1'

# =============================================================================
# EMAIL SETTINGS
# =============================================================================
SMTP_SERVER = 'smtp.gmail.com'
SMTP_PORT = 587
EMAIL_SENDER = 'your_email@gmail.com'
EMAIL_PASSWORD = os.environ.get('EMAIL_APP_PASSWORD', 'your_app_password_here')
EMAIL_RECIPIENT = 'your_email@gmail.com'

# =============================================================================
# EIA (Energy Information Administration) API
# =============================================================================
EIA_API_KEY = os.environ.get('EIA_API_KEY', 'your_eia_key_here')

# =============================================================================
# UNIVERSE - What to Track
# =============================================================================

WATCHLIST_EXTRAS = [
    'MSTR', 'COIN', 'HOOD', 'PLTR', 'RIVN', 'LCID', 'NIO', 'GME', 'AMC',
    'MARA', 'RIOT', 'BITF', 'HUT', 'CLSK',
]

VOLATILITY_PRODUCTS = [
    'VIX', 'VIX9D', 'VIX3M', 'VIX6M', 'VXN', 'RVX', 'GVZ', 'OVX', 'VXUS',
]

COMMODITY_ETFS = [
    'GLD', 'SLV', 'USO', 'UNG', 'DBA', 'CORN', 'WEAT', 'SOYB', 'CPER', 'PPLT', 'PALL',
]

SECTOR_ETFS = [
    'XLF', 'XLE', 'XLK', 'XLV', 'XLI', 'XLP', 'XLY', 'XLU', 'XLB', 'XLRE',
]

# =============================================================================
# ANALYSIS THRESHOLDS
# =============================================================================

IV_RANK_HIGH = 80
IV_RANK_LOW = 20
IV_RANK_EXTREME_HIGH = 90
IV_RANK_EXTREME_LOW = 10

IVHV_OVERPRICED = 1.3
IVHV_UNDERPRICED = 0.8

SKEW_HIGH = 5.0
SKEW_LOW = -2.0

CONTANGO_THRESHOLD = 5.0
BACKWARDATION_THRESHOLD = -3.0

# =============================================================================
# DATA COLLECTION
# =============================================================================

HV_PERIODS = [10, 20, 30, 60, 90]
IV_PERIODS = [30, 60, 90]
INITIAL_HISTORY_DAYS = 365 * 2
IV_RANK_LOOKBACK = 252
