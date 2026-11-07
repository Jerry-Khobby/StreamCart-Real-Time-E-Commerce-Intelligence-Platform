"""
The business and data quality rules, defined once.

Python code imports these. Rules that must also be written in SQL, YAML or JSON
(ksqlDB, dbt and the Great Expectations suites) are checked against this file by
tests/test_rules_consistency.py, so a change here that isn't made everywhere fails CI.
"""

# What a valid event looks like
MARKET_CURRENCIES = ["USD", "EUR", "JPY", "NGN", "BRL"]
ORDER_STATUSES = ["completed", "pending", "failed", "refunded"]
INVENTORY_UPDATE_TYPES = ["order_fulfillment", "restock", "damage_writeoff", "audit_adjustment"]
PAGE_TYPES = ["home", "category", "search", "product", "cart", "checkout", "order_confirmation", "account", "wishlist"]
MAX_AMOUNT_USD = 50_000

# When it must have arrived: no more than 5 minutes ahead of, or 7 days behind, its arrival
FUTURE_TOLERANCE_SECONDS = 300
MAX_EVENT_AGE_SECONDS = 7 * 24 * 3600

# Bot detection. Simulated humans take at least 5 seconds per page.
BOT_USER_AGENT_PATTERN = "(?i)bot|python-requests|curl|scrapy|spider|crawler"
BOT_MIN_EVENTS = 5
BOT_MAX_AVG_GAP_SECONDS = 3.0

# Session stitching
SESSION_GAP_SECONDS = 1800

# Fraud signals, shared by ksqlDB (real time) and dbt (agg_fraud_summary)
HIGH_VALUE_USD = 5000
NEW_SESSION_SECONDS = 60
VELOCITY_MAX_ORDERS_PER_HOUR = 10
CARD_TEST_MAX_AMOUNT_USD = 10
CARD_TEST_MIN_SMALL_ORDERS = 5
