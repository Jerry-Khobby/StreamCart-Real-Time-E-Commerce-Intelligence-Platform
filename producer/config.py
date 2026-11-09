import random
import os

REGION_WEIGHTS = {
    "US":     0.35,
    "EU":     0.25,
    "Asia":   0.22,
    "Africa": 0.08,
    "LatAm":  0.10,
}
REGIONS = ["US", "EU", "Asia", "Africa", "LatAm"]

# Each region has its own Kafka partition, for parallel per-market consumption.
# (Hashing the key could map two of the five regions to one partition.)
REGION_PARTITIONS = {region: index for index, region in enumerate(REGIONS)}

CURRENCIES = {
    "US":     "USD",
    "EU":     "EUR",
    "Asia":   "JPY",
    "Africa": "NGN",
    "LatAm":  "BRL",
}

FX_TO_USD = {
    "USD": 1.00,
    "EUR": 1.09,
    "JPY": 0.0067,
    "NGN": 0.00064,
    "BRL": 0.20,
}

# Monday to Sunday: demand dips at the weekend
DAY_OF_WEEK_WEIGHTS = [1.0, 1.0, 1.0, 1.0, 1.05, 0.75, 0.7]

# Warehouse → the market it serves
WAREHOUSES = {
    "WH-US-EAST": "US",
    "WH-US-WEST": "US",
    "WH-EU-CENTRAL": "EU",
    "WH-ASIA-SING": "Asia",
    "WH-AF-LAGOS": "Africa",
    "WH-LATAM-SP": "LatAm",
}
WAREHOUSES_BY_REGION = {
    region: [wh for wh, wh_region in WAREHOUSES.items() if wh_region == region]
    for region in REGIONS
}

PRODUCT_CATEGORIES = [
    "Electronics",
    "Clothing",
    "Home & Garden",
    "Sports",
    "Beauty",
    "Books",
    "Toys",
    "Automotive",
    "Food & Grocery",
    "Jewellery",
]


SEASONAL_WEIGHTS = {
    "Electronics":    [0.9, 0.8, 0.9, 0.9, 0.9, 0.9, 0.9, 0.9, 1.0, 1.1, 1.4, 1.5],
    "Clothing":       [0.9, 0.9, 1.1, 1.2, 1.2, 1.0, 0.9, 0.9, 1.0, 1.1, 1.2, 1.2],
    "Home & Garden":  [0.7, 0.7, 1.0, 1.3, 1.4, 1.3, 1.2, 1.1, 1.0, 0.9, 0.8, 0.8],
    "Sports":         [0.8, 0.8, 1.0, 1.2, 1.4, 1.4, 1.4, 1.2, 1.0, 0.9, 0.8, 0.8],
    "Beauty":         [1.0, 1.0, 1.1, 1.1, 1.2, 1.1, 1.0, 1.0, 1.0, 1.0, 1.1, 1.2],
    "Books":          [1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.1, 1.1, 1.2, 1.0, 1.1, 1.1],
    "Toys":           [0.8, 0.8, 0.8, 0.9, 0.9, 0.9, 0.9, 0.9, 1.0, 1.1, 1.4, 1.8],
    "Automotive":     [0.9, 0.9, 1.0, 1.1, 1.2, 1.1, 1.1, 1.0, 1.0, 1.0, 0.9, 0.9],
    "Food & Grocery": [1.0, 1.0, 1.0, 1.0, 1.1, 1.1, 1.1, 1.1, 1.0, 1.0, 1.1, 1.2],
    "Jewellery":      [1.0, 1.2, 1.0, 1.0, 1.1, 1.0, 0.9, 0.9, 0.9, 0.9, 1.1, 1.4],
}

PRICE_RANGES = {
    "Electronics":    (30,   2500),
    "Clothing":       (10,   300),
    "Home & Garden":  (5,    800),
    "Sports":         (10,   600),
    "Beauty":         (5,    200),
    "Books":          (5,    80),
    "Toys":           (5,    250),
    "Automotive":     (15,   1000),
    "Food & Grocery": (2,    150),
    "Jewellery":      (20,   5000),
}


PAGE_TYPES = [
    "home",
    "category",
    "search",
    "product",
    "cart",
    "checkout",
    "order_confirmation",
    "account",
    "wishlist",
]

PAGE_FUNNEL_WEIGHTS = {
    "home":               {"category": 0.4, "search": 0.3, "product": 0.2, "account": 0.1},
    "category":           {"product": 0.6, "search": 0.2, "category": 0.1, "home": 0.1},
    "search":             {"product": 0.6, "category": 0.2, "search": 0.1, "home": 0.1},
    "product":            {"cart": 0.3, "product": 0.3, "category": 0.2, "wishlist": 0.1, "home": 0.1},
    "cart":               {"checkout": 0.5, "product": 0.2, "home": 0.2, "cart": 0.1},
    "checkout":           {"order_confirmation": 0.7, "cart": 0.2, "home": 0.1},
    "order_confirmation": {"home": 0.7, "account": 0.3},
    "account":            {"home": 0.5, "category": 0.3, "wishlist": 0.2},
    "wishlist":           {"product": 0.5, "cart": 0.3, "home": 0.2},
}

# Known products (small catalogue; in production this comes from PostgreSQL)
NUM_PRODUCTS = 500
NUM_USERS    = 10_000


# Pre-generate stable IDs so referential integrity holds across events
PRODUCT_IDS = [f"PROD-{i:05d}" for i in range(1, NUM_PRODUCTS + 1)]
USER_IDS    = [f"USER-{i:07d}" for i in range(1, NUM_USERS + 1)]


# A private generator keeps these stable between runs without reseeding the global
# one, which would make every restart replay the same stream of events.
_stable_random = random.Random(42)

# Each customer belongs to one market, so a mismatch with ip_region means something.
USER_HOME_REGION = dict(zip(
    USER_IDS,
    _stable_random.choices(REGIONS, weights=[REGION_WEIGHTS[r] for r in REGIONS], k=NUM_USERS),
))

# The accounts that fraud scenarios strike
FRAUD_USER_POOL = _stable_random.sample(USER_IDS, min(50, NUM_USERS))

_SEARCH_TERMS = [
    "laptop", "running shoes", "coffee maker", "bluetooth headphones",
    "yoga mat", "winter jacket", "gaming chair", "air fryer", "novel",
    "skincare", "watch", "backpack", "phone case", "desk lamp", "sneakers",
]


_REFERRERS = [
    "https://google.com", "https://facebook.com", "https://instagram.com",
    "https://twitter.com", "direct", "https://email.streamcart.com",
    "https://affiliate.partner.com",
]


_BOT_UAS = [
    "Mozilla/5.0 (compatible; Googlebot/2.1)",
    "python-requests/2.28",
    "curl/7.68.0",
    "AhrefsBot/7.0",
    "scrapy/2.7",
]

_HUMAN_UAS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X) AppleWebKit/605.1.15",
    "Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36",
    "Mozilla/5.0 (iPad; CPU OS 16_0 like Mac OS X) AppleWebKit/605.1.15",
]



LOG_DIR = os.getenv("LOG_DIR", "/opt/airflow/logs")
LOG_FILE = os.getenv("LOG_FILE", "streamcart.log")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")


def kafka_auth() -> dict:
    """
    Login settings for Kafka clients (librdkafka names). Each service has its own user,
    set by KAFKA_SASL_USERNAME and KAFKA_SASL_PASSWORD. Without them, plaintext, as in tests.
    """
    import os

    user = os.getenv("KAFKA_SASL_USERNAME")
    if not user:
        return {}
    return {
        "security.protocol": "SASL_PLAINTEXT",
        "sasl.mechanism": "PLAIN",
        "sasl.username": user,
        "sasl.password": os.environ["KAFKA_SASL_PASSWORD"],
    }


def spark_kafka_auth() -> dict:
    """The same login, as options for Spark's Kafka source."""
    auth = kafka_auth()
    if not auth:
        return {}
    jaas = ('org.apache.kafka.common.security.plain.PlainLoginModule required '
            f'username="{auth["sasl.username"]}" password="{auth["sasl.password"]}";')
    return {"kafka.security.protocol": "SASL_PLAINTEXT", "kafka.sasl.mechanism": "PLAIN", "kafka.sasl.jaas.config": jaas}
