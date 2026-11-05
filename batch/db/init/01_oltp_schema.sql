-- The simulated transactional database: the system of record the storefront
-- writes to. The simulator inserts orders and payments here; the oltp_daily_batch
-- DAG extracts them into Bronze.

CREATE SCHEMA IF NOT EXISTS oltp;

CREATE TABLE IF NOT EXISTS oltp.users (
    user_id     TEXT PRIMARY KEY,
    email       TEXT NOT NULL,
    created_at  TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS oltp.products (
    product_id  TEXT PRIMARY KEY,
    name        TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS oltp.orders (
    order_id    TEXT PRIMARY KEY,
    user_id     TEXT NOT NULL REFERENCES oltp.users (user_id),
    product_id  TEXT NOT NULL REFERENCES oltp.products (product_id),
    region      TEXT NOT NULL,
    quantity    INT NOT NULL,
    status      TEXT NOT NULL,
    created_at  TIMESTAMP NOT NULL
);
CREATE INDEX IF NOT EXISTS orders_created_at_idx ON oltp.orders (created_at);

CREATE TABLE IF NOT EXISTS oltp.payments (
    payment_id   TEXT PRIMARY KEY,
    order_id     TEXT NOT NULL REFERENCES oltp.orders (order_id),
    method       TEXT NOT NULL,
    currency     TEXT NOT NULL,
    amount_local DOUBLE PRECISION NOT NULL,
    created_at   TIMESTAMP NOT NULL
);
CREATE INDEX IF NOT EXISTS payments_created_at_idx ON oltp.payments (created_at);

-- The ID formats match producer/config.py, so the Silver referential-integrity
-- check has a real table of known users to compare against.
INSERT INTO oltp.users (user_id, email)
SELECT format('USER-%s', lpad(i::text, 7, '0')), format('user%s@example.com', i)
FROM generate_series(1, 10000) AS i
ON CONFLICT DO NOTHING;

INSERT INTO oltp.products (product_id, name)
SELECT format('PROD-%s', lpad(i::text, 5, '0')), format('Product %s', i)
FROM generate_series(1, 500) AS i
ON CONFLICT DO NOTHING;
