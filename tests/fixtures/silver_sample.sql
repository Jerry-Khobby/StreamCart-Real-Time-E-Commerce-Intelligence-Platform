-- A small, valid Silver dataset used by CI to prove that Gold actually builds from data.
-- The expected row counts are asserted in .github/workflows/ci.yml.

INSERT INTO silver.transactions (
    transaction_id, user_id, product_id, category, region, ip_region, currency, amount_local, amount_usd,
    amount_usd_normalised, fx_rate_used, quantity, payment_method, status, session_age_seconds, event_time,
    fx_discrepancy
) VALUES
    ('t1', 'USER-0000001', 'PROD-00001', 'Books', 'US', 'US', 'USD', 20.0, 20.0, 20.0, 1.0, 1, 'paypal',
     'completed', 120.0, now() - interval '2 days', false),
    ('t2', 'USER-0000001', 'PROD-00002', 'Toys', 'US', 'US', 'USD', 6000.0, 6000.0, 6000.0, 1.0, 2, 'credit_card',
     'completed', 30.0, now() - interval '1 day', false),
    ('t3', 'USER-0000002', 'PROD-00001', 'Books', 'EU', 'US', 'EUR', 10.0, 10.9, 10.9, 1.09, 1, 'debit_card',
     'pending', 200.0, now() - interval '1 day', false),
    -- Failed orders are excluded from Gold.
    ('t4', 'USER-0000002', 'PROD-00002', 'Toys', 'EU', 'EU', 'EUR', 50.0, 54.5, 54.5, 1.09, 1, 'paypal',
     'failed', 200.0, now() - interval '1 day', false);

INSERT INTO silver.clickstream (
    event_id, session_id, session_id_silver, user_id, region, page_type, product_id, search_query, referrer,
    user_agent, event_time
) VALUES
    ('c1', 's1', 'stitched-1', 'USER-0000001', 'US', 'home', NULL, NULL, 'direct', 'Mozilla/5.0',
     now() - interval '1 day' - interval '5 minutes'),
    ('c2', 's1', 'stitched-1', 'USER-0000001', 'US', 'product', 'PROD-00001', NULL, NULL, 'Mozilla/5.0',
     now() - interval '1 day' - interval '4 minutes'),
    ('c3', 's1', 'stitched-1', 'USER-0000001', 'US', 'cart', NULL, NULL, NULL, 'Mozilla/5.0',
     now() - interval '1 day' - interval '3 minutes'),
    ('c4', 's2', 'stitched-2', 'USER-0000002', 'EU', 'home', NULL, NULL, 'direct', 'Mozilla/5.0',
     now() - interval '1 day');

INSERT INTO silver.inventory (
    event_id, product_id, warehouse_id, region, update_type, quantity_delta, stock_after, reorder_point, event_time
) VALUES
    ('i1', 'PROD-00001', 'WH-US-EAST', 'US', 'restock', 100, 150, 20, now() - interval '2 days'),
    ('i2', 'PROD-00001', 'WH-US-EAST', 'US', 'order_fulfillment', -1, 149, 20, now() - interval '2 days'),
    ('i3', 'PROD-00001', 'WH-US-EAST', 'US', 'order_fulfillment', -2, 147, 20, now() - interval '1 day');
