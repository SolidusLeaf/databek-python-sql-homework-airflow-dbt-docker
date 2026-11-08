CREATE SCHEMA IF NOT EXISTS source;

CREATE TABLE source.orders (
    order_id    INTEGER PRIMARY KEY,
    customer_id INTEGER NOT NULL,
    order_ts    TIMESTAMP NOT NULL,
    amount      NUMERIC(10, 2) NOT NULL,
    status      TEXT NOT NULL
);

INSERT INTO source.orders (order_id, customer_id, order_ts, amount, status)
SELECT
    g,
    (g % 25) + 1,
    CURRENT_DATE::timestamp
        - ((g % 60) + 1) * INTERVAL '1 day'
        + ((g * 37) % 1440) * INTERVAL '1 minute',
    ROUND(10 + ((g * 7.3) % 190), 2),
    CASE WHEN g % 7 = 0 THEN 'cancelled' ELSE 'completed' END
FROM generate_series(1, 600) AS g;