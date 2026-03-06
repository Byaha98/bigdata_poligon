CREATE TABLE raw_table_utc(
    order_id UInt64,
    created_at DateTime('UTC')
) ENGINE = MergeTree()
ORDER BY order_id;

-- Тестовые данные (5 строк)
INSERT INTO raw_table_utc VALUES
    (1, '2025-01-01 10:00:00'),
    (2, '2025-01-01 10:05:00'),
    (3, '2025-01-01 10:10:00'),
    (4, '2025-01-01 10:15:00'),
    (5, '2025-01-01 10:20:00');