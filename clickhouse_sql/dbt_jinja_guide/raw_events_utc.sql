-- Таблица событий: user_id, user_event, timestamp (UTC)
CREATE TABLE raw_events_utc (
    user_id UInt32,
    user_event String,
    event_timestamp DateTime('UTC')
) ENGINE = MergeTree()
ORDER BY (user_id, event_timestamp);

-- Тестовые данные (15 строк, 4 юзера)
INSERT INTO raw_events_utc VALUES
    (1, 'click', '2025-01-04 10:00:00'),
    (1, 'view', '2025-01-04 10:01:00'),
    (1, 'add_to_cart', '2025-01-04 10:02:00'),
    (1, 'checkout', '2025-01-04 10:03:00'),
    (2, 'click', '2025-01-04 10:05:00'),
    (2, 'view', '2025-01-04 11:06:00'),
    (2, 'purchase', '2025-01-04 11:08:00'),
    (3, 'click', '2025-01-04 10:10:00'),
    (3, 'add_to_cart', '2025-01-04 11:11:00'),
    (3, 'checkout', '2025-01-04 11:12:00'),
    (4, 'click', '2025-01-04 10:15:00'),
    (4, 'view', '2025-01-04 10:16:00'),
    (4, 'view', '2025-01-04 10:17:00'),
    (4, 'purchase', '2025-01-04 10:20:00'),
    (4, 'click', '2025-01-04 10:22:00');
