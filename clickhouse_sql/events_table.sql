CREATE TABLE events (
    user_id UInt32,
    event String,
    event_timestamp DateTime
) ENGINE = MergeTree()
ORDER BY (event_timestamp);

-- Вставляем тестовые данные с различными типами событий
INSERT INTO events VALUES
(1, 'click', '2025-01-04 10:00:00'),
(1, 'view', '2025-01-04 10:01:00'),
(1, 'add_to_cart', '2025-01-04 10:02:00'),
(1, 'checkout', '2025-01-04 10:03:00'),
(2, 'click', '2025-01-04 10:05:00'),
(2, 'view', '2025-01-04 10:06:00'),
(2, 'click', '2025-01-04 10:07:00'),
(3, 'click', '2025-01-04 10:10:00'),
(3, 'add_to_cart', '2025-01-04 10:11:00'),
(3, 'checkout', '2025-01-04 10:12:00'),
(1, 'purchase', '2025-01-04 10:04:00'),
(2, 'purchase', '2025-01-04 10:08:00');
