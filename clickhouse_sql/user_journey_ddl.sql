-- Таблица user_journey с JSON структурой для хранения событий пользователя
CREATE TABLE user_journey (
    user_id UInt32,
    events_json String  -- JSON массив событий: [{"event_name": "click", "event_ts": "2025-01-04 10:00:00", "event_value": 0.5}, ...]
) ENGINE = MergeTree()
ORDER BY user_id;

-- Вставляем тестовые данные с JSON событиями
INSERT INTO user_journey VALUES
(1, '[{"event_name":"click","event_ts":"2025-01-04 10:00:00","event_value":0.5},{"event_name":"view","event_ts":"2025-01-04 10:01:00","event_value":1.0},{"event_name":"purchase","event_ts":"2025-01-04 10:04:00","event_value":99.99}]'),
(2, '[{"event_name":"click","event_ts":"2025-01-04 10:05:00","event_value":0.5},{"event_name":"click","event_ts":"2025-01-04 10:07:00","event_value":0.5},{"event_name":"purchase","event_ts":"2025-01-04 10:08:00","event_value":49.99}]'),
(3, '[{"event_name":"click","event_ts":"2025-01-04 10:10:00","event_value":0.5},{"event_name":"add_to_cart","event_ts":"2025-01-04 10:11:00","event_value":5.0}]');
