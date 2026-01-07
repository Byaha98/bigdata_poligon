-- Извлекаем основные параметры из JSON

SELECT

event_id,

timestamp,

user_id,

JSONExtractString(event_properties, 'event_name') AS event_name,

JSONExtractString(event_properties, 'product_id') AS product_id,

JSONExtractString(user_profile, 'country') AS user_country,

JSONExtractString(session_data, 'source') AS traffic_source

FROM web_analytics

ORDER BY event_id;


-- Извлекаем разные типы: String, Int, Float, UInt8 (для Boolean)
SELECT
    event_id,
    JSONExtractString(event_properties, 'event_name') AS event_name,
    JSONExtract(event_properties, 'price', 'Float64') AS product_price,
    JSONExtract(event_properties, 'quantity', 'UInt32') AS quantity,
    JSONExtract(event_properties, 'discount_amount', 'Nullable(Float64)') AS discount,
    JSONExtract(user_profile, 'age', 'UInt32') AS user_age,
    JSONExtract(user_profile, 'premium', 'UInt8') AS is_premium  -- 1 = true, 0 = false
FROM web_analytics
WHERE JSONExtractString(event_properties, 'event_name') IN ('add_to_cart', 'checkout');


--JSONExtractKeysAndValuesRaw — Парсинг всех ключ-значений
SELECT
    event_id,
    user_id,
    JSONExtractKeysAndValuesRaw(user_profile) AS all_user_attributes
FROM web_analytics
LIMIT 3;


-- Извлекаем вложенные объекты (например, filters внутри event_properties)
SELECT
    event_id,
    JSONExtractString(event_properties, 'event_name') AS event_name,
    -- Извлекаем вложенный объект 'filters' как сырую строку
    JSONExtractRaw(event_properties, 'filters') AS filters_raw,
    -- Далее парсим его из RAW
    JSONExtractString(
        JSONExtractRaw(event_properties, 'filters'),
        'color'
    ) AS color_filter
FROM web_analytics
WHERE JSONExtractString(event_properties, 'event_name') = 'search';


-- Проверяем, есть ли определённые поля в JSON (используя JSONPath синтаксис)
SELECT
    event_id,
    JSONExtractString(event_properties, 'event_name') AS event_name,
    JSON_EXISTS(event_properties, '$.product_id') AS has_product_id,
    JSON_EXISTS(event_properties, '$.discount_amount') AS has_discount,
    JSON_EXISTS(event_properties, '$.error') AS has_error,
    JSON_EXISTS(user_profile, '$.premium') AS has_premium_flag
FROM web_analytics;

