--groupArray — Агрегирование всех значений в массив

SELECT
    user_id,
    groupArray(event) AS event_sequence,
    groupArray(event_timestamp) AS timestamps
FROM events
GROUP BY user_id
ORDER BY user_id;


--ARRAY JOIN — Развёртывание массивов в отдельные строки

SELECT [1, 2, 3] AS nums ARRAY JOIN nums AS num;


WITH user_events AS (
    SELECT
        user_id,
        groupArray(event) AS events,
        arrayEnumerate(events) AS indices
    FROM
    (
        SELECT
            user_id,
            event
        FROM events
        ORDER BY user_id, event_timestamp
    )
    GROUP BY user_id
)
SELECT
    user_id,
    event,
    event_index
FROM user_events
ARRAY JOIN
    events  AS event,
    indices AS event_index
ORDER BY
    user_id,
    event_index;

-- Результат: каждое событие в отдельной строке с его индексом


-- arrayMap с lambda-функциями - Преобразование элементов массива, с двумя массивами
WITH user_events AS (
    SELECT
        user_id,
        groupArray(event) AS events,
        groupArray(toUnixTimestamp(event_timestamp)) AS timestamps
    FROM (
        SELECT
            user_id,
            event,
            event_timestamp
        FROM events
        ORDER BY user_id, event_timestamp
    )
    GROUP BY user_id
)

SELECT
    user_id,
    events AS original_events,
    arrayEnumerate(events) AS event_positions, -- индексы 1,2,3,...
    arrayMap(
        (ts, i) -> if(i = 1, 0, ts - timestamps[i-1]),
        timestamps,
        arrayEnumerate(timestamps)
    ) AS time_deltas
FROM user_events;

-- Лямбды к словарям с фильтрацией (arrayFilter)
-- Из всех событий выбираем только конверсионные (checkout, purchase)
WITH user_events AS (
    SELECT
        user_id,
        groupArray(event) AS events,
        groupArray(event_timestamp) AS timestamps
    FROM events
    GROUP BY user_id
)
SELECT
    user_id,
    events AS all_events,
    -- Фильтруем только события покупок
    arrayFilter(x -> (x = 'checkout' OR x = 'purchase'), events) AS conversion_events,
    -- Подсчитываем количество конверсионных событий
    length(arrayFilter(x -> (x = 'checkout' OR x = 'purchase'), events)) AS conversion_count
FROM user_events
ORDER BY user_id;



-- arrayReduce — Агрегация значений событий из JSON
-- Вычисляем сумму всех значений событий через arrayReduce
--Работа с массивами внутри JSON
WITH user_events AS (
    SELECT
        user_id,
        toFloat32(JSONExtractString(event_json, 'event_value')) AS values
    FROM user_journey
    ARRAY JOIN JSONExtractArrayRaw(events_json) AS event_json
)
SELECT
    user_id,
    groupArray(values) AS event_values,
    -- Сумма через arrayReduce
    arrayReduce('sum', groupArray(values)) AS total_value,
    -- Среднее через arrayReduce
    arrayReduce('avg', groupArray(values)) AS avg_value,
FROM user_events
GROUP BY user_id;


--  mapFromArrays для создания словарей 
-- Создаём словарь событие → количество для каждого пользователя
WITH event_counts AS (
    SELECT
        user_id,
        JSONExtractString(event_json, 'event_name') AS event,
        count() AS cnt
    FROM user_journey
    ARRAY JOIN JSONExtractArrayRaw(events_json) AS event_json
    GROUP BY user_id, event
)
SELECT
    user_id,
    -- Преобразуем результаты в Map (ключ: событие, значение: количество)
    mapFromArrays(
        groupArray(event),
        groupArray(cnt)
    ) AS event_map
FROM event_counts
GROUP BY user_id
ORDER BY user_id;

-- Результат: {click: 1, view: 1, purchase: 1} для user_id=1