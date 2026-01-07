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