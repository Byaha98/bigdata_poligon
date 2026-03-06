{#
  Размечает пользовательские сессии и предыдущее/следующее событие в рамках сессии.
  Сессия = действия одного пользователя в пределах session_interval_seconds (по умолчанию 1 час).
  prev_event: если нет предыдущего — 'start'; next_event: если нет следующего — 'end'.

  CTE:
  - base: для каждой строки — время предыдущего события по тому же пользователю (_prev_ts).
  - with_session_id: флаг «начало новой сессии» (нет предыдущего или разрыв по времени > session_interval_seconds), затем session_id как накопительная сумма флагов.
  - with_prev_next: в рамках (user_id, session_id) — prev_event (lagInFrame, иначе 'start') и next_event (leadInFrame с кадром following, иначе 'end').
#}
{% macro sessionize_events(
    relation,
    user_id_col,
    event_col,
    timestamp_col,
    session_interval_seconds=3600
) %}
with
-- Для каждой строки — время предыдущего события того же пользователя (по порядку timestamp).
base as (
    select
        {{ user_id_col }},
        {{ event_col }},
        {{ timestamp_col }},
        lagInFrame({{ timestamp_col }}) over (partition by {{ user_id_col }} order by {{ timestamp_col }}) as _prev_ts
    from {{ relation }}
),
-- Флаг «новая сессия»: 1, если нет предыдущего события или разрыв по времени > session_interval_seconds; иначе 0.
-- session_id = накопительная сумма флагов по пользователю (каждая сессия получает свой номер).
with_session_id as (
    select
        {{ user_id_col }},
        {{ event_col }},
        {{ timestamp_col }},
        sum(
            if(_prev_ts is null or dateDiff('second', _prev_ts, {{ timestamp_col }}) > {{ session_interval_seconds }}, 1, 0)
        ) over (partition by {{ user_id_col }} order by {{ timestamp_col }} rows between unbounded preceding and current row) as session_id
    from base
),
-- В рамках (user_id, session_id) по порядку timestamp: prev_event и next_event; при отсутствии — 'start' и 'end' через coalesce.
with_prev_next as (
    select
        {{ user_id_col }},
        {{ event_col }},
        {{ timestamp_col }},
        session_id,
        -- prev_event: lagInFrame с offset=1 и default='start'
        lagInFrame({{ event_col }}, 1, 'start')
            over (partition by {{ user_id_col }}, session_id order by {{ timestamp_col }}) as prev_event,
        -- next_event: leadInFrame с offset=1 и default='end', кадр включает все последующие строки
        leadInFrame({{ event_col }}, 1, 'end')
            over (
                partition by {{ user_id_col }}, session_id
                order by {{ timestamp_col }}
                rows between current row and unbounded following
            ) as next_event
    from with_session_id
)
select * from with_prev_next
{% endmacro %}
