{#
  Строит таблицу переходов между событиями в рамках пользовательских сессий.
  Ожидает таблицу наподобие events_sessions:
    - user_id
    - session_id
    - user_event  (текущее событие)
    - prev_event  (предыдущее событие в сессии, либо 'start')
    - next_event  (следующее событие в сессии, либо 'end')

  Переходы считаем так:
    - prev_event -> user_event (включая 'start' -> первое событие)
    - user_event -> 'end' (для последнего события в сессии)

  Результат: строки = from_event, колонки = to_<event>, значения = количество переходов.
#}

{% macro build_event_transitions(
    relation,
    user_id_col,   
    event_col      
) %}
{# 1) Собираем список всех возможных целевых событий для динамических столбцов (все user_event + служебное end).
    Ожидаем, что входная таблица уже содержит session_id, prev_event, next_event с такими именами,
    т.к. её сгенерировала модель events_sessions/sessionize_events. #}
{# sql_events — это строка с SQL, которую мы потом передаём в run_query():
     - {{ event_col }} и {{ relation }} подставятся Jinjой;
     - ev — псевдоним колонки события, который дальше будем читать из результата run_query. #}
{% set sql_events %}
select distinct {{ event_col }} as ev from {{ relation }}
union all
select 'end' as ev
{% endset %}

{# 2) Выполняем запрос и превращаем результат в Python‑список events #}
{% set events_result = run_query(sql_events) %}
{% if events_result is none %}
  {{ exceptions.raise_compiler_error('build_event_transitions: не нашел событий в таблице sessionize_events') }}
{% endif %}

{% set events = [] %}
{% for row in events_result %}
  {# исключаем NULL и служебное 'start' из списка целевых событий #}
  {% if row[0] is not none and row[0] != 'start' %}
    {% do events.append(row[0]) %}
  {% endif %}
{% endfor %}

{# 3) Строим таблицу всех переходов from_event → to_event на основе сессионной таблицы:
   - prev_event → user_event (обычные шаги внутри сессии)
   - user_event → 'end' для последних событий (next_event = 'end')
   prev_event/next_event/session_id считаем фиксированными именами, т.к. их генерит предыдущая модель. #}
with transitions as (
    select
        prev_event as from_event,
        {{ event_col }} as to_event
    from {{ relation }}

    union all

    select
        {{ event_col }} as from_event,
        'end' as to_event
    from {{ relation }}
    where next_event = 'end'
)
{# 4) Широкая таблица переходов: каждая строка = from_event, каждый столбец to_<event> = число переходов туда #}
select
    from_event,
    {%- for ev in events %}
    countIf(to_event = '{{ ev }}') as to_{{ ev }}
        {%- if not loop.last %},{% endif %}
    {%- endfor %}
from transitions
group by from_event
order by from_event

{% endmacro %}

