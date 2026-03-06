{{
    config(
        materialized='table'
    )
}}

{# Таблица переходов между событиями в пользовательских сессиях на основе events_sessions #}
{{
    build_event_transitions(
        relation=ref('events_sessions'),
        user_id_col='user_id',
        event_col='user_event'
    )
}}

