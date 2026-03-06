{{
    config(
        materialized='table'
    )
}}

{{
    sessionize_events(
        relation=source('raw', 'raw_events_utc'),
        user_id_col='user_id',
        event_col='user_event',
        timestamp_col='event_timestamp'
    )
}}
