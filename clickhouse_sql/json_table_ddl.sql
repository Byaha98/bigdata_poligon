-- Таблица с JSON-логами веб-события (типа как в Amplitude)

CREATE TABLE web_analytics (

event_id UInt64,

timestamp DateTime,

user_id UInt32,

session_data String, -- JSON с данными о сессии

event_properties String, -- JSON с параметрами события

user_profile String -- JSON с профилем пользователя

) ENGINE = MergeTree()

ORDER BY (timestamp);

  

-- Вставляем тестовые JSON-данные

INSERT INTO web_analytics FORMAT Values

(

1,

'2025-01-04 10:00:00',

101,

'{"session_id":"s_001","source":"organic","campaign":"","device":"desktop","os":"Windows","browser":"Chrome"}',

'{"event_name":"page_view","page":"/products","category":"electronics","load_time_ms":245,"user_action":"scroll"}',

'{"name":"Alice","country":"US","age":28,"premium":true,"ltv":1250.50,"tags":["vip","high_value","loyal"]}'

),

(

2,

'2025-01-04 10:05:00',

101,

'{"session_id":"s_001","source":"organic","campaign":"","device":"desktop","os":"Windows","browser":"Chrome"}',

'{"event_name":"product_click","product_id":"SKU_789","price":899.99,"currency":"USD","position_on_page":3,"click_time_ms":145}',

'{"name":"Alice","country":"US","age":28,"premium":true,"ltv":1250.50,"tags":["vip","high_value","loyal"]}'

),

(

3,

'2025-01-04 10:10:00',

101,

'{"session_id":"s_001","source":"organic","campaign":"","device":"desktop","os":"Windows","browser":"Chrome"}',

'{"event_name":"add_to_cart","product_id":"SKU_789","quantity":1,"price":899.99,"discount":null,"estimated_shipping":19.99}',

'{"name":"Alice","country":"US","age":28,"premium":true,"ltv":1250.50,"tags":["vip","high_value","loyal"]}'

),

(

4,

'2025-01-04 10:15:00',

101,

'{"session_id":"s_001","source":"organic","campaign":"","device":"desktop","os":"Windows","browser":"Chrome"}',

'{"event_name":"checkout","cart_value":899.99,"items_count":1,"coupon_code":"SAVE20","discount_amount":180.00,"final_price":719.99,"payment_method":"credit_card"}',

'{"name":"Alice","country":"US","age":28,"premium":true,"ltv":1250.50,"tags":["vip","high_value","loyal"]}'

),

(

5,

'2025-01-04 10:20:00',

102,

'{"session_id":"s_002","source":"paid_search","campaign":"winter_sale","device":"mobile","os":"iOS","browser":"Safari"}',

'{"event_name":"page_view","page":"/sale","category":"clothing","load_time_ms":512,"user_action":"view"}',

'{"name":"Bob","country":"UK","age":35,"premium":false,"ltv":245.30,"tags":["new_user","price_sensitive"]}'

),

(

6,

'2025-01-04 10:25:00',

102,

'{"session_id":"s_002","source":"paid_search","campaign":"winter_sale","device":"mobile","os":"iOS","browser":"Safari"}',

'{"event_name":"product_view","product_id":"SKU_456","price":49.99,"category":"clothing","reviews_count":234,"rating":4.5}',

'{"name":"Bob","country":"UK","age":35,"premium":false,"ltv":245.30,"tags":["new_user","price_sensitive"]}'

),

(

7,

'2025-01-04 10:30:00',

102,

'{"session_id":"s_002","source":"paid_search","campaign":"winter_sale","device":"mobile","os":"iOS","browser":"Safari"}',

'{"event_name":"page_view","page":"/checkout","load_time_ms":450,"steps_completed":1,"total_steps":3,"error":null}',

'{"name":"Bob","country":"UK","age":35,"premium":false,"ltv":245.30,"tags":["new_user","price_sensitive"]}'

),

(

8,

'2025-01-04 10:35:00',

103,

'{"session_id":"s_003","source":"direct","campaign":"","device":"desktop","os":"Mac","browser":"Firefox"}',

'{"event_name":"search","query":"winter jackets","results_count":42,"filters":{"color":"black","size":"M"},"sort_by":"price_asc"}',

'{"name":"Carol","country":"CA","age":42,"premium":true,"ltv":3500.75,"tags":["vip","long_term","high_engagement"]}'

);