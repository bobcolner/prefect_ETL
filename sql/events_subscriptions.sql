create or replace table events.subscriptions_instasize
partition by event_date
as
select
  event as event_name
  , datetime(received_at) as event_time
  , date(received_at) as event_date
  , updated_at as updated_at
  , product_type
  , case when product_type like '%year%' then 'year'
      when product_type like '%week%' then 'week'
      else 'month' end as product_duration
  , if(starts_with(product_type, 'com.munkeeapps.'), 'android', 'ios') as os
  , transaction_id
  , currency_code as original_currency
  , revenue_usd_cents / 100 as revenue_usd
  , original_transaction_id as subscriber_id
from analytics.api_premium_subscription_events
where 
  not starts_with(original_transaction_id, '1000000')
  and date(received_at) < '2020-10-01'

union all

select
  event as event_name
  , datetime(received_at) as event_time
  , date(received_at) as event_date
  , updated_at as updated_at
  , product_type
  , case when product_type like '%year%' then 'year'
      when product_type like '%week%' then 'week'
      else 'month' end as product_duration
  , if(starts_with(product_type, 'com.munkeeapps.'), 'android', 'ios') as os
  , transaction_id
  , currency_code as original_currency
  , revenue_usd_cents / 100 as revenue_usd
  , original_transaction_id as subscriber_id
from fastream.instasize_api_subscription_events_prod
where 
  not starts_with(original_transaction_id, '1000000')
  and date(received_at) >= '2020-10-01'
;

create or replace table events.subscriptions_selfiemade
partition by event_date
as
select
  event as event_name
  , datetime(received_at) as event_time
  , date(received_at) as event_date
  , updated_at as updated_at
  , product_type
  , case when product_type like '%year%' then 'year'
      when product_type like '%week%' then 'week'
      else 'month' end as product_duration
  , os
  , transaction_id
  , currency_code as original_currency
  , revenue_usd_cents / 100 as revenue_usd
  , original_transaction_id as subscriber_id
from fastream.selfiemade_api_subscription_events_prod
where not starts_with(original_transaction_id, '1000000')
;

create or replace table events.subscriptions_videomade
partition by event_date
as
select
  event as event_name
  , datetime(received_at) as event_time
  , date(received_at) as event_date
  , updated_at as updated_at
  , product_type
  , case when product_type like '%year%' then 'year'
      when product_type like '%week%' then 'week'
      else 'month' end as product_duration
  , os
  , transaction_id
  , currency_code as original_currency
  , revenue_usd_cents / 100 as revenue_usd
  , original_transaction_id as subscriber_id
from fastream.videomade_api_subscription_events_prod
where not starts_with(original_transaction_id, '1000000')
;