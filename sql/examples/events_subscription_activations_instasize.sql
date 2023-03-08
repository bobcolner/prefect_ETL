create or replace table events.subscription_actiations_instasize
as
with lags as (
select 
  subscriber_id
  , event_name
  , event_time
  , revenue_usd
  , lag(event_time) over (partition by subscriber_id order by event_time asc) as last_event_time
  , row_number() over(partition by subscriber_id order by event_time) as payment_count
  , date_diff(event_time, lag(event_time) over (partition by subscriber_id order by event_time), day) as payment_date_diff
from events.subscriptions_instasize
where 
  revenue_usd is not null
  and os = 'ios'
  and event_time >= '2021-01-01'
), activations as (
select 
  *
  , if(payment_count = 1 or payment_date_diff > 60, 'activation', null) as sub_status
from lags
), activation_keys as (
select
  *
  , if(sub_status is not null, row_number() over(partition by subscriber_id order by event_time), null) as actiation_mark
from activations
), sub_actiation_ids as (
select 
  *
  , subscriber_id || '-' || actiation_mark as sub_activation_id_tmp
from activation_keys
)
select 
  *
  , last_value(sub_activation_id_tmp IGNORE NULLS) over(partition by subscriber_id order by event_time) as sub_activation_id
from sub_actiation_ids
