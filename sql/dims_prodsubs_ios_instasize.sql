create or replace table dimensions.prodsubs_ios_instasize
as
with subs as (
select
  subscriber_id
  , product_type
  , min(product_duration) as product_duration
  , min(original_currency) as original_currency
  , min(event_time) as product_start_at
  , count(1) as event_count
  , min(if(event_name = 'trial_start', event_time, null)) as trial_start_at
  , min(if(event_name = 'trial_conversion', event_time, null)) as trial_conversion_at
  , min(if(revenue_usd is not null, event_time, null)) as first_revenue_at
  , min(if(event_name in ('purchase','recovered','reactivation','trial_conversion'), event_time, null)) as activation_at
  , min(if(event_name in ('purchase','recovered','reactivation','trial_conversion','crossgrade'), event_time, null)) as new_activation_at
  , min(if(event_name = 'crossgrade', event_time, null)) as crossgrade_at
  , min(if(event_name = 'cancel', event_time, null)) as cancel_at
  , min(if(event_name = 'reactivation', event_time, null)) as reactivation_at
from events.subscriptions_instasize
where os = 'ios'
group by 1, 2
), sub_metrics as (
select
  s.subscriber_id
  , s.product_type
  , min(s.product_duration) as product_duration
  , row_number() over(partition by s.subscriber_id order by min(s.product_start_at)) as product_order
  , min(s.original_currency) as original_currency
  , min(s.product_start_at) as product_start_at
  , min(s.event_count) as event_count
  , min(s.trial_start_at) as trial_start_at
  , min(s.trial_conversion_at) as trial_conversion_at
  , min(s.first_revenue_at) as first_revenue_at
  , min(s.activation_at) as activation_at
  , min(s.new_activation_at) as new_activation_at
  , min(s.crossgrade_at) as crossgrade_at
  , min(s.cancel_at) as cancel_at
  , min(s.reactivation_at) as reactivation_at
  , sum(if(e.event_name in ('canceled','cancel'), 1, 0)) as refund_requests
  , ifnull(sum(e.revenue_usd), 0) as revenue_usd_total
  -- cumulative revenue
  , sum(if(e.event_time <= datetime_add(s.first_revenue_at, interval 1 month), e.revenue_usd, 0)) as revenue_m1
  , sum(if(e.event_time <= datetime_add(s.first_revenue_at, interval 2 month), e.revenue_usd, 0)) as revenue_m2
  , sum(if(e.event_time <= datetime_add(s.first_revenue_at, interval 3 month), e.revenue_usd, 0)) as revenue_m3
  , sum(if(e.event_time <= datetime_add(s.first_revenue_at, interval 4 month), e.revenue_usd, 0)) as revenue_m4
  , sum(if(e.event_time <= datetime_add(s.first_revenue_at, interval 5 month), e.revenue_usd, 0)) as revenue_m5
  , sum(if(e.event_time <= datetime_add(s.first_revenue_at, interval 6 month), e.revenue_usd, 0)) as revenue_m6
  , sum(if(e.event_time <= datetime_add(s.first_revenue_at, interval 7 month), e.revenue_usd, 0)) as revenue_m7
  , sum(if(e.event_time <= datetime_add(s.first_revenue_at, interval 8 month), e.revenue_usd, 0)) as revenue_m8
  , sum(if(e.event_time <= datetime_add(s.first_revenue_at, interval 9 month), e.revenue_usd, 0)) as revenue_m9
  , sum(if(e.event_time <= datetime_add(s.first_revenue_at, interval 10 month), e.revenue_usd, 0)) as revenue_m10
  , sum(if(e.event_time <= datetime_add(s.first_revenue_at, interval 11 month), e.revenue_usd, 0)) as revenue_m11
  , sum(if(e.event_time <= datetime_add(s.first_revenue_at, interval 12 month), e.revenue_usd, 0)) as revenue_m12
  , sum(if(e.event_time <= datetime_add(s.first_revenue_at, interval 13 month), e.revenue_usd, 0)) as revenue_m13
  , sum(if(e.event_time <= datetime_add(s.first_revenue_at, interval 14 month), e.revenue_usd, 0)) as revenue_m14
  , sum(if(e.event_time <= datetime_add(s.first_revenue_at, interval 15 month), e.revenue_usd, 0)) as revenue_m15
  , sum(if(e.event_time <= datetime_add(s.first_revenue_at, interval 16 month), e.revenue_usd, 0)) as revenue_m16
  , sum(if(e.event_time <= datetime_add(s.first_revenue_at, interval 17 month), e.revenue_usd, 0)) as revenue_m17
  , sum(if(e.event_time <= datetime_add(s.first_revenue_at, interval 18 month), e.revenue_usd, 0)) as revenue_m18
  , sum(if(e.event_time <= datetime_add(s.first_revenue_at, interval 19 month), e.revenue_usd, 0)) as revenue_m19
  , sum(if(e.event_time <= datetime_add(s.first_revenue_at, interval 20 month), e.revenue_usd, 0)) as revenue_m20
  , sum(if(e.event_time <= datetime_add(s.first_revenue_at, interval 21 month), e.revenue_usd, 0)) as revenue_m21
  , sum(if(e.event_time <= datetime_add(s.first_revenue_at, interval 22 month), e.revenue_usd, 0)) as revenue_m22
  , sum(if(e.event_time <= datetime_add(s.first_revenue_at, interval 23 month), e.revenue_usd, 0)) as revenue_m23
  , sum(if(e.event_time <= datetime_add(s.first_revenue_at, interval 24 month), e.revenue_usd, 0)) as revenue_m24
from subs as s
join events.subscriptions_instasize as e
  on(s.subscriber_id = e.subscriber_id 
    and s.product_type = e.product_type)
where e.os = 'ios'  
group by 1, 2
)
select
  subscriber_id
  , product_type
  , product_duration
  , product_order
  , original_currency
  , product_start_at
  , event_count
  , trial_start_at
  , trial_conversion_at
  , first_revenue_at
  , activation_at
  , new_activation_at
  , crossgrade_at
  , cancel_at
  , reactivation_at
  , refund_requests
  , revenue_usd_total
  -- cum revenue
  , revenue_m1
  , if(datetime_add(first_revenue_at, interval 2 month) <= current_datetime(), revenue_m2, null) as revenue_m2
  , if(datetime_add(first_revenue_at, interval 3 month) <= current_datetime(), revenue_m3, null) as revenue_m3
  , if(datetime_add(first_revenue_at, interval 4 month) <= current_datetime(), revenue_m4, null) as revenue_m4
  , if(datetime_add(first_revenue_at, interval 5 month) <= current_datetime(), revenue_m5, null) as revenue_m5
  , if(datetime_add(first_revenue_at, interval 6 month) <= current_datetime(), revenue_m6, null) as revenue_m6
  , if(datetime_add(first_revenue_at, interval 7 month) <= current_datetime(), revenue_m7, null) as revenue_m7
  , if(datetime_add(first_revenue_at, interval 8 month) <= current_datetime(), revenue_m8, null) as revenue_m8
  , if(datetime_add(first_revenue_at, interval 9 month) <= current_datetime(), revenue_m9, null) as revenue_m9
  , if(datetime_add(first_revenue_at, interval 10 month) <= current_datetime(), revenue_m10, null) as revenue_m10
  , if(datetime_add(first_revenue_at, interval 11 month) <= current_datetime(), revenue_m11, null) as revenue_m11
  , if(datetime_add(first_revenue_at, interval 12 month) <= current_datetime(), revenue_m12, null) as revenue_m12
  , if(datetime_add(first_revenue_at, interval 13 month) <= current_datetime(), revenue_m12, null) as revenue_m13
  , if(datetime_add(first_revenue_at, interval 14 month) <= current_datetime(), revenue_m12, null) as revenue_m14
  , if(datetime_add(first_revenue_at, interval 15 month) <= current_datetime(), revenue_m12, null) as revenue_m15
  , if(datetime_add(first_revenue_at, interval 16 month) <= current_datetime(), revenue_m12, null) as revenue_m16
  , if(datetime_add(first_revenue_at, interval 17 month) <= current_datetime(), revenue_m12, null) as revenue_m17
  , if(datetime_add(first_revenue_at, interval 18 month) <= current_datetime(), revenue_m12, null) as revenue_m18
  , if(datetime_add(first_revenue_at, interval 19 month) <= current_datetime(), revenue_m12, null) as revenue_m19
  , if(datetime_add(first_revenue_at, interval 20 month) <= current_datetime(), revenue_m12, null) as revenue_m20
  , if(datetime_add(first_revenue_at, interval 21 month) <= current_datetime(), revenue_m12, null) as revenue_m21
  , if(datetime_add(first_revenue_at, interval 22 month) <= current_datetime(), revenue_m12, null) as revenue_m22
  , if(datetime_add(first_revenue_at, interval 23 month) <= current_datetime(), revenue_m12, null) as revenue_m23
  , if(datetime_add(first_revenue_at, interval 24 month) <= current_datetime(), revenue_m12, null) as revenue_m24
from sub_metrics
