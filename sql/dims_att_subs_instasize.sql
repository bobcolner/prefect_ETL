create or replace table dimensions.att_subs_instasize
as
with projected_ltv as (
select
  product_type
  , count(1) as subs_total
  , avg(revenue_m12) as avg_revenue_m12
from dimensions.prodsubs_ios_instasize
where 
  first_revenue_at > '2018-01-01'
  and activation_at is not null
group by 1
), att_devices as (
select *
from dimensions.attribution_instasize
where 
  subscriber_id is not null
qualify 
  row_number() over(partition by subscriber_id order by created_at) = 1
), eligible_subs as (
select
  e.subscriber_id
  , e.product_type
  , case when e.product_type like '%year%' then 'year'
      when e.product_type like '%week%' then 'week'
      else 'month' end as product_duration
  , min(e.event_time) as subprod_start_at
  , min(if(e.event_name = 'trial_start', e.event_time, null)) as trial_start_at
  , min(if(e.event_name = 'trial_conversion', e.event_time, null)) as trial_conversion_at
  , min(if(e.event_name in ('purchase','recovered','reactivation','trial_conversion') and
      e.event_time < datetime_add(a.created_at, interval 1 month), e.event_time, null)) as first_activation_at
  , min(if(e.revenue_usd is not null, e.event_time, null)) as first_revenue_at
  , sum(e.revenue_usd) as revenue_usd
  , sum(if(e.event_time < datetime_add(a.created_at, interval 1 month), e.revenue_usd, 0)) as revenue_usd_m1
  , sum(if(e.event_name in ('canceled','cancel'), 1, 0)) as refund_requests
  , min(e.original_currency) as original_currency
  , min(a.device_vendor_id) as device_vendor_id
  , min(a.country) as country
  , min(a.installed_at) as installed_at
  , min(a.trial_started_at) as device_trial_started_at
  , min(a.paid_started_at) as device_paid_started_at
  , min(a.click_date) as click_date
  , min(a.created_at) as created_at
  , min(a.campaign_id) as campaign_id
  , min(a.adgroup_id) as adgroup_id
  , min(a.keyword_id) as keyword_id
from att_devices as a
join events.subscriptions_instasize as e
  using(subscriber_id)
where 
  e.event_time > a.created_at
group by 1, 2, 3
qualify 
  row_number() over(partition by subscriber_id, product_type order by subprod_start_at) = 1
)
select
  s.*
  , l.avg_revenue_m12
  , if(s.revenue_usd > 0, if(s.product_duration != 'year', l.avg_revenue_m12, s.revenue_usd), 0) as pltv_m12
from eligible_subs as s
left outer join projected_ltv as l
  using(product_type)
;