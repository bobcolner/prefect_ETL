create or replace table dimensions.experiments_instasize
as
with start as (
select
  device_vendor_id
  , json_extract_scalar(json_meta, '$.experimentName') as experiment_name
  , json_extract_scalar(json_meta, '$.variantName') as variant_name
  , min(subscriber_id) as subscriber_id
  , min(event_time) as exp_start_at
from fastream.instasize_ios_prod
where
  event_date >= '2021-01-01'
  and event_date <= current_datetime()
  and device_vendor_id is not null
  and event_name = 'apptimize_participated'
group by 1, 2, 3
having experiment_name is not null
)
select
  s.device_vendor_id
  , s.experiment_name
  , s.variant_name
  , min(s.exp_start_at) as exp_start_at
  , if(date(min(d.installed_date)) = date(min(s.exp_start_at)), 'new install', 'existing device') as participant_segment
  , min(d.country) as country
  , min(s.subscriber_id) as subscriber_id
  , max(if(d.trial_started_at > s.exp_start_at, 1, 0)) as post_exp_trial_start
  , max(if(d.paid_started_at > s.exp_start_at, 1, 0)) as post_exp_paid_start
  , count(distinct if(e.event_time > s.exp_start_at, date(event_date), null)) as active_days_total
  , count(distinct if(e.event_time > s.exp_start_at and e.event_time <= datetime_add(s.exp_start_at, interval 30 day), date(event_date), null)) as active_days_d30
  , sum(if(e.event_time > s.exp_start_at and event_name in ('edit_asset', 'share_action'), 1, 0)) as edit_and_share_total
  , sum(if(e.event_time > s.exp_start_at and event_name = 'premium_purchase_success', 1, 0)) as premium_purchase_success_total
from start as s
join fastream.instasize_ios_prod as e
  on s.device_vendor_id = e.device_vendor_id
left outer join dimensions.devices_instasize as d
  on s.device_vendor_id = d.device_vendor_id
where 
  e.device_vendor_id is not null
  and e.event_date >= '2021-01-01'
  and e.event_date <= current_datetime()
group by 1, 2, 3
;