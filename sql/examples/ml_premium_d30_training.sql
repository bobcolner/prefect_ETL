-- emerald-skill-201716 ml premium_d30_training
create or replace table ml.premium_d30_training__20200622
as
with daily_counts as (
select
  i.device_vendor_id
  , e.event_date
  , i.installed_date
  , date_diff(e.event_date, i.installed_date, day) as device_age
  , max(e.language) as language
  , max(e.model) as model
  , count(1) as events_count
  , sum(case when e.premium_status = 'trial' then 1 else 0 end) as trial_event_count
  , sum(case when e.premium_status = 'active' then 1 else 0 end) as premium_event_count
  , sum(case when event_name = 'hit_paywall' then 1 else 0 end) as hit_paywall_count
  , sum(case when event_name = 'edit_asset' then 1 else 0 end) as edit_asset_count
  , sum(case when event_name = 'editor_done_taps' then 1 else 0 end) as editor_done_taps_count
  , sum(case when event_name = 'share_action' then 1 else 0 end) as share_action_count
from dimensions.devices_instasize_installed as i
inner join fastream.instasize_ios_prod as e
  on(e.device_vendor_id = i.device_vendor_id)
where
  e.device_vendor_id is not null
  and e.event_date >= '2020-03-01'
  and e.event_date <= current_date()
  and date_diff(e.event_date, i.installed_date, day) >= 0
  and date_diff(e.event_date, i.installed_date, day) <= 30
  and i.installed_date >= '2020-03-01'
  and i.installed_date < date_sub(current_date(), interval 30 day)
group by 1, 2, 3, 4
), daily_cumsum as (
select
  c.device_vendor_id
  , c.installed_date
  , c.language
  , c.model
  , c.event_date
  , c.device_age
  , row_number() over(partition by c.device_vendor_id order by c.event_date) as active_day
  -- , c.events_count
  -- , c.trial_event_count
  -- , c.premium_event_count
  -- , c.hit_paywall_count
  -- , c.edit_asset_count
  -- , c.editor_done_taps_count
  -- , c.share_action_count
  , sum(c.events_count) over(partition by c.device_vendor_id order by c.event_date) as events_cumsum
  -- , sum(c.premium_event_count) over(partition by c.device_vendor_id order by c.event_date) as premium_event_cumsum
  , sum(c.trial_event_count) over(partition by c.device_vendor_id order by c.event_date) as trial_event_cumsum
  , sum(c.hit_paywall_count) over(partition by c.device_vendor_id order by c.event_date) as hit_paywall_cumsum
  , sum(c.edit_asset_count) over(partition by c.device_vendor_id order by c.event_date) as edit_asset_cumsum
  , sum(c.editor_done_taps_count) over(partition by c.device_vendor_id order by c.event_date) as editor_done_taps_cumsum
  , sum(c.share_action_count) over(partition by c.device_vendor_id order by c.event_date) as share_action_cumsum
  -- target
  , d.idfa
  , ifnull(d.subscription_start_d30, 0) as subscription_start_d30
  , case when d.subscription_start_d30 = 1 then 0.8 else 0.2 end as sample_weight
  , rand() as sample_rand
from daily_counts as c
join dimensions.devices_instasize as d
  on c.device_vendor_id = d.device_vendor_id
where premium_event_count = 0
)
select *
from daily_cumsum
;

create or replace table ml.premium_d30_training_samples__20200622
as
with lucky as (
select
  device_vendor_id
  , max(sample_rand) as max_sample
from ml.premium_d30_training__20200622
group by 1
)
select 
  t.*
from ml.premium_d30_training__20200622 as t
join lucky as l
  on t.device_vendor_id = l.device_vendor_id  
    and t.sample_rand = l.max_sample
;
