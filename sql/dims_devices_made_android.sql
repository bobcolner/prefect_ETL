create or replace table dimensions.devices_made_android
partition by installed_date
as
with installs as (
select
    device_vendor_id
    , min(event_date) as installed_at
    , date(min(event_date)) as installed_date
    , max(event_date) as latest_event_at
from fastream.made_android_prod
where 
    device_vendor_id is not null
    and event_date <= current_datetime()
group by 1
), devices as (
select
    e.device_vendor_id
    , max(country) as country
    -- dates
    , min(i.installed_at) as installed_at
    , min(i.installed_date) as installed_date
    , max(latest_event_at) as latest_event_at
    -- counts
    , count(1) as event_count
    , sum(case when e.event_date >= datetime_add(i.installed_at, interval 6 day) and e.event_date < datetime_add(i.installed_at, interval 7 day) then 1 else 0 end) as retention_d7
    , sum(case when e.premium_status = 'free_trial' and e.event_date < datetime_add(i.installed_at, interval 1 day) then 1 else 0 end) as subscription_trial_d1
    , sum(case when e.premium_status = 'free_trial' and e.event_date < datetime_add(i.installed_at, interval 7 day) then 1 else 0 end) as subscription_trial_d7
    , sum(case when e.premium_status = 'premium' and e.event_date < datetime_add(i.installed_at, interval 1 day) then 1 else 0 end) as subscription_start_d1
    , sum(case when e.premium_status = 'premium' and e.event_date < datetime_add(i.installed_at, interval 30 day) then 1 else 0 end) as subscription_start_d30
from fastream.made_android_prod as e
join installs as i
    on e.device_vendor_id = i.device_vendor_id
where 
    e.device_vendor_id is not null
    and e.event_date <= current_datetime()
group by 1
)
select
    device_vendor_id
    , country
    , installed_at
    , installed_date
    , latest_event_at
    , event_count
    , if(datetime_add(installed_at, interval 7 day) < current_datetime(), retention_d7, null) as retention_d7
    , if(datetime_add(installed_at, interval 1 day) < current_datetime(), subscription_trial_d1, null) as subscription_trial_d1
    , if(datetime_add(installed_at, interval 7 day) < current_datetime(), subscription_trial_d7, null) as subscription_trial_d7
    , if(datetime_add(installed_at, interval 1 day) < current_datetime(), subscription_start_d1, null) as subscription_start_d1
    , if(datetime_add(installed_at, interval 30 day) < current_datetime(), subscription_start_d30, null) as subscription_start_d30
from devices
;

-- create or replace table dimensions.devices_android_made
-- partition by installed_date
-- as
-- with installs as (
-- select
--     device_id
--     , min(time) as installed_at
--     , max(time) as latest_event_at
--     , date(min(time)) as installed_date
-- from analytics.made_android_app_events
-- where 
--     device_id is not null
--     and time <= current_datetime()
-- group by 1
-- ), devices as (
-- select
--     e.device_id
--     , max(country) as country
--     -- dates
--     , min(i.installed_at) as installed_at
--     , min(i.installed_date) as installed_date
--     , max(latest_event_at) as latest_event_at
--     -- counts
--     , count(1) as event_count
--     , sum(case when e.time >= datetime_add(i.installed_at, interval 6 day) and e.time < datetime_add(i.installed_at, interval 7 day) then 1 else 0 end) as retention_d7
--     , sum(case when e.subscription_status = 'free_trial' and e.time < datetime_add(i.installed_at, interval 1 day) then 1 else 0 end) as subscription_trial_d1
--     , sum(case when e.subscription_status = 'free_trial' and e.time < datetime_add(i.installed_at, interval 7 day) then 1 else 0 end) as subscription_trial_d7
--     , sum(case when e.subscription_status = 'premium' and e.time < datetime_add(i.installed_at, interval 1 day) then 1 else 0 end) as subscription_start_d1
--     , sum(case when e.subscription_status = 'premium' and e.time < datetime_add(i.installed_at, interval 30 day) then 1 else 0 end) as subscription_start_d30
-- from analytics.made_android_app_events as e
-- join installs as i
--     on e.device_id = i.device_id
-- where 
--     e.device_id is not null
--     and e.time <= current_datetime()
-- group by 1
-- )
-- select
--     device_id
--     , country
--     , installed_at
--     , installed_date
--     , latest_event_at
--     , event_count
--     , if(datetime_add(installed_at, interval 7 day) < current_datetime(), retention_d7, null) as retention_d7
--     , if(datetime_add(installed_at, interval 1 day) < current_datetime(), subscription_trial_d1, null) as subscription_trial_d1
--     , if(datetime_add(installed_at, interval 7 day) < current_datetime(), subscription_trial_d7, null) as subscription_trial_d7
--     , if(datetime_add(installed_at, interval 1 day) < current_datetime(), subscription_start_d1, null) as subscription_start_d1
--     , if(datetime_add(installed_at, interval 30 day) < current_datetime(), subscription_start_d30, null) as subscription_start_d30
-- from devices
-- ;
