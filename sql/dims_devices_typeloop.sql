create or replace table dimensions.devices_typeloop_install
as 
select
    device_vendor_id
    , min(event_time) as installed_at
from fastream.typeloop_ios_prod
where 
    device_vendor_id is not null
    and date(event_date) >= date('2021-05-01')
    and date(event_date) <= current_date()
group by 1
;
-- call bq.refresh_materialized_view('mat.typeloop_ios_devices_install')
-- ;
create or replace table dimensions.devices_typeloop
partition by installed_date
as
with devices as (
select
    e.device_vendor_id
    , max(country) as country
    -- dates
    , date(min(i.installed_at)) as installed_date
    , min(i.installed_at) as installed_at
    -- counts
    , count(1) as event_count
    , sum(case when e.event_time >= datetime_add(i.installed_at, interval 1 day) and e.event_time < datetime_add(i.installed_at, interval 2 day) then 1 else 0 end) as retention_d2
    , sum(case when e.event_time >= datetime_add(i.installed_at, interval 6 day) and e.event_time < datetime_add(i.installed_at, interval 7 day) then 1 else 0 end) as retention_d7
    -- , sum(case when e.premium_status = 'free_trial' and e.event_time < datetime_add(i.installed_at, interval 1 day) then 1 else 0 end) as subscription_trial_d1
    -- , sum(case when e.premium_status = 'free_trial' and e.event_time < datetime_add(i.installed_at, interval 7 day) then 1 else 0 end) as subscription_trial_d7
    -- , sum(case when e.premium_status = 'active' and e.event_time < datetime_add(i.installed_at, interval 1 day) then 1 else 0 end) as subscription_start_d1
    -- , sum(case when e.premium_status = 'active' and e.event_time < datetime_add(i.installed_at, interval 30 day) then 1 else 0 end) as subscription_start_d30
from fastream.typeloop_ios_prod as e
join dimensions.devices_typeloop_install as i
    using(device_vendor_id)
where 
    e.device_vendor_id is not null
    and event_date >= date('2021-05-01')
    and event_date <= current_date()
    and i.installed_at <= current_datetime()
group by 1
)
select
    device_vendor_id
    , country
    , installed_at
    , installed_date
    , event_count
    , if(datetime_add(installed_at, interval 2 day) < current_datetime(), retention_d2, null) as retention_d2
    , if(datetime_add(installed_at, interval 7 day) < current_datetime(), retention_d7, null) as retention_d7
    -- , if(datetime_add(installed_at, interval 1 day) < current_datetime(), subscription_trial_d1, null) as subscription_trial_d1
    -- , if(datetime_add(installed_at, interval 7 day) < current_datetime(), subscription_trial_d7, null) as subscription_trial_d7
    -- , if(datetime_add(installed_at, interval 1 day) < current_datetime(), subscription_start_d1, null) as subscription_start_d1
    -- , if(datetime_add(installed_at, interval 30 day) < current_datetime(), subscription_start_d30, null) as subscription_start_d30
from devices
;