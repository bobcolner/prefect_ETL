create or replace table dimensions.devices_selfiemade
partition by installed_date
as
with installs as (
select
    device_vendor_id
    , min(idfa) as idfa
    -- , min(subscriber_id) as subscriber_id
    , min(event_time) as installed_at
    , date(min(event_time)) as installed_date
    , max(event_time) as latest_event_at
    , min(if(event_name = 'onboarding_show_page_1', event_time, null)) as onboarding_at
from fastream.selfiemade_ios_prod
where 
    device_vendor_id is not null
    and event_date <= current_date()
    and event_date > date('2021-05-01')
group by 1
), devices as (
select
    e.device_vendor_id
    , min(i.idfa) as idfa
    -- , min(i.subscriber_id) as subscriber_id
    , max(country) as country
    -- dates
    , min(i.installed_at) as installed_at
    , min(i.installed_date) as installed_date
    , max(i.latest_event_at) as latest_event_at
    , min(i.onboarding_at) as onboarding_at
    -- counts
    , count(1) as event_count
    , sum(case when e.event_time >= datetime_add(i.installed_at, interval 1 day) and e.event_time < datetime_add(i.installed_at, interval 2 day) then 1 else 0 end) as retention_d2
    , sum(case when e.event_time >= datetime_add(i.installed_at, interval 6 day) and e.event_time < datetime_add(i.installed_at, interval 7 day) then 1 else 0 end) as retention_d7
    , sum(case when e.premium_status = 'free_trial' and e.event_time < datetime_add(i.installed_at, interval 1 day) then 1 else 0 end) as subscription_trial_d1
    , sum(case when e.premium_status = 'free_trial' and e.event_time < datetime_add(i.installed_at, interval 7 day) then 1 else 0 end) as subscription_trial_d7
    , sum(case when e.premium_status = 'active' and e.event_time < datetime_add(i.installed_at, interval 1 day) then 1 else 0 end) as subscription_start_d1
    , sum(case when e.premium_status = 'active' and e.event_time < datetime_add(i.installed_at, interval 30 day) then 1 else 0 end) as subscription_start_d30
    -- funnel
    , sum(if(e.event_time < datetime_add(i.installed_at, interval 14 day) and e.event_name = 'onboarding_show_page_1', 1, 0)) as onboarding_show_page_1_d1
    , sum(if(e.event_time < datetime_add(i.installed_at, interval 14 day) and e.event_name = 'onboarding_show_page_2', 1, 0)) as onboarding_show_page_2_d1
    , sum(if(e.event_time < datetime_add(i.installed_at, interval 14 day) and e.event_name = 'onboarding_show_page_3', 1, 0)) as onboarding_show_page_3_d1
    , sum(if(e.event_time < datetime_add(i.installed_at, interval 14 day) and e.event_name = 'onboarding_show_page_4', 1, 0)) as onboarding_show_page_4_d1
    , sum(if(e.event_time < datetime_add(i.installed_at, interval 14 day) and e.event_name = 'onboarding_completed', 1, 0)) as onboarding_completed_d1
    , sum(if(e.event_time < datetime_add(i.installed_at, interval 14 day) and e.event_name = 'edit_photo_tap', 1, 0)) as edit_photo_tap_d1
    , sum(if(e.event_time < datetime_add(i.installed_at, interval 14 day) and e.event_name = 'editor_save_tap', 1, 0)) as editor_save_tap_d1
    , sum(if(e.event_time < datetime_add(i.installed_at, interval 14 day) and e.event_name = 'save_export_tap', 1, 0)) as save_export_tap_d1
    -- onbarding funnel
    , sum(if(e.event_time < datetime_add(i.onboarding_at, interval 1 day) and e.event_name = 'onboarding_show_page_1', 1, 0)) as onboarding_funnel_1_d1
    , sum(if(e.event_time < datetime_add(i.onboarding_at, interval 1 day) and e.event_name = 'onboarding_show_page_2', 1, 0)) as onboarding_funnel_2_d1
    , sum(if(e.event_time < datetime_add(i.onboarding_at, interval 1 day) and e.event_name = 'onboarding_show_page_3', 1, 0)) as onboarding_funnel_3_d1
    , sum(if(e.event_time < datetime_add(i.onboarding_at, interval 1 day) and e.event_name = 'onboarding_show_page_4', 1, 0)) as onboarding_funnel_4_d1
    , sum(if(e.event_time < datetime_add(i.onboarding_at, interval 1 day) and e.event_name = 'onboarding_completed', 1, 0)) as onboarding_funnel_completed_d1
from fastream.selfiemade_ios_prod as e
join installs as i
    on(e.device_vendor_id = i.device_vendor_id)
where 
    e.device_vendor_id is not null
    and e.event_date <= current_datetime()
    and e.event_date > datetime('2021-05-01')
group by 1
)
select
    device_vendor_id
    , idfa
    -- , subscriber_id
    , country
    , installed_at
    , installed_date
    , onboarding_at
    , latest_event_at
    , event_count
    , if(datetime_add(installed_at, interval 2 day) < current_datetime(), retention_d2, null) as retention_d2
    , if(datetime_add(installed_at, interval 7 day) < current_datetime(), retention_d7, null) as retention_d7
    , if(datetime_add(installed_at, interval 1 day) < current_datetime(), subscription_trial_d1, null) as subscription_trial_d1
    , if(datetime_add(installed_at, interval 7 day) < current_datetime(), subscription_trial_d7, null) as subscription_trial_d7
    , if(datetime_add(installed_at, interval 1 day) < current_datetime(), subscription_start_d1, null) as subscription_start_d1
    , if(datetime_add(installed_at, interval 30 day) < current_datetime(), subscription_start_d30, null) as subscription_start_d30
    -- install funnel
    , if(datetime_add(installed_at, interval 1 day) < current_datetime(), onboarding_show_page_1_d1, null) as onboarding_show_page_1_d1
    , if(datetime_add(installed_at, interval 1 day) < current_datetime(), onboarding_show_page_2_d1, null) as onboarding_show_page_2_d1
    , if(datetime_add(installed_at, interval 1 day) < current_datetime(), onboarding_show_page_3_d1, null) as onboarding_show_page_3_d1
    , if(datetime_add(installed_at, interval 1 day) < current_datetime(), onboarding_show_page_4_d1, null) as onboarding_show_page_4_d1
    , if(datetime_add(installed_at, interval 1 day) < current_datetime(), onboarding_completed_d1, null) as onboarding_completed_d1
    , if(datetime_add(installed_at, interval 1 day) < current_datetime(), edit_photo_tap_d1, null) as edit_photo_tap_d1
    , if(datetime_add(installed_at, interval 1 day) < current_datetime(), editor_save_tap_d1, null) as editor_save_tap_d1
    , if(datetime_add(installed_at, interval 1 day) < current_datetime(), save_export_tap_d1, null) as save_export_tap_d1
    -- onboarding funnel
    , if(datetime_add(onboarding_at, interval 1 day) < current_datetime(), onboarding_funnel_1_d1, null) as onboarding_funnel_1_d1
    , if(datetime_add(onboarding_at, interval 1 day) < current_datetime(), onboarding_funnel_2_d1, null) as onboarding_funnel_2_d1
    , if(datetime_add(onboarding_at, interval 1 day) < current_datetime(), onboarding_funnel_3_d1, null) as onboarding_funnel_3_d1
    , if(datetime_add(onboarding_at, interval 1 day) < current_datetime(), onboarding_funnel_4_d1, null) as onboarding_funnel_4_d1
    , if(datetime_add(onboarding_at, interval 1 day) < current_datetime(), onboarding_funnel_completed_d1, null) as onboarding_funnel_completed_d1
from devices
;