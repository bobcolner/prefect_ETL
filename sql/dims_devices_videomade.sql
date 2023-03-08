create or replace table dimensions.devices_videomade
partition by installed_date
as
with installs as (
select
    device_vendor_id
    , min(idfa) as idfa
    -- , min(subscriber_id) as subscriber_id
    , min(event_time) as installed_at
    , max(event_time) as latest_event_at
    , date(min(event_time)) as installed_date
from fastream.videomade_ios_prod
where 
    device_vendor_id is not null
    and event_date > date('2021-05-01')
    and event_date <= current_date()
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
    , max(latest_event_at) as latest_event_at
    -- counts
    , count(1) as event_count
    , sum(if(e.event_time >= datetime_add(i.installed_at, interval 1 day) and e.event_time < datetime_add(i.installed_at, interval 2 day), 1, 0)) as retention_d2
    , sum(if(e.event_time >= datetime_add(i.installed_at, interval 6 day) and e.event_time < datetime_add(i.installed_at, interval 7 day), 1, 0)) as retention_d7
    -- , sum(case when e.premium_status = 'free_trial' and e.event_time < datetime_add(i.installed_at, interval 1 day) then 1 else 0 end) as subscription_trial_d1
    -- , sum(case when e.premium_status = 'free_trial' and e.event_time < datetime_add(i.installed_at, interval 7 day) then 1 else 0 end) as subscription_trial_d7
    -- , sum(case when e.premium_status = 'active' and e.event_time < datetime_add(i.installed_at, interval 1 day) then 1 else 0 end) as subscription_start_d1
    -- , sum(case when e.premium_status = 'active' and e.event_time < datetime_add(i.installed_at, interval 30 day) then 1 else 0 end) as subscription_start_d30
    , sum(if(e.event_time < datetime_add(i.installed_at, interval 1 day) and e.event_name = 'library_start_editing', 1, 0)) as f1_library_start_editing_d1
    , sum(if(e.event_time < datetime_add(i.installed_at, interval 1 day) and e.event_name = 'editor_selection_next_tap', 1, 0)) as f2_editor_selection_next_tap_d1
    , sum(if(e.event_time < datetime_add(i.installed_at, interval 1 day) and e.event_name = 'editor_reorder_save_tap', 1, 0)) as f3_editor_reorder_save_tap_d1
    , sum(if(e.event_time < datetime_add(i.installed_at, interval 1 day) and e.event_name = 'save_export_tap', 1, 0)) as f4_save_export_tap_d1

    , sum(if(e.event_time < datetime_add(i.installed_at, interval 1 day) and e.event_name = 'onboarding_show_page_1', 1, 0)) as onboarding_show_page_1_d1
    , sum(if(e.event_time < datetime_add(i.installed_at, interval 1 day) and e.event_name = 'onboarding_show_page_2', 1, 0)) as onboarding_show_page_2_d1
    , sum(if(e.event_time < datetime_add(i.installed_at, interval 1 day) and e.event_name = 'onboarding_show_page_3', 1, 0)) as onboarding_show_page_3_d1
    , sum(if(e.event_time < datetime_add(i.installed_at, interval 1 day) and e.event_name = 'onboarding_show_page_4', 1, 0)) as onboarding_show_page_4_d1
from fastream.videomade_ios_prod as e
join installs as i
    on e.device_vendor_id = i.device_vendor_id
where 
    e.device_vendor_id is not null
    and event_date > date('2021-05-01')
    and event_date <= current_date()
group by 1
)
select
    device_vendor_id
    , idfa
    -- , subscriber_id
    , country
    , installed_at
    , installed_date
    , latest_event_at
    , event_count
    , if(datetime_add(installed_at, interval 2 day) < current_datetime(), retention_d2, null) as retention_d2
    , if(datetime_add(installed_at, interval 7 day) < current_datetime(), retention_d7, null) as retention_d7
    -- , if(datetime_add(installed_at, interval 1 day) < current_datetime(), subscription_trial_d1, null) as subscription_trial_d1
    -- , if(datetime_add(installed_at, interval 7 day) < current_datetime(), subscription_trial_d7, null) as subscription_trial_d7
    -- , if(datetime_add(installed_at, interval 1 day) < current_datetime(), subscription_start_d1, null) as subscription_start_d1
    -- , if(datetime_add(installed_at, interval 30 day) < current_datetime(), subscription_start_d30, null) as subscription_start_d30
    , if(datetime_add(installed_at, interval 1 day) < current_datetime(), f1_library_start_editing_d1, null) as f1_library_start_editing_d1
    , if(datetime_add(installed_at, interval 1 day) < current_datetime(), f2_editor_selection_next_tap_d1, null) as f2_editor_selection_next_tap_d1
    , if(datetime_add(installed_at, interval 1 day) < current_datetime(), f3_editor_reorder_save_tap_d1, null) as f3_editor_reorder_save_tap_d1
    , if(datetime_add(installed_at, interval 1 day) < current_datetime(), f4_save_export_tap_d1, null) as f4_save_export_tap_d1

    , if(datetime_add(installed_at, interval 1 day) < current_datetime(), onboarding_show_page_1_d1, null) as onboarding_show_page_1_d1
    , if(datetime_add(installed_at, interval 1 day) < current_datetime(), onboarding_show_page_2_d1, null) as onboarding_show_page_2_d1
    , if(datetime_add(installed_at, interval 1 day) < current_datetime(), onboarding_show_page_3_d1, null) as onboarding_show_page_3_d1
    , if(datetime_add(installed_at, interval 1 day) < current_datetime(), onboarding_show_page_4_d1, null) as onboarding_show_page_4_d1
from devices
