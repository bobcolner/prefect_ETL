create or replace table dimensions.devices_instasize_android
partition by installed_date
as
with installs as (
select
    device_vendor_id
    , min(event_time) as installed_at
    , date(min(event_time)) as installed_date
from fastream.instasize_android_prod
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
     -- counts
    , sum(case when e.event_time >= datetime_add(i.installed_at, interval 6 day) and e.event_time < datetime_add(i.installed_at, interval 7 day) then 1 else 0 end) as retention_d7
    , sum(case when e.premium_status = 'free_trial' and e.event_time < datetime_add(i.installed_at, interval 1 day) then 1 else 0 end) as subscription_trial_d1
    , sum(case when e.premium_status = 'free_trial' and e.event_time < datetime_add(i.installed_at, interval 7 day) then 1 else 0 end) as subscription_trial_d7
    , sum(case when e.premium_status = 'premium' and e.event_time < datetime_add(i.installed_at, interval 1 day) then 1 else 0 end) as subscription_start_d1
    , sum(case when e.premium_status = 'premium' and e.event_time < datetime_add(i.installed_at, interval 30 day) then 1 else 0 end) as subscription_start_d30
    -- funnel
    , sum(case when e.event_name = 'onboarding_show_first_page' and e.event_time < datetime_add(i.installed_at, interval 1 day) then 1 else 0 end) as funnel_onboarding_show_first_page_d1
    , sum(case when e.event_name = 'hit_paywall' and e.event_time < datetime_add(i.installed_at, interval 1 day) then 1 else 0 end) as funnel_onboarding_paywall_d1
    , sum(case when e.event_name = 'open_library' and e.event_time < datetime_add(i.installed_at, interval 1 day) then 1 else 0 end) as funnel_open_library_d1
    , sum(case when e.event_name = 'edit_asset' and e.event_time < datetime_add(i.installed_at, interval 1 day) then 1 else 0 end) as funnel_edit_asset_d1
    , sum(case when e.event_name = 'editor_done_taps' and e.event_time < datetime_add(i.installed_at, interval 1 day) then 1 else 0 end) as funnel_editor_done_d1
    , sum(case when e.event_name in ('Share Action', 'share_action') and e.event_time < datetime_add(i.installed_at, interval 1 day) then 1 else 0 end) as funnel_share_action_d1
    -- editor
    , sum(case when e.event_name = 'share_action' and json_extract_scalar(json_meta, '$.Filter Name') <> 'NA' and e.event_time < datetime_add(i.installed_at, interval 1 day) then 1 else 0 end) as edit_filter_d1
    , sum(case when e.event_name = 'share_action' and json_extract_scalar(json_meta, '$.Has Adjustment') = 'Yes' and e.event_time < datetime_add(i.installed_at, interval 1 day) then 1 else 0 end) as edit_adjustments_d1
    , sum(case when e.event_name = 'share_action' and json_extract_scalar(json_meta, '$.Border Size') <> '0' and e.event_time < datetime_add(i.installed_at, interval 1 day) then 1 else 0 end) as edit_border_size_d1
    , sum(case when e.event_name = 'share_action' and json_extract_scalar(json_meta, '$.Asset Cropped') = 'Yes' and e.event_time < datetime_add(i.installed_at, interval 1 day) then 1 else 0 end) as edit_crop_d1
    , sum(case when e.event_name = 'share_action' and json_extract_scalar(json_meta, '$.Aspect') not in('Full', 'Equal', '') and e.event_time < datetime_add(i.installed_at, interval 1 day) then 1 else 0 end) as edit_aspect_d1
    , sum(case when e.event_name = 'share_action' and json_extract_scalar(json_meta, '$.Collage Size') not in('1', '') and e.event_time < datetime_add(i.installed_at, interval 1 day) then 1 else 0 end) as edit_collage_d1
    , sum(case when e.event_name = 'share_action' and json_extract_scalar(json_meta, '$.Text Used') <> 'No' and e.event_time < datetime_add(i.installed_at, interval 1 day) then 1 else 0 end) as edit_text_d1
    , sum(case when e.event_name = 'share_action' and json_extract_scalar(json_meta, '$.Has Edits') <> 'No' and e.event_time < datetime_add(i.installed_at, interval 1 day) then 1 else 0 end) as has_edits_d1
    -- paywall
    , sum(case when e.event_name = 'hit_paywall' and json_extract_scalar(json_meta, '$.Origin') = 'EDITOR_DONE' and e.event_time < datetime_add(i.installed_at, interval 1 day) then 1 else 0 end) as paywall_editor_done_d1
    , sum(case when e.event_name = 'hit_paywall' and json_extract_scalar(json_meta, '$.Origin') = 'GRID' and e.event_time < datetime_add(i.installed_at, interval 1 day) then 1 else 0 end) as paywall_grid_d1
    , sum(case when e.event_name = 'hit_paywall' and json_extract_scalar(json_meta, '$.Origin') = 'EDITOR' and e.event_time < datetime_add(i.installed_at, interval 1 day) then 1 else 0 end) as paywall_editor_d1
    , sum(case when e.event_name = 'hit_paywall' and json_extract_scalar(json_meta, '$.Origin') = 'PROFILE' and e.event_time < datetime_add(i.installed_at, interval 1 day) then 1 else 0 end) as paywall_profile_d1
    , sum(case when e.event_name = 'hit_paywall' and json_extract_scalar(json_meta, '$.Origin') = 'ONBOARDING_NATIVE_PAYWALL' and e.event_time < datetime_add(i.installed_at, interval 1 day) then 1 else 0 end) as paywall_onboarding_native_d1
from fastream.instasize_android_prod as e
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
    , if(datetime_add(installed_at, interval 7 day) < current_datetime(), retention_d7, null) as retention_d7
    , if(datetime_add(installed_at, interval 1 day) < current_datetime(), subscription_trial_d1, null) as subscription_trial_d1
    , if(datetime_add(installed_at, interval 7 day) < current_datetime(), subscription_trial_d7, null) as subscription_trial_d7
    , if(datetime_add(installed_at, interval 1 day) < current_datetime(), subscription_start_d1, null) as subscription_start_d1
    , if(datetime_add(installed_at, interval 30 day) < current_datetime(), subscription_start_d30, null) as subscription_start_d30
    -- funnel
    , if(datetime_add(installed_at, interval 1 day) < current_datetime(), funnel_onboarding_show_first_page_d1, null) as funnel_onboarding_show_first_page_d1
    , if(datetime_add(installed_at, interval 1 day) < current_datetime(), funnel_onboarding_paywall_d1, null) as funnel_onboarding_paywall_d1
    , if(datetime_add(installed_at, interval 1 day) < current_datetime(), funnel_open_library_d1, null) as funnel_open_library_d1
    , if(datetime_add(installed_at, interval 1 day) < current_datetime(), funnel_edit_asset_d1, null) as funnel_edit_asset_d1
    , if(datetime_add(installed_at, interval 1 day) < current_datetime(), funnel_editor_done_d1, null) as funnel_editor_done_d1
    , if(datetime_add(installed_at, interval 1 day) < current_datetime(), funnel_share_action_d1, null) as funnel_share_action_d1
    -- editor
    , if(datetime_add(installed_at, interval 1 day) < current_datetime(), edit_filter_d1, null) as edit_filter_d1
    , if(datetime_add(installed_at, interval 1 day) < current_datetime(), edit_adjustments_d1, null) as edit_adjustments_d1
    , if(datetime_add(installed_at, interval 1 day) < current_datetime(), edit_border_size_d1, null) as edit_border_size_d1
    , if(datetime_add(installed_at, interval 1 day) < current_datetime(), edit_crop_d1, null) as edit_crop_d1
    , if(datetime_add(installed_at, interval 1 day) < current_datetime(), edit_aspect_d1, null) as edit_aspect_d1
    , if(datetime_add(installed_at, interval 1 day) < current_datetime(), edit_collage_d1, null) as edit_collage_d1
    , if(datetime_add(installed_at, interval 1 day) < current_datetime(), edit_text_d1, null) as edit_text_d1
    , if(datetime_add(installed_at, interval 1 day) < current_datetime(), has_edits_d1, null) as has_edits_d1
    -- paywall
    , if(datetime_add(installed_at, interval 1 day) < current_datetime(), paywall_editor_done_d1, null) as paywall_editor_done_d1
    , if(datetime_add(installed_at, interval 1 day) < current_datetime(), paywall_grid_d1, null) as paywall_grid_d1
    , if(datetime_add(installed_at, interval 1 day) < current_datetime(), paywall_editor_d1, null) as paywall_editor_d1
    , if(datetime_add(installed_at, interval 1 day) < current_datetime(), paywall_profile_d1, null) as paywall_profile_d1
    , if(datetime_add(installed_at, interval 1 day) < current_datetime(), paywall_onboarding_native_d1, null) as paywall_onboarding_native_d1
from devices
;