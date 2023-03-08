create or replace table dimensions.devices_instasize
partition by installed_date
as
with devices_censored as (
select
    device_vendor_id
    , idfa
    , subscriber_id
    , case when idfa = '00000000-0000-0000-0000-000000000000' then 'LAT Enabled' else 'LAT Disabled' end as lat_status
    , installed_at
    , installed_date
    , onboarding_at
    , trial_started_at
    , paid_started_at
    , country
    -- sessions
    , if(datetime_add(installed_at, interval 1 day) < current_datetime(), session_sec_avg_d1, null) as session_sec_avg_d1
    , if(datetime_add(installed_at, interval 7 day) < current_datetime(), session_sec_avg_d7, null) as session_sec_avg_d7
    -- funnel day 1
    , if(datetime_add(installed_at, interval 1 day) < current_datetime(), funnel_onboarding_show_first_page_d1, null) as funnel_onboarding_show_first_page_d1
    , if(datetime_add(installed_at, interval 1 day) < current_datetime(), funnel_onboarding_paywall_d1, null) as funnel_onboarding_paywall_d1
    , if(datetime_add(installed_at, interval 1 day) < current_datetime(), onboarding_show_premium_d1, null) as onboarding_show_premium_d1
    , if(datetime_add(installed_at, interval 1 day) < current_datetime(), funnel_open_library_d1, null) as funnel_open_library_d1
    , if(datetime_add(installed_at, interval 1 day) < current_datetime(), funnel_edit_asset_d1, null) as funnel_edit_asset_d1
    , if(datetime_add(installed_at, interval 1 day) < current_datetime(), funnel_editor_done_d1, null) as funnel_editor_done_d1
    , if(datetime_add(installed_at, interval 1 day) < current_datetime(), funnel_share_action_d1, null) as funnel_share_action_d1
    -- 
    , if(datetime_add(onboarding_at, interval 1 day) < current_datetime(), f2_onboarding_paywall_d1, null) as f2_onboarding_paywall_d1
    , if(datetime_add(onboarding_at, interval 1 day) < current_datetime(), f2_show_premium_d1, null) as f2_show_premium_d1
    , if(datetime_add(onboarding_at, interval 1 day) < current_datetime(), f2_open_library_d1, null) as f2_open_library_d1
    , if(datetime_add(onboarding_at, interval 1 day) < current_datetime(), f2_edit_asset_d1, null) as f2_edit_asset_d1
    , if(datetime_add(onboarding_at, interval 1 day) < current_datetime(), f2_editor_done_d1, null) as f2_editor_done_d1
    , if(datetime_add(onboarding_at, interval 1 day) < current_datetime(), f2_share_action_d1, null) as f2_share_action_d1
    -- edit/share by content
    , if(datetime_add(installed_at, interval 7 day) < current_datetime(), share_image_d7, null) as share_image_d7
    , if(datetime_add(installed_at, interval 7 day) < current_datetime(), share_collage_d7, null) as share_collage_d7
    , if(datetime_add(installed_at, interval 7 day) < current_datetime(), share_video_d7, null) as share_video_d7
    -- editor engagment
    , if(datetime_add(installed_at, interval 1 day) < current_datetime(), edit_filter_d1, null) as edit_filter_d1
    , if(datetime_add(installed_at, interval 1 day) < current_datetime(), edit_adjustments_d1, null) as edit_adjustments_d1
    , if(datetime_add(installed_at, interval 1 day) < current_datetime(), edit_border_size_d1, null) as edit_border_size_d1
    , if(datetime_add(installed_at, interval 1 day) < current_datetime(), edit_crop_d1, null) as edit_crop_d1
    , if(datetime_add(installed_at, interval 1 day) < current_datetime(), edit_text_style_d1, null) as edit_text_style_d1
    , if(datetime_add(installed_at, interval 1 day) < current_datetime(), edit_aspect_d1, null) as edit_aspect_d1
    , if(datetime_add(installed_at, interval 1 day) < current_datetime(), edit_border_d1, null) as edit_border_d1
    , if(datetime_add(installed_at, interval 1 day) < current_datetime(), edit_text_d1, null) as edit_text_d1
    -- format
    , if(datetime_add(installed_at, interval 1 day) < current_datetime(), format_full_d1, null) as format_full_d1
    , if(datetime_add(installed_at, interval 1 day) < current_datetime(), format_ig_full_d1, null) as format_ig_full_d1
    , if(datetime_add(installed_at, interval 1 day) < current_datetime(), format_ig_story_d1, null) as format_ig_story_d1
    , if(datetime_add(installed_at, interval 1 day) < current_datetime(), format_instasize_d1, null) as format_instasize_d1
    , if(datetime_add(installed_at, interval 1 day) < current_datetime(), format_landscape_d1, null) as format_landscape_d1
    -- subscritpion status
    , if(datetime_add(installed_at, interval 1 day) < current_datetime(), subscription_trial_d1, null) as subscription_trial_d1
    , if(datetime_add(installed_at, interval 7 day) < current_datetime(), subscription_trial_d7, null) as subscription_trial_d7
    , if(datetime_add(installed_at, interval 1 day) < current_datetime(), subscription_start_d1, null) as subscription_start_d1
    , if(datetime_add(installed_at, interval 7 day) < current_datetime(), subscription_start_d7, null) as subscription_start_d7
    , if(datetime_add(installed_at, interval 14 day) < current_datetime(), subscription_start_d14, null) as subscription_start_d14
    , if(datetime_add(installed_at, interval 30 day) < current_datetime(), subscription_start_d30, null) as subscription_start_d30
    -- pawalls
    , if(datetime_add(installed_at, interval 1 day) < current_datetime(), paywall_editor_done_d1, null) as paywall_editor_done_d1
    , if(datetime_add(installed_at, interval 1 day) < current_datetime(), paywall_deep_link_d1, null) as paywall_deep_link_d1
    , if(datetime_add(installed_at, interval 1 day) < current_datetime(), paywall_app_launch_d1, null) as paywall_app_launch_d1
    , if(datetime_add(installed_at, interval 1 day) < current_datetime(), paywall_onbarding_v3_d1, null) as paywall_onbarding_v3_d1
    , if(datetime_add(installed_at, interval 1 day) < current_datetime(), paywall_editor_d1, null) as paywall_editor_d1
    , if(datetime_add(installed_at, interval 1 day) < current_datetime(), paywall_grid_d1, null) as paywall_grid_d1
    , if(datetime_add(installed_at, interval 1 day) < current_datetime(), paywall_settings_d1, null) as paywall_settings_d1
    -- early user engagment
    , if(datetime_add(installed_at, interval 1 day) < current_datetime(), engagment_d1, null) as engagment_d1
    , if(datetime_add(installed_at, interval 7 day) < current_datetime(), engagment_d7, null) as engagment_d7
    , if(datetime_add(installed_at, interval 30 day) < current_datetime(), engagment_d30, null) as engagment_d30
    -- daily retention
    , if(datetime_add(installed_at, interval 2 day) < current_datetime(), retention_d2, null) as retention_d2
    , if(datetime_add(installed_at, interval 3 day) < current_datetime(), retention_d3, null) as retention_d3
    , if(datetime_add(installed_at, interval 7 day) < current_datetime(), retention_d7, null) as retention_d7
    , if(datetime_add(installed_at, interval 30 day) < current_datetime(), retention_d30, null) as retention_d30
    -- weekly
    , if(datetime_add(installed_at, interval 2 week) < current_datetime(), retention_w2, null) as retention_w2
    , if(datetime_add(installed_at, interval 4 week) < current_datetime(), retention_w4, null) as retention_w4
from dimensions.devices_metrics_instasize
)
select 
    *
    , current_datetime() as updated_at
    , 1 as updated_count
from devices_censored
;