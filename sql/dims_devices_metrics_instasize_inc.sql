create or replace table dimensions.devices_metrics_instasize_stg
partition by installed_date
as
select
    e.device_vendor_id
    , max(e.idfa) as idfa
    , max(e.subscriber_id) as subscriber_id
    , max(e.country) as country
    , min(i.installed_at) as installed_at
    , date(min(i.installed_at)) as installed_date
    , min(i.onboarding_at) as onboarding_at
    , min(i.trial_started_at) as trial_started_at
    , min(i.paid_started_at) as paid_started_at
     -- session length
    , avg(case when e.event_name = 'session_end' and e.event_time < datetime_add(i.installed_at, interval 1 day) then cast(json_extract_scalar(json_meta, '$.total_session_sec') as int64) else null end) as session_sec_avg_d1
    , avg(case when e.event_name = 'session_end' and e.event_time < datetime_add(i.installed_at, interval 7 day) then cast(json_extract_scalar(json_meta, '$.total_session_sec') as int64) else null end) as session_sec_avg_d7
    -- subscription status
    , max(case when e.premium_status = 'trial' and e.event_time < datetime_add(i.installed_at, interval 1 day) then 1 else 0 end) as subscription_trial_d1
    , max(case when e.premium_status = 'trial' and e.event_time < datetime_add(i.installed_at, interval 7 day) then 1 else 0 end) as subscription_trial_d7
    , max(case when e.premium_status = 'active' and e.event_time < datetime_add(i.installed_at, interval 1 day) then 1 else 0 end) as subscription_start_d1
    , max(case when e.premium_status = 'active' and e.event_time < datetime_add(i.installed_at, interval 7 day) then 1 else 0 end) as subscription_start_d7
    , max(case when e.premium_status = 'active' and e.event_time < datetime_add(i.installed_at, interval 14 day) then 1 else 0 end) as subscription_start_d14
    , max(case when e.premium_status = 'active' and e.event_time < datetime_add(i.installed_at, interval 30 day) then 1 else 0 end) as subscription_start_d30
    -- product funnel (day1): install cohorted
    , max(case when e.event_name = 'onboarding_show_first_page' and e.event_time < datetime_add(i.installed_at, interval 1 day) then 1 else 0 end) as funnel_onboarding_show_first_page_d1
    , max(case when e.event_name = 'hit_paywall' and e.event_time < datetime_add(i.installed_at, interval 1 day) then 1 else 0 end) as funnel_onboarding_paywall_d1
    , max(case when e.event_name = 'onboarding_show_premium' and e.event_time < datetime_add(i.installed_at, interval 1 day) then 1 else 0 end) as onboarding_show_premium_d1
    , max(case when e.event_name = 'open_library' and e.event_time < datetime_add(i.installed_at, interval 1 day) then 1 else 0 end) as funnel_open_library_d1
    , max(case when e.event_name = 'edit_asset' and e.event_time < datetime_add(i.installed_at, interval 1 day) then 1 else 0 end) as funnel_edit_asset_d1
    , max(case when e.event_name = 'editor_done_taps' and e.event_time < datetime_add(i.installed_at, interval 1 day) then 1 else 0 end) as funnel_editor_done_d1
    , max(case when e.event_name in ('Share Action', 'share_action') and e.event_time < datetime_add(i.installed_at, interval 1 day) then 1 else 0 end) as funnel_share_action_d1
    -- funnel: oboarding cohorted
    , max(case when e.event_name = 'hit_paywall' and e.event_time < datetime_add(i.onboarding_at, interval 1 day) then 1 else 0 end) as f2_onboarding_paywall_d1
    , max(case when e.event_name = 'onboarding_show_premium' and e.event_time < datetime_add(i.onboarding_at, interval 1 day) then 1 else 0 end) as f2_show_premium_d1
    , max(case when e.event_name = 'open_library' and e.event_time < datetime_add(i.onboarding_at, interval 1 day) then 1 else 0 end) as f2_open_library_d1
    , max(case when e.event_name = 'edit_asset' and e.event_time < datetime_add(i.onboarding_at, interval 1 day) then 1 else 0 end) as f2_edit_asset_d1
    , max(case when e.event_name = 'editor_done_taps' and e.event_time < datetime_add(i.onboarding_at, interval 1 day) then 1 else 0 end) as f2_editor_done_d1
    , max(case when e.event_name in ('Share Action', 'share_action') and e.event_time < datetime_add(i.onboarding_at, interval 1 day) then 1 else 0 end) as f2_share_action_d1
    -- share content types
    , max(case when e.event_name in ('Share Action', 'share_action') and json_extract_scalar(json_meta, '$.Type') = 'Image' and e.event_time < datetime_add(i.installed_at, interval 7 day) then 1 else 0 end) as share_image_d7
    , max(case when e.event_name in ('Share Action', 'share_action') and cast(json_extract_scalar(json_meta, '$.Collage Size') as int64) > 1 and e.event_time < datetime_add(i.installed_at, interval 7 day) then 1 else 0 end) as share_collage_d7
    , max(case when e.event_name in ('Share Action', 'share_action') and json_extract_scalar(json_meta, '$.Type') = 'Video' and e.event_time < datetime_add(i.installed_at, interval 7 day) then 1 else 0 end) as share_video_d7
    -- editor engagment
    , max(case when e.event_name in ('Share Action', 'share_action') and json_extract_scalar(json_meta, '$.Filter name') <> 'NA' and e.event_time < datetime_add(i.installed_at, interval 1 day) then 1 else 0 end) as edit_filter_d1
    , max(case when e.event_name in ('Share Action', 'share_action') and json_extract_scalar(json_meta, '$.Has Adjustment') = 'true' and e.event_time < datetime_add(i.installed_at, interval 1 day) then 1 else 0 end) as edit_adjustments_d1
    , max(case when e.event_name in ('Share Action', 'share_action') and json_extract_scalar(json_meta, '$.Border size') <> '0' and e.event_time < datetime_add(i.installed_at, interval 1 day) then 1 else 0 end) as edit_border_size_d1
    , max(case when e.event_name in ('Share Action', 'share_action') and json_extract_scalar(json_meta, '$.border_pattern') not in('Color - C1', 'NA') and e.event_time < datetime_add(i.installed_at, interval 1 day) then 1 else 0 end) as edit_border_d1
    , max(case when e.event_name in ('Share Action', 'share_action') and json_extract_scalar(json_meta, '$.Asset Cropped') = 'true' and e.event_time < datetime_add(i.installed_at, interval 1 day) then 1 else 0 end) as edit_crop_d1
    , max(case when e.event_name in ('Share Action', 'share_action') and json_extract_scalar(json_meta, '$.Text Styles used') = 'Yes' and e.event_time < datetime_add(i.installed_at, interval 1 day) then 1 else 0 end) as edit_text_style_d1
    , max(case when e.event_name in ('Share Action', 'share_action') and json_extract_scalar(json_meta, '$.Aspect') not in('Full', 'Equal', '') and e.event_time < datetime_add(i.installed_at, interval 1 day) then 1 else 0 end) as edit_aspect_d1
    , max(case when e.event_name in ('Share Action', 'share_action') and json_extract_scalar(json_meta, '$.Collage Size') not in('1', '') and e.event_time < datetime_add(i.installed_at, interval 1 day) then 1 else 0 end) as edit_collage_d1
    , max(case when e.event_name in ('Share Action', 'share_action') and json_extract_scalar(json_meta, '$.Premium Tools Used') = 'true' and e.event_time < datetime_add(i.installed_at, interval 1 day) then 1 else 0 end) as edit_premium_tools_d1
    , max(case when e.event_name in ('Share Action', 'share_action') and (json_extract_scalar(json_meta, '$.text_block_font') <> 'NA' or json_extract_scalar(json_meta, '$.Text used') = 'Yes') and e.event_time < datetime_add(i.installed_at, interval 1 day) then 1 else 0 end) as edit_text_d1
    -- aspect format
    , max(case when e.event_name in ('Share Action', 'share_action') and json_extract_scalar(json_meta, '$.Aspect') = 'Full' and e.event_time < datetime_add(i.installed_at, interval 1 day) then 1 else 0 end) as format_full_d1
    , max(case when e.event_name in ('Share Action', 'share_action') and json_extract_scalar(json_meta, '$.Aspect') = 'IG Full' and e.event_time < datetime_add(i.installed_at, interval 1 day) then 1 else 0 end) as format_ig_full_d1
    , max(case when e.event_name in ('Share Action', 'share_action') and json_extract_scalar(json_meta, '$.Aspect') = 'IG Story' and e.event_time < datetime_add(i.installed_at, interval 1 day) then 1 else 0 end) as format_ig_story_d1
    , max(case when e.event_name in ('Share Action', 'share_action') and json_extract_scalar(json_meta, '$.Aspect') = 'Instasize' and e.event_time < datetime_add(i.installed_at, interval 1 day) then 1 else 0 end) as format_instasize_d1
    , max(case when e.event_name in ('Share Action', 'share_action') and json_extract_scalar(json_meta, '$.Aspect') = 'Landscape' and e.event_time < datetime_add(i.installed_at, interval 1 day) then 1 else 0 end) as format_landscape_d1
    -- paywall
    , max(case when e.event_name = 'hit_paywall' and json_extract_scalar(json_meta, '$.Origin') = 'Editor Done' and e.event_time < datetime_add(i.installed_at, interval 1 day) then 1 else 0 end) as paywall_editor_done_d1
    , max(case when e.event_name = 'hit_paywall' and json_extract_scalar(json_meta, '$.Origin') = 'Deep Link' and e.event_time < datetime_add(i.installed_at, interval 1 day) then 1 else 0 end) as paywall_deep_link_d1
    , max(case when e.event_name = 'hit_paywall' and json_extract_scalar(json_meta, '$.Origin') = 'App Launch' and e.event_time < datetime_add(i.installed_at, interval 1 day) then 1 else 0 end) as paywall_app_launch_d1
    , max(case when e.event_name = 'hit_paywall' and json_extract_scalar(json_meta, '$.Origin') = 'Onboarding V3' and e.event_time < datetime_add(i.installed_at, interval 1 day) then 1 else 0 end) as paywall_onbarding_v3_d1
    , max(case when e.event_name = 'hit_paywall' and json_extract_scalar(json_meta, '$.Origin') = 'Editor' and e.event_time < datetime_add(i.installed_at, interval 1 day) then 1 else 0 end) as paywall_editor_d1
    , max(case when e.event_name = 'hit_paywall' and json_extract_scalar(json_meta, '$.Origin') = 'Grid' and e.event_time < datetime_add(i.installed_at, interval 1 day) then 1 else 0 end) as paywall_grid_d1
    , max(case when e.event_name = 'hit_paywall' and json_extract_scalar(json_meta, '$.Origin') = 'Settings' and e.event_time < datetime_add(i.installed_at, interval 1 day) then 1 else 0 end) as paywall_settings_d1
    -- engagment metric
    , sum(case when e.event_time < datetime_add(i.installed_at, interval 1 day) and e.event_name in ('edit_asset', 'share_action', 'Share Action') then 1 else 0 end) as engagment_d1
    , sum(case when e.event_time < datetime_add(i.installed_at, interval 7 day) and e.event_name in ('edit_asset', 'share_action', 'Share Action') then 1 else 0 end) as engagment_d7
    , sum(case when e.event_time < datetime_add(i.installed_at, interval 30 day) and e.event_name in ('edit_asset', 'share_action', 'Share Action') then 1 else 0 end) as engagment_d30
    -- daily retention
    , max(case when e.event_time >= datetime_add(i.installed_at, interval 1 day) and e.event_time < datetime_add(i.installed_at, interval 2 day) then 1 else 0 end) as retention_d2
    , max(case when e.event_time >= datetime_add(i.installed_at, interval 2 day) and e.event_time < datetime_add(i.installed_at, interval 3 day) then 1 else 0 end) as retention_d3
    , max(case when e.event_time >= datetime_add(i.installed_at, interval 6 day) and e.event_time < datetime_add(i.installed_at, interval 7 day) then 1 else 0 end) as retention_d7
    , max(case when e.event_time >= datetime_add(i.installed_at, interval 29 day) and e.event_time < datetime_add(i.installed_at, interval 30 day) then 1 else 0 end) as retention_d30
    -- weekly retention
    , max(case when e.event_time >= datetime_add(i.installed_at, interval 1 week) and e.event_time < datetime_add(i.installed_at, interval 2 week) then 1 else 0 end) as retention_w2
    , max(case when e.event_time >= datetime_add(i.installed_at, interval 3 week) and e.event_time < datetime_add(i.installed_at, interval 4 week) then 1 else 0 end) as retention_w4
    -- metadata
    , current_datetime() as updated_at
    , 1 as updated_count
from fastream.instasize_ios_prod as e
join dimensions.devices_install_instasize as i
    using(device_vendor_id)
where    
    e.device_vendor_id is not null
    and date(e.event_date) between
        date_add(current_date(), interval - 32 day)
        and date_add(current_date(), interval - 1 day)
    -- and date(e.event_date) >= date('2019-04-01') --un-comment to rebuild full table
    -- and date(e.event_date) < current_date()
group by 1
;

create or replace table dimensions.devices_metrics_instasize_backup
as
select * from dimensions.devices_metrics_instasize
;

merge into dimensions.devices_metrics_instasize as t
using dimensions.devices_metrics_instasize_stg as s
    on t.device_vendor_id = s.device_vendor_id
when matched then
    update set
        session_sec_avg_d1 = greatest(t.session_sec_avg_d1, s.session_sec_avg_d1)
        , session_sec_avg_d7 = greatest(t.session_sec_avg_d7, s.session_sec_avg_d7)
        , subscription_trial_d1 = greatest(t.subscription_trial_d1, s.subscription_trial_d1)
        , subscription_trial_d7 = greatest(t.subscription_trial_d7, s.subscription_trial_d7)
        , subscription_start_d1 = greatest(t.subscription_start_d1, s.subscription_start_d1)
        , subscription_start_d7 = greatest(t.subscription_start_d7, s.subscription_start_d7)
        , subscription_start_d14 = greatest(t.subscription_start_d14, s.subscription_start_d14)
        , subscription_start_d30 = greatest(t.subscription_start_d30, s.subscription_start_d30)
        , funnel_onboarding_show_first_page_d1 = greatest(t.funnel_onboarding_show_first_page_d1, s.funnel_onboarding_show_first_page_d1)
        , funnel_onboarding_paywall_d1 = greatest(t.funnel_onboarding_paywall_d1, s.funnel_onboarding_paywall_d1)
        , onboarding_show_premium_d1 = greatest(t.onboarding_show_premium_d1, s.onboarding_show_premium_d1)
        , funnel_open_library_d1 = greatest(t.funnel_open_library_d1, s.funnel_open_library_d1)
        , funnel_edit_asset_d1 = greatest(t.funnel_edit_asset_d1, s.funnel_edit_asset_d1)
        , funnel_editor_done_d1 = greatest(t.funnel_editor_done_d1, s.funnel_editor_done_d1)
        , funnel_share_action_d1 = greatest(t.funnel_share_action_d1, s.funnel_share_action_d1)
        , f2_onboarding_paywall_d1 = greatest(t.f2_onboarding_paywall_d1, s.f2_onboarding_paywall_d1)
        , f2_show_premium_d1 = greatest(t.f2_show_premium_d1, s.f2_show_premium_d1)
        , f2_open_library_d1 = greatest(t.f2_open_library_d1, s.f2_open_library_d1)
        , f2_edit_asset_d1 = greatest(t.f2_edit_asset_d1, s.f2_edit_asset_d1)
        , f2_editor_done_d1 = greatest(t.f2_editor_done_d1, s.f2_editor_done_d1)
        , f2_share_action_d1 = greatest(t.f2_share_action_d1, s.f2_share_action_d1)
        , share_image_d7 = greatest(t.share_image_d7, s.share_image_d7)
        , share_collage_d7 = greatest(t.share_collage_d7, s.share_collage_d7)
        , share_video_d7 = greatest(t.share_video_d7, s.share_video_d7)
        , edit_filter_d1 = greatest(t.edit_filter_d1, s.edit_filter_d1)
        , edit_adjustments_d1 = greatest(t.edit_adjustments_d1, s.edit_adjustments_d1)
        , edit_border_size_d1 = greatest(t.edit_border_size_d1, s.edit_border_size_d1)
        , edit_border_d1 = greatest(t.edit_border_d1, s.edit_border_d1)
        , edit_crop_d1 = greatest(t.edit_crop_d1, s.edit_crop_d1)
        , edit_text_style_d1 = greatest(t.edit_text_style_d1, s.edit_text_style_d1)
        , edit_aspect_d1 = greatest(t.edit_aspect_d1, s.edit_aspect_d1)
        , edit_collage_d1 = greatest(t.edit_collage_d1, s.edit_collage_d1)
        , edit_premium_tools_d1 = greatest(t.edit_premium_tools_d1, s.edit_premium_tools_d1)
        , edit_text_d1 = greatest(t.edit_text_d1, s.edit_text_d1)
        , format_full_d1 = greatest(t.format_full_d1, s.format_full_d1)
        , format_ig_full_d1 = greatest(t.format_ig_full_d1, s.format_ig_full_d1)
        , format_ig_story_d1 = greatest(t.format_ig_story_d1, s.format_ig_story_d1)
        , format_instasize_d1 = greatest(t.format_instasize_d1, s.format_instasize_d1)
        , format_landscape_d1 = greatest(t.format_landscape_d1, s.format_landscape_d1)
        , paywall_editor_done_d1 = greatest(t.paywall_editor_done_d1, s.paywall_editor_done_d1)
        , paywall_deep_link_d1 = greatest(t.paywall_deep_link_d1, s.paywall_deep_link_d1)
        , paywall_app_launch_d1 = greatest(t.paywall_app_launch_d1, s.paywall_app_launch_d1)
        , paywall_onbarding_v3_d1 = greatest(t.paywall_onbarding_v3_d1, s.paywall_onbarding_v3_d1)
        , paywall_editor_d1 = greatest(t.paywall_editor_d1, s.paywall_editor_d1)
        , paywall_grid_d1 = greatest(t.paywall_grid_d1, s.paywall_grid_d1)
        , paywall_settings_d1 = greatest(t.paywall_settings_d1, s.paywall_settings_d1)
        , engagment_d1 = greatest(t.engagment_d1, s.engagment_d1)
        , engagment_d7 = greatest(t.engagment_d7, s.engagment_d7)
        , engagment_d30 = greatest(t.engagment_d30, s.engagment_d30)
        , retention_d2 = greatest(t.retention_d2, s.retention_d2)
        , retention_d3 = greatest(t.retention_d3, s.retention_d3)
        , retention_d7 = greatest(t.retention_d7, s.retention_d7)
        , retention_d30 = greatest(t.retention_d30, s.retention_d30)
        , retention_w2 = greatest(t.retention_w2, s.retention_w2)
        , retention_w4 = greatest(t.retention_w4, s.retention_w4)
        , updated_at = current_datetime()
        , updated_count = t.updated_count + 1
when not matched by target then
    insert row
;