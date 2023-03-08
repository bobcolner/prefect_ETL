create or replace table adhoc.devices_instasize
partition by installed_date
as
select 
    *
    , current_datetime() as updated_at
    , 1 as updated_count
from adhoc.devices_instasize_stg
;

create or replace table dimensions.devices_metrics_instasize_20220928
as
select * from dimensions.devices_metrics_instasize
;

create table dimensions.devices_metrics_instasize as
select 
    * 
    , datetime('2022-09-28') as updated_at
    , 1 as updated_count
from dimensions.dimensions.devices_metrics_instasize_20220928
;


select count(1) as total
from dimensions.devices_metrics_instasize as t
full outer join dimensions.devices_metrics_instasize_stg as s
    using(device_vendor_id)
where 
    -- t.device_vendor_id is null
    t.device_vendor_id is not null and s.device_vendor_id is not nulla
;

drop table adhoc.devices_instasize;
create table adhoc.devices_instasize
as 
select * from adhoc.devices_instasize_stg
;

select count(1) from dimensions.devices_install_instasize;

create or replace table dimensions.devices_install_instasize
as 
select 
    device_vendor_id
    , installed_at
    , onboarding_at
    , trial_started_at
    , paid_started_at
    , updated_at
    , updated_count
from dimensions.devices_install_instasize
;

select count(1)
from dimensions.devices_install_instasize as t
full outer join dimensions.devices_install_instasize_stg as s
    using(device_vendor_id)
where t.device_vendor_id is null
;


select 
    updated_count
    , count(1) as t
from dimensions.devices_install_instasize
-- from adhoc.devices_instasize
group by 1
order by 1


when not matched by target then
    insert(device_vendor_id, installed_at, onboarding_at, trial_started_at, paid_started_at, idfa, subscriber_id, country, session_sec_sum_d1, session_sec_count_d1, subscription_trial_d1, subscription_trial_d7, subscription_start_d1, subscription_start_d7, subscription_start_d14, subscription_start_d30, funnel_onboarding_show_first_page_d1, funnel_onboarding_paywall_d1, onboarding_show_premium_d1, funnel_open_library_d1, funnel_edit_asset_d1, funnel_editor_done_d1, funnel_share_action_d1, f2_onboarding_paywall_d1, f2_show_premium_d1, f2_open_library_d1, f2_edit_asset_d1, f2_editor_done_d1, f2_share_action_d1, share_image_d7, share_collage_d7, share_video_d7, edit_filter_d1, edit_adjustments_d1, edit_border_size_d1, edit_border_d1, edit_crop_d1, edit_text_style_d1, edit_aspect_d1, edit_collage_d1, edit_premium_tools_d1, edit_text_d1, format_full_d1, format_ig_full_d1, format_ig_story_d1, format_instasize_d1, format_landscape_d1, paywall_editor_done_d1, paywall_deep_link_d1, paywall_app_launch_d1, paywall_onbarding_v3_d1, paywall_editor_d1, paywall_grid_d1, paywall_settings_d1, engagment_h4, engagment_d1, engagment_d7, engagment_d30, retention_d1, retention_d2, retention_d3, retention_d7, retention_d30, retention_m3, retention_m6, updated_at, updated_count)
    values(device_vendor_id, installed_at, onboarding_at, trial_started_at, paid_started_at, idfa, subscriber_id, country, session_sec_sum_d1, session_sec_count_d1, subscription_trial_d1, subscription_trial_d7, subscription_start_d1, subscription_start_d7, subscription_start_d14, subscription_start_d30, funnel_onboarding_show_first_page_d1, funnel_onboarding_paywall_d1, onboarding_show_premium_d1, funnel_open_library_d1, funnel_edit_asset_d1, funnel_editor_done_d1, funnel_share_action_d1, f2_onboarding_paywall_d1, f2_show_premium_d1, f2_open_library_d1, f2_edit_asset_d1, f2_editor_done_d1, f2_share_action_d1, share_image_d7, share_collage_d7, share_video_d7, edit_filter_d1, edit_adjustments_d1, edit_border_size_d1, edit_border_d1, edit_crop_d1, edit_text_style_d1, edit_aspect_d1, edit_collage_d1, edit_premium_tools_d1, edit_text_d1, format_full_d1, format_ig_full_d1, format_ig_story_d1, format_instasize_d1, format_landscape_d1, paywall_editor_done_d1, paywall_deep_link_d1, paywall_app_launch_d1, paywall_onbarding_v3_d1, paywall_editor_d1, paywall_grid_d1, paywall_settings_d1, engagment_h4, engagment_d1, engagment_d7, engagment_d30, retention_d1, retention_d2, retention_d3, retention_d7, retention_d30, retention_m3, retention_m6, current_datetime(), 1)

    insert(device_vendor_id, installed_at, onboarding_at, trial_started_at, paid_started_at, updated_at, updated_count)
    values(device_vendor_id, installed_at, onboarding_at, trial_started_at, paid_started_at, current_datetime(), 1)

#standardSQL UDFs
create temp function least_array(arr any type) as ((
    select min(a) from unnest(arr) a where a is not null
));
create temp function greatest_array(arr any type) as ((
    select max(a) from unnest(arr) a where a is not null
));
    
select
  datetime(TIMESTAMP_SECONDS(LEAST_ARRAY([UNIX_SECONDS(timestamp(datetime("2022-02-01"))), null]))) as val
  , LEAST_ARRAY([NULL, NULL]) as nulltest
  , LEAST_ARRAY([datetime("2022-02-01"), NULL]) as nulltest2
  , LEAST_ARRAY([datetime("2022-02-01"), datetime("2022-03-01")]) as nulltest3
  