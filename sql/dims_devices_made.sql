create or replace table dimensions.devices_made_installed
partition by installed_date
as
select
    device_vendor_id 
    , min(event_time) as installed_at
    , date(min(event_time)) as installed_date
from fastream.made_ios_prod
where
    event_date < current_date()
    and device_vendor_id is not null
    and installed_at >= datetime('2019-04-01')
group by 1
having installed_date >= date('2019-04-01')
;


-- drop table if exists dimensions.devices_made;
create or replace table dimensions.devices_made
partition by installed_date
cluster by country
options(
   description="dimension talbe for made devices"
   , require_partition_filter=false
)
as
with devices as (
select
	-- ids
	e.device_vendor_id
	, count(distinct e.idfa) as unq_idfa
	, max(e.idfa) as idfa
	, min(e.idfa) as idfa_min
	-- , count(distinct e.subscriber_id) as unq_subscriber_id
	-- , max(e.subscriber_id) as subscriber_id
	-- install-date
	, min(i.installed_at) as installed_at
	, min(i.installed_date) as installed_date
	, max(e.event_time) as last_event_at
	-- app versions
	, min(e.app_ver) as first_app_ver
	, max(e.app_ver) as latest_app_ver
	, count(distinct app_ver) as unq_app_ver
	-- meta-data
	, max(e.language) as language
	, max(e.country) as country
	, max(e.platform) as platform
	, max(e.model) as model
	, min(if(json_extract_scalar(json_meta, '$.tracker') is null, 'organic', json_extract_scalar(json_meta, '$.tracker'))) as source
	-- sessions
	, count(1) as event_count
	, count(distinct session_id) as sessions_count
	-- subscription status
	, min(case when e.premium_status = 'trial' then e.event_time else null end) as first_subscription_trial
	, min(case when e.premium_status = 'active' then e.event_time else null end) as first_subscription_start
	-- cohorted subscription status
	, sum(case when e.premium_status = 'trial' then 1 else 0 end) as subscription_trial_total
	, max(case when e.premium_status = 'trial' and e.event_time < datetime_add(i.installed_at, interval 1 day) then 1 else 0 end) as subscription_trial_d1
	, max(case when e.premium_status = 'trial' and e.event_time < datetime_add(i.installed_at, interval 7 day) then 1 else 0 end) as subscription_trial_d7
	, max(case when e.premium_status = 'trial' and e.event_time < datetime_add(i.installed_at, interval 30 day) then 1 else 0 end) as subscription_trial_d30
	, sum(case when e.premium_status = 'active' then 1 else 0 end) as subscription_start_total
	, max(case when e.premium_status = 'active' and e.event_time < datetime_add(i.installed_at, interval 1 day) then 1 else 0 end) as subscription_start_d1
	, max(case when e.premium_status = 'active' and e.event_time < datetime_add(i.installed_at, interval 7 day) then 1 else 0 end) as subscription_start_d7
	, max(case when e.premium_status = 'active' and e.event_time < datetime_add(i.installed_at, interval 14 day) then 1 else 0 end) as subscription_start_d14
	, max(case when e.premium_status = 'active' and e.event_time < datetime_add(i.installed_at, interval 30 day) then 1 else 0 end) as subscription_start_d30
	-- referral
	, sum(case when e.event_name = 'referral_tap' and e.event_time < datetime_add(i.installed_at, interval 1 day) then 1 else 0 end) as referral_tap_d1
	, sum(case when e.event_name = 'referral_tap' and e.event_time < datetime_add(i.installed_at, interval 7 day) then 1 else 0 end) as referral_tap_d7
	, sum(case when e.event_name = 'referral_invite_sent' and e.event_time < datetime_add(i.installed_at, interval 7 day) then 1 else 0 end) as referral_invite_sent_d7
	-- product funnel
	, max(case when e.event_name = 'hit_paywall' and e.event_time < datetime_add(i.installed_at, interval 1 day) then 1 else 0 end) as paywall_d1
	, max(case when e.event_name = 'editor_back' and e.event_time < datetime_add(i.installed_at, interval 1 day) then 1 else 0 end) as editor_save_d1
	, max(case when e.event_name = 'editor_opened' and e.event_time < datetime_add(i.installed_at, interval 1 day) then 1 else 0 end) as editor_opened_d1
	, max(case when e.event_name = 'editor_share' and e.event_time < datetime_add(i.installed_at, interval 1 day) then 1 else 0 end) as editor_share_d1
	, max(case when e.event_name = 'share_action' and e.event_time < datetime_add(i.installed_at, interval 1 day) then 1 else 0 end) as share_action_d1
	, max(case when e.event_name = 'new_story_tap' and e.event_time < datetime_add(i.installed_at, interval 1 day) then 1 else 0 end) as new_story_d1
	, max(case when e.event_name = 'add_filter_tap' and e.event_time < datetime_add(i.installed_at, interval 1 day) then 1 else 0 end) as add_filter_tap_d1
	, max(case when e.event_name = 'add_text_tap' and e.event_time < datetime_add(i.installed_at, interval 1 day) then 1 else 0 end) as add_text_tap_d1
	, max(case when e.event_name = 'plan_tap' and e.event_time < datetime_add(i.installed_at, interval 1 day) then 1 else 0 end) as plan_tap_d1
	, max(case when e.event_name = 'edit_adjustment_tap' and e.event_time < datetime_add(i.installed_at, interval 1 day) then 1 else 0 end) as edit_adjustment_tap_d1
	, max(case when e.event_name = 'new_page_tap' and e.event_time < datetime_add(i.installed_at, interval 1 day) then 1 else 0 end) as new_page_tap_d1
	, max(case when e.event_name = 'rearrange_action' and e.event_time < datetime_add(i.installed_at, interval 1 day) then 1 else 0 end) as rearrange_action_d1	
	-- paywall total
	, sum(case when e.event_name = 'hit_paywall' and e.event_time < datetime_add(i.installed_at, interval 1 day) then 1 else 0 end) as paywall_count_d1
	, sum(case when e.event_name = 'hit_paywall' and e.event_time < datetime_add(i.installed_at, interval 30 day) then 1 else 0 end) as paywall_count_d30
	-- engagment metric
	, sum(case when e.event_time < datetime_add(i.installed_at, interval 4 hour) and e.event_name in ('share_action') then 1 else 0 end) as engagment_h4
	, sum(case when e.event_time < datetime_add(i.installed_at, interval 1 day) and e.event_name in ('share_action') then 1 else 0 end) as engagment_d1
	, sum(case when e.event_time < datetime_add(i.installed_at, interval 7 day) and e.event_name in ('share_action') then 1 else 0 end) as engagment_d7
	, sum(case when e.event_time < datetime_add(i.installed_at, interval 30 day) and e.event_name in ('share_action') then 1 else 0 end) as engagment_d30
	-- video
	, max(case when e.event_time < datetime_add(i.installed_at, interval 1 day) 
			and e.event_name in ('story_page_data')
			and cast(json_extract_scalar(json_meta, '$.video_cells_count') as int64) > 0 then 1
			else 0 end) as video_story_d1
	, max(case when e.event_time < datetime_add(i.installed_at, interval 7 day) 
			and e.event_name in ('story_page_data')
			and cast(json_extract_scalar(json_meta, '$.video_cells_count') as int64) > 0 then 1
			else 0 end) as video_story_d7
	-- templates
	, max(case when e.event_time < datetime_add(i.installed_at, interval 3 day) 
			and e.event_name in ('story_page_data')
			and json_extract_scalar(json_meta, '$.template_package_name') in ('CLASSIC', '00001') then 1
			else 0 end) as template_classic_d3
	, max(case when e.event_time < datetime_add(i.installed_at, interval 3 day) 
			and e.event_name in ('story_page_data')
			and json_extract_scalar(json_meta, '$.template_package_name') = 'MADE' then 1
			else 0 end) as template_made_d3
	, max(case when e.event_time < datetime_add(i.installed_at, interval 3 day) 
			and e.event_name in ('story_page_data')
			and json_extract_scalar(json_meta, '$.template_package_name') = 'FILM' then 1
			else 0 end) as template_film_d3
	, max(case when e.event_time < datetime_add(i.installed_at, interval 3 day) 
			and e.event_name in ('story_page_data')
			and json_extract_scalar(json_meta, '$.template_package_name') = 'SNAPSHOT' then 1
			else 0 end) as template_snapshot_d3
	, max(case when e.event_time < datetime_add(i.installed_at, interval 3 day) 
			and e.event_name in ('story_page_data')
			and json_extract_scalar(json_meta, '$.template_package_name') = 'TRAVEL' then 1
			else 0 end) as template_travel_d3
	, max(case when e.event_time < datetime_add(i.installed_at, interval 3 day) 
			and e.event_name in ('story_page_data')
			and json_extract_scalar(json_meta, '$.template_package_name') = 'LAYERED' then 1
			else 0 end) as template_layered_d3
	, max(case when e.event_time < datetime_add(i.installed_at, interval 3 day) 
			and e.event_name in ('story_page_data')
			and json_extract_scalar(json_meta, '$.template_package_name') = 'TORN' then 1
			else 0 end) as template_tron_d3
	, max(case when e.event_time < datetime_add(i.installed_at, interval 3 day) 
			and e.event_name in ('story_page_data')
			and json_extract_scalar(json_meta, '$.template_package_name') = 'DEFINITION' then 1
			else 0 end) as template_definition_d3
	, max(case when e.event_time < datetime_add(i.installed_at, interval 3 day) 
			and e.event_name in ('story_page_data')
			and json_extract_scalar(json_meta, '$.template_package_name') = 'CLASSIC II' then 1
			else 0 end) as template_classic_2_d3
	, max(case when e.event_time < datetime_add(i.installed_at, interval 3 day) 
			and e.event_name in ('story_page_data')
			and json_extract_scalar(json_meta, '$.template_package_name') = 'MINIMAL' then 1
			else 0 end) as template_minimal_d3
	, max(case when e.event_time < datetime_add(i.installed_at, interval 3 day) 
			and e.event_name in ('story_page_data')
			and json_extract_scalar(json_meta, '$.template_package_name') = 'PRIDE' then 1
			else 0 end) as template_pride_d3
	, max(case when e.event_time < datetime_add(i.installed_at, interval 3 day) 
			and e.event_name in ('story_page_data')
			and json_extract_scalar(json_meta, '$.template_package_name') = 'GOLD' then 1
			else 0 end) as template_gold_d3
	-- filters
	, max(case when e.event_time < datetime_add(i.installed_at, interval 3 day) 
			and e.event_name in ('story_page_data')
			and json_extract_scalar(json_meta, '$.filter_name') = 'Hiro' then 1
			else 0 end) as filter_hiro_d3
	, max(case when e.event_time < datetime_add(i.installed_at, interval 3 day) 
			and e.event_name in ('story_page_data')
			and json_extract_scalar(json_meta, '$.filter_name') = 'Tiki' then 1
			else 0 end) as filter_tiki_d3
	, max(case when e.event_time < datetime_add(i.installed_at, interval 3 day) 
			and e.event_name in ('story_page_data')
			and json_extract_scalar(json_meta, '$.filter_name') = 'Coast' then 1
			else 0 end) as filter_coast_d3
	, max(case when e.event_time < datetime_add(i.installed_at, interval 3 day) 
			and e.event_name in ('story_page_data')
			and json_extract_scalar(json_meta, '$.filter_name') = 'Oak' then 1
			else 0 end) as filter_oak_d3
	, max(case when e.event_time < datetime_add(i.installed_at, interval 3 day) 
			and e.event_name in ('story_page_data')
			and json_extract_scalar(json_meta, '$.filter_name') = 'Radio' then 1
			else 0 end) as filter_radio_d3
	, max(case when e.event_time < datetime_add(i.installed_at, interval 3 day) 
			and e.event_name in ('story_page_data')
			and json_extract_scalar(json_meta, '$.filter_name') = 'Nova' then 1
			else 0 end) as filter_nova_d3
	, max(case when e.event_time < datetime_add(i.installed_at, interval 3 day) 
			and e.event_name in ('story_page_data')
			and json_extract_scalar(json_meta, '$.filter_name') = 'Bark' then 1
			else 0 end) as filter_bark_d3
	, max(case when e.event_time < datetime_add(i.installed_at, interval 3 day) 
			and e.event_name in ('story_page_data')
			and json_extract_scalar(json_meta, '$.filter_name') = 'Tokyo' then 1
			else 0 end) as filter_tokyo_d3
	, max(case when e.event_time < datetime_add(i.installed_at, interval 3 day) 
			and e.event_name in ('story_page_data')
			and json_extract_scalar(json_meta, '$.filter_name') = 'Kayak' then 1
			else 0 end) as filter_kayak_d3
	, max(case when e.event_time < datetime_add(i.installed_at, interval 3 day) 
			and e.event_name in ('story_page_data')
			and json_extract_scalar(json_meta, '$.filter_name') = '1989' then 1
			else 0 end) as filter_1098_d3
	, max(case when e.event_time < datetime_add(i.installed_at, interval 3 day) 
			and e.event_name in ('story_page_data')
			and json_extract_scalar(json_meta, '$.filter_name') = 'Lincoln' then 1
			else 0 end) as filter_lincoln_d3
	, max(case when e.event_time < datetime_add(i.installed_at, interval 3 day) 
			and e.event_name in ('story_page_data')
			and json_extract_scalar(json_meta, '$.filter_name') = 'Nomad' then 1
			else 0 end) as filter_nomad_d3
	, max(case when e.event_time < datetime_add(i.installed_at, interval 3 day) 
			and e.event_name in ('story_page_data')
			and json_extract_scalar(json_meta, '$.filter_name') = 'Celsius' then 1
			else 0 end) as filter_celsius_d3
	, max(case when e.event_time < datetime_add(i.installed_at, interval 3 day) 
			and e.event_name in ('story_page_data')
			and json_extract_scalar(json_meta, '$.filter_name') = 'Hula' then 1
			else 0 end) as filter_hula_d3
	, max(case when e.event_time < datetime_add(i.installed_at, interval 3 day) 
			and e.event_name in ('story_page_data')
			and json_extract_scalar(json_meta, '$.filter_name') = 'Baltic' then 1
			else 0 end) as filter_baltic_d3
	, max(case when e.event_time < datetime_add(i.installed_at, interval 3 day) 
			and e.event_name in ('story_page_data')
			and json_extract_scalar(json_meta, '$.filter_name') = 'Market' then 1
			else 0 end) as filter_market_d3
	, max(case when e.event_time < datetime_add(i.installed_at, interval 3 day) 
			and e.event_name in ('story_page_data')
			and json_extract_scalar(json_meta, '$.filter_name') = 'Flux' then 1
			else 0 end) as filter_flux_d3
	, max(case when e.event_time < datetime_add(i.installed_at, interval 3 day) 
			and e.event_name in ('story_page_data')
			and json_extract_scalar(json_meta, '$.filter_name') = 'Athens' then 1
			else 0 end) as filter_athens_d3
	, max(case when e.event_time < datetime_add(i.installed_at, interval 3 day) 
			and e.event_name in ('story_page_data')
			and json_extract_scalar(json_meta, '$.filter_name') = 'Newport' then 1
			else 0 end) as filter_newport_d3
	, max(case when e.event_time < datetime_add(i.installed_at, interval 3 day) 
			and e.event_name in ('story_page_data')
			and json_extract_scalar(json_meta, '$.filter_name') = 'Waves' then 1
			else 0 end) as filter_waves_d3
	-- upsplash
	, max(case when e.event_name in ('selection_camera_roll_tab_tap', 'selection_unsplash_tab_tap') and e.event_time < datetime_add(i.installed_at, interval 1 day) then 1 else 0 end) as unsplash_tap_d1
	, max(case when e.event_name = 'selection_unsplash_photo_selected' and e.event_time < datetime_add(i.installed_at, interval 1 day) then 1 else 0 end) as unsplash_used_d1
	, max(case when e.event_name in ('selection_camera_roll_tab_tap', 'selection_unsplash_tab_tap') and e.event_time < datetime_add(i.installed_at, interval 7 day) then 1 else 0 end) as unsplash_tap_d7
	, max(case when e.event_name = 'selection_unsplash_photo_selected' and e.event_time < datetime_add(i.installed_at, interval 7 day) then 1 else 0 end) as unsplash_used_d7
	-- daily retention
	, max(case when e.event_time >= datetime_add(i.installed_at, interval 1 day) and e.event_time < datetime_add(i.installed_at, interval 2 day) then 1 else 0 end) as retention_d2
	, max(case when e.event_time >= datetime_add(i.installed_at, interval 2 day) and e.event_time < datetime_add(i.installed_at, interval 3 day) then 1 else 0 end) as retention_d3
	, max(case when e.event_time >= datetime_add(i.installed_at, interval 6 day) and e.event_time < datetime_add(i.installed_at, interval 7 day) then 1 else 0 end) as retention_d7
	, max(case when e.event_time >= datetime_add(i.installed_at, interval 29 day) and e.event_time < datetime_add(i.installed_at, interval 30 day) then 1 else 0 end) as retention_d30
	-- weekly retention
	, max(case when e.event_time >= datetime_add(i.installed_at, interval 1 week) and e.event_time < datetime_add(i.installed_at, interval 2 week) then 1 else 0 end) as retention_w2
	, max(case when e.event_time >= datetime_add(i.installed_at, interval 2 week) and e.event_time < datetime_add(i.installed_at, interval 3 week) then 1 else 0 end) as retention_w3
	, max(case when e.event_time >= datetime_add(i.installed_at, interval 3 week) and e.event_time < datetime_add(i.installed_at, interval 4 week) then 1 else 0 end) as retention_w4
	, max(case when e.event_time >= datetime_add(i.installed_at, interval 4 week) and e.event_time < datetime_add(i.installed_at, interval 5 week) then 1 else 0 end) as retention_w5
	, max(case when e.event_time >= datetime_add(i.installed_at, interval 5 week) and e.event_time < datetime_add(i.installed_at, interval 6 week) then 1 else 0 end) as retention_w6
	, max(case when e.event_time >= datetime_add(i.installed_at, interval 6 week) and e.event_time < datetime_add(i.installed_at, interval 7 week) then 1 else 0 end) as retention_w7
	, max(case when e.event_time >= datetime_add(i.installed_at, interval 7 week) and e.event_time < datetime_add(i.installed_at, interval 8 week) then 1 else 0 end) as retention_w8
	-- monthly retention
	, max(case when e.event_time >= datetime_add(i.installed_at, interval 1 month) and e.event_time < datetime_add(i.installed_at, interval 2 month) then 1 else 0 end) as retention_m2
	, max(case when e.event_time >= datetime_add(i.installed_at, interval 2 month) and e.event_time < datetime_add(i.installed_at, interval 3 month) then 1 else 0 end) as retention_m3
	, max(case when e.event_time >= datetime_add(i.installed_at, interval 3 month) and e.event_time < datetime_add(i.installed_at, interval 4 month) then 1 else 0 end) as retention_m4
	, max(case when e.event_time >= datetime_add(i.installed_at, interval 4 month) and e.event_time < datetime_add(i.installed_at, interval 5 month) then 1 else 0 end) as retention_m5
	, max(case when e.event_time >= datetime_add(i.installed_at, interval 5 month) and e.event_time < datetime_add(i.installed_at, interval 6 month) then 1 else 0 end) as retention_m6
from fastream.made_ios_prod as e
join dimensions.devices_made_installed as i
	on e.device_vendor_id = i.device_vendor_id
where 
	e.event_date < current_date()	
group by 1
), devices_censored as (
select
	device_vendor_id
	, unq_idfa
	, idfa
	, idfa_min
	-- , unq_subscriber_id
	-- , subscriber_id
	-- attrabution
	, case when idfa = '00000000-0000-0000-0000-000000000000' then 'LAT enabled' else 'LAT disabled' end as lat_status
	-- install-date
	, installed_at
	, installed_date
	, last_event_at
	-- subscription firsts
	, first_subscription_trial
	, first_subscription_start
	-- app versions
	, first_app_ver
	, latest_app_ver
	, unq_app_ver
	-- meta-data
	, language
	, country
	, platform
	, model
	, source
	-- sessions/events 
	, event_count
	, sessions_count
	-- subscritpion status
	, subscription_trial_total
	, if(datetime_add(installed_at, interval 1 day) < current_datetime(), subscription_trial_d1, null) as subscription_trial_d1
	, if(datetime_add(installed_at, interval 7 day) < current_datetime(), subscription_trial_d7, null) as subscription_trial_d7
	, if(datetime_add(installed_at, interval 30 day) < current_datetime(), subscription_trial_d30, null) as subscription_trial_d30
	, subscription_start_total
	, if(datetime_add(installed_at, interval 1 day) < current_datetime(), subscription_start_d1, null) as subscription_start_d1
	, if(datetime_add(installed_at, interval 7 day) < current_datetime(), subscription_start_d7, null) as subscription_start_d7
	, if(datetime_add(installed_at, interval 14 day) < current_datetime(), subscription_start_d14, null) as subscription_start_d14
	, if(datetime_add(installed_at, interval 30 day) < current_datetime(), subscription_start_d30, null) as subscription_start_d30
	-- referral
	, if(datetime_add(installed_at, interval 1 day) < current_datetime(), referral_tap_d1, null) as referral_tap_d1
	, if(datetime_add(installed_at, interval 7 day) < current_datetime(), referral_invite_sent_d7, null) as referral_invite_sent_d7
	-- funnel
	, if(datetime_add(installed_at, interval 1 day) < current_datetime(), paywall_d1, null) as paywall_d1
	, if(datetime_add(installed_at, interval 1 day) < current_datetime(), editor_opened_d1, null) as editor_opened_d1
	, if(datetime_add(installed_at, interval 1 day) < current_datetime(), editor_save_d1, null) as editor_save_d1
	, if(datetime_add(installed_at, interval 1 day) < current_datetime(), editor_share_d1, null) as editor_share_d1
	, if(datetime_add(installed_at, interval 1 day) < current_datetime(), share_action_d1, null) as share_action_d1
	, if(datetime_add(installed_at, interval 1 day) < current_datetime(), new_story_d1, null) as new_story_d1
	, if(datetime_add(installed_at, interval 1 day) < current_datetime(), add_filter_tap_d1, null) as add_filter_tap_d1
	, if(datetime_add(installed_at, interval 1 day) < current_datetime(), add_text_tap_d1, null) as add_text_tap_d1
	, if(datetime_add(installed_at, interval 1 day) < current_datetime(), plan_tap_d1, null) as plan_tap_d1
	, if(datetime_add(installed_at, interval 1 day) < current_datetime(), edit_adjustment_tap_d1, null) as edit_adjustment_tap_d1
	, if(datetime_add(installed_at, interval 1 day) < current_datetime(), rearrange_action_d1, null) as rearrange_action_d1
	, if(datetime_add(installed_at, interval 1 day) < current_datetime(), new_page_tap_d1, null) as new_page_tap_d1
	-- paywall total
	, if(datetime_add(installed_at, interval 1 day) < current_datetime(), paywall_count_d1, null) as paywall_count_d1
	, if(datetime_add(installed_at, interval 31 day) < current_datetime(), paywall_count_d30, null) as paywall_count_d30
	-- early user engagment
	, if(datetime_add(installed_at, interval 4 hour) < current_datetime(), engagment_h4, null) as engagment_h4
	, if(datetime_add(installed_at, interval 1 day) < current_datetime(), engagment_d1, null) as engagment_d1
	, if(datetime_add(installed_at, interval 7 day) < current_datetime(), engagment_d7, null) as engagment_d7
	, if(datetime_add(installed_at, interval 30 day) < current_datetime(), engagment_d30, null) as engagment_d30
	-- video
	, if(datetime_add(installed_at, interval 1 day) < current_datetime(), video_story_d1, null) as video_story_d1
	, if(datetime_add(installed_at, interval 7 day) < current_datetime(), video_story_d7, null) as video_story_d7
	-- templates
	, if(datetime_add(installed_at, interval 3 day) < current_datetime(), template_classic_d3, null) as template_classic_d3
	, if(datetime_add(installed_at, interval 3 day) < current_datetime(), template_film_d3, null) as template_film_d3
	, if(datetime_add(installed_at, interval 3 day) < current_datetime(), template_snapshot_d3, null) as template_snapshot_d3
	, if(datetime_add(installed_at, interval 3 day) < current_datetime(), template_travel_d3, null) as template_travel_d3
	, if(datetime_add(installed_at, interval 3 day) < current_datetime(), template_layered_d3, null) as template_layered_d3
	, if(datetime_add(installed_at, interval 3 day) < current_datetime(), template_tron_d3, null) as template_tron_d3
	, if(datetime_add(installed_at, interval 3 day) < current_datetime(), template_definition_d3, null) as template_definition_d3
	, if(datetime_add(installed_at, interval 3 day) < current_datetime(), template_classic_2_d3, null) as template_classic_2_d3
	, if(datetime_add(installed_at, interval 3 day) < current_datetime(), template_minimal_d3, null) as template_minimal_d3
	, if(datetime_add(installed_at, interval 3 day) < current_datetime(), template_pride_d3, null) as template_pride_d3
	, if(datetime_add(installed_at, interval 3 day) < current_datetime(), template_gold_d3, null) as template_gold_d3
	-- filters
	, if(datetime_add(installed_at, interval 3 day) < current_datetime(), filter_hiro_d3, null) as filter_hiro_d3
	, if(datetime_add(installed_at, interval 3 day) < current_datetime(), filter_tiki_d3, null) as filter_tiki_d3
	, if(datetime_add(installed_at, interval 3 day) < current_datetime(), filter_coast_d3, null) as filter_coast_d3
	, if(datetime_add(installed_at, interval 3 day) < current_datetime(), filter_oak_d3, null) as filter_oak_d3
	, if(datetime_add(installed_at, interval 3 day) < current_datetime(), filter_radio_d3, null) as filter_radio_d3
	, if(datetime_add(installed_at, interval 3 day) < current_datetime(), filter_nova_d3, null) as filter_nova_d3
	, if(datetime_add(installed_at, interval 3 day) < current_datetime(), filter_bark_d3, null) as filter_bark_d3
	, if(datetime_add(installed_at, interval 3 day) < current_datetime(), filter_tokyo_d3, null) as filter_tokyo_d3
	, if(datetime_add(installed_at, interval 3 day) < current_datetime(), filter_kayak_d3, null) as filter_kayak_d3
	, if(datetime_add(installed_at, interval 3 day) < current_datetime(), filter_1098_d3, null) as filter_1098_d3
	, if(datetime_add(installed_at, interval 3 day) < current_datetime(), filter_lincoln_d3, null) as filter_lincoln_d3
	, if(datetime_add(installed_at, interval 3 day) < current_datetime(), filter_nomad_d3, null) as filter_nomad_d3
	, if(datetime_add(installed_at, interval 3 day) < current_datetime(), filter_celsius_d3, null) as filter_celsius_d3
	, if(datetime_add(installed_at, interval 3 day) < current_datetime(), filter_hula_d3, null) as filter_hula_d3
	, if(datetime_add(installed_at, interval 3 day) < current_datetime(), filter_baltic_d3, null) as filter_baltic_d3
	, if(datetime_add(installed_at, interval 3 day) < current_datetime(), filter_market_d3, null) as filter_market_d3
	, if(datetime_add(installed_at, interval 3 day) < current_datetime(), filter_flux_d3, null) as filter_flux_d3
	, if(datetime_add(installed_at, interval 3 day) < current_datetime(), filter_athens_d3, null) as filter_athens_d3
	, if(datetime_add(installed_at, interval 3 day) < current_datetime(), filter_newport_d3, null) as filter_newport_d3
	, if(datetime_add(installed_at, interval 3 day) < current_datetime(), filter_waves_d3, null) as filter_waves_d3
	-- unsplash
	, if(datetime_add(installed_at, interval 1 day) < current_datetime(), unsplash_tap_d1, null) as unsplash_tap_d1
	, if(datetime_add(installed_at, interval 1 day) < current_datetime(), unsplash_used_d1, null) as unsplash_used_d1
	, if(datetime_add(installed_at, interval 7 day) < current_datetime(), unsplash_tap_d7, null) as unsplash_tap_d7
	, if(datetime_add(installed_at, interval 7 day) < current_datetime(), unsplash_used_d7, null) as unsplash_used_d7
	-- daily retention
	, if(datetime_add(installed_at, interval 2 day) < current_datetime(), retention_d2, null) as retention_d2
	, if(datetime_add(installed_at, interval 3 day) < current_datetime(), retention_d3, null) as retention_d3
	, if(datetime_add(installed_at, interval 7 day) < current_datetime(), retention_d7, null) as retention_d7
	, if(datetime_add(installed_at, interval 30 day) < current_datetime(), retention_d30, null) as retention_d30
	-- weekly
	, if(datetime_add(installed_at, interval 2 week) < current_datetime(), retention_w2, null) as retention_w2
	, if(datetime_add(installed_at, interval 3 week) < current_datetime(), retention_w3, null) as retention_w3
	, if(datetime_add(installed_at, interval 4 week) < current_datetime(), retention_w4, null) as retention_w4
	, if(datetime_add(installed_at, interval 5 week) < current_datetime(), retention_w5, null) as retention_w5
	, if(datetime_add(installed_at, interval 6 week) < current_datetime(), retention_w6, null) as retention_w6
	, if(datetime_add(installed_at, interval 7 week) < current_datetime(), retention_w7, null) as retention_w7
	, if(datetime_add(installed_at, interval 8 week) < current_datetime(), retention_w8, null) as retention_w8
	-- monthly
	, if(datetime_add(installed_at, interval 2 month) < current_datetime(), retention_m2, null) as retention_m2
	, if(datetime_add(installed_at, interval 3 month) < current_datetime(), retention_m3, null) as retention_m3
	, if(datetime_add(installed_at, interval 4 month) < current_datetime(), retention_m4, null) as retention_m4
	, if(datetime_add(installed_at, interval 5 month) < current_datetime(), retention_m5, null) as retention_m5
	, if(datetime_add(installed_at, interval 6 month) < current_datetime(), retention_m6, null) as retention_m6
from devices
)
select *
from devices_censored
;