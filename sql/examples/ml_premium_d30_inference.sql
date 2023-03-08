create or replace table ml.premium_d30_inference
as
select
  device_vendor_id
  , installed_date
  , language
  , model
  -- , event_date
  , device_age
  , active_days as active_day
  , total_events as events_cumsum
  , subscription_trial_total as trial_event_cumsum
  , hit_paywall_count as hit_paywall_cumsum
  , edit_asset_count as edit_asset_cumsum
  , editor_done_taps_count as editor_done_taps_cumsum
  , share_action_count as share_action_cumsum
from dimensions.devices_instasize
where 
  device_age >= 0
  and device_age <= 30
