create or replace table dimensions.attribution_instasize
as
with att_devices as (
select *
from fastream.instasize_api_sql_prod_attribution_events
where 
  attribution is true
  and campaign_id <> 1234567890
  and ad_group_id <> 1234567890
  and (keyword_id <> 12323222 or keyword_id is null)
qualify 
  row_number() over(partition by device_uid order by created_at) = 1
)
select
  coalesce(d.device_vendor_id, lower(a.device_uid)) as device_vendor_id
  , d.installed_at
  , d.country
  , d.subscriber_id
  , d.trial_started_at
  , d.paid_started_at
  , a.campaign_id
  , a.ad_group_id as adgroup_id
  , a.keyword_id
  , datetime(a.created_at) as created_at
  , datetime(a.updated_at) as updated_at
  , datetime(a.click_date) as click_date
from att_devices as a
left outer join dimensions.devices_instasize as d
  on(lower(a.device_uid) = d.device_vendor_id)
;