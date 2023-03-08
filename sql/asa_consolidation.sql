create or replace table apple_search_ads.campaigns_fs 
as
select 
  cast(date as string) as spend_date
  , cast(orgId as int) as org_id
  , app.appName as app_name
  , cast(campaignId as int) as campaign_id
  , campaignName as campaign_name
  , modificationTime as modification_at
  , _metadata_file_modified_at as inserted_at
  , cast(localSpend.amount as float64) as spend_amount
  , cast(impressions as int) as impressions
  , cast(taps as int) as taps
  , cast(installs as int) as installs
  , countriesOrRegions[offset(0)] as country
from fastream.apple_search_ads_v4_campaigns
;

create or replace table apple_search_ads.adgroups_fs 
as
select 
  cast(date as string) as spend_date
  , cast(orgId as int) as org_id
  , cast(campaignId as int) as campaign_id
  , cast(adgroupId as int) as adgroup_id
  , adGroupName as adgroup_name
  , modificationTime as modification_at
  , _metadata_file_modified_at as inserted_at
  , cast(localSpend.amount as float64) as spend_amount
  , cast(impressions as int) as impressions
  , cast(taps as int) as taps
  , cast(installs as int) as installs
from fastream.apple_search_ads_v4_adgroups
;

create or replace table apple_search_ads.keywords_fs 
as
select 
  cast(date as string) as spend_date
  , cast(adgroupId as int) as adgroup_id
  , cast(keywordId as int) as keyword_id
  , keyword
  , modificationTime as modification_at
  , _metadata_file_modified_at as inserted_at
  , cast(localSpend.amount as float64) as spend_amount
  , cast(impressions as int) as impressions
  , cast(taps as int) as taps
  , cast(installs as int) as installs
from fastream.apple_search_ads_v4_keywords
;

create or replace table apple_search_ads.campaigns_batch_dedup
as
select 
  *
  , current_datetime() as consolidation_at
from apple_search_ads.campaigns_batch
qualify row_number() over(partition by org_id, campaign_id, spend_date order by inserted_at desc) = 1
;

create or replace table apple_search_ads.adgroups_batch_dedup
as
select 
  *
  , current_datetime() as consolidation_at
from apple_search_ads.adgroups_batch
qualify row_number() over(partition by org_id, campaign_id, adgroup_id, spend_date order by inserted_at desc) = 1
;

create or replace table apple_search_ads.keywords_batch_dedup
as
select 
  *
  , current_datetime() as consolidation_at
from apple_search_ads.keywords_batch
qualify row_number() over(partition by org_id, campaign_id, adgroup_id, keyword_id, spend_date order by inserted_at desc) = 1
;


-- part dux
create or replace table apple_search_ads.campaigns_stream_dedup
as
select 
  *
  , current_datetime() as consolidation_at
from apple_search_ads.campaigns_stream
qualify row_number() over(partition by org_id, campaign_id, spend_date order by inserted_at desc) = 1
;

create or replace table apple_search_ads.adgroups_stream_dedup
as
select 
  *
  , current_datetime() as consolidation_at
from apple_search_ads.adgroups_stream
qualify row_number() over(partition by org_id, campaign_id, adgroup_id, spend_date order by inserted_at desc) = 1
;

create or replace table apple_search_ads.keywords_stream_dedup
as
select 
  *
  , current_datetime() as consolidation_at
from apple_search_ads.keywords_stream
qualify row_number() over(partition by org_id, campaign_id, adgroup_id, keyword_id, spend_date order by inserted_at desc) = 1
;

-- merge with new prefect batch tables
create or replace table apple_search_ads.campaigns_merged as
select
  date(coalesce(n.spend_date, o.spend_date)) as spend_date
  , coalesce(n.org_id, o.org_id) as org_id
  , coalesce(n.app_name, o.app_name) as app_name
  , coalesce(n.campaign_id, o.campaign_id) as campaign_id
  , coalesce(n.campaign_name, o.campaign_name) as campaign_name
  , coalesce(n.spend_amount, o.spend_amount) as spend_amount
  , coalesce(n.installs, o.installs) as installs
from apple_search_ads.campaigns_stream_dedup as o
full outer join apple_search_ads.campaigns_batch_dedup as n
  using(spend_date, org_id, campaign_id)
;

create or replace table apple_search_ads.adgroups_merged as
select
  date(coalesce(n.spend_date, o.spend_date)) as spend_date
  , coalesce(n.org_id, o.org_id) as org_id
  , coalesce(n.campaign_id, o.campaign_id) as campaign_id
  , coalesce(n.adgroup_id, o.adgroup_id) as adgroup_id
  , coalesce(n.adgroup_name, o.adgroup_name) as adgroup_name
  , coalesce(n.spend_amount, o.spend_amount) as spend_amount
  , coalesce(n.installs, o.installs) as installs
from apple_search_ads.adgroups_stream_dedup as o
full outer join apple_search_ads.adgroups_batch_dedup as n
  using(spend_date, org_id, campaign_id, adgroup_id)
;

create or replace table apple_search_ads.keywords_merged as
select
  date(coalesce(n.spend_date, o.spend_date)) as spend_date
  , coalesce(n.org_id, o.org_id) as org_id
  , coalesce(n.campaign_id, o.campaign_id) as campaign_id
  , coalesce(n.adgroup_id, o.adgroup_id) as adgroup_id
  , coalesce(n.keyword_id, o.keyword_id) as keyword_id
  , coalesce(n.keyword, o.keyword) as keyword
  , coalesce(n.spend_amount, o.spend_amount) as spend_amount
  , coalesce(n.installs, o.installs) as installs
from apple_search_ads.keywords_stream_dedup as o
full outer join apple_search_ads.keywords_batch_dedup as n
  using(spend_date, org_id, campaign_id, adgroup_id, keyword_id)
;


-- fastream <> etl-flow
create or replace table apple_search_ads.campaigns as
select
  coalesce(n.spend_date, o.spend_date) as spend_date
  , coalesce(n.org_id, o.org_id) as org_id
  , coalesce(n.app_name, o.app_name) as app_name
  , coalesce(n.campaign_id, o.campaign_id) as campaign_id
  , coalesce(n.campaign_name, o.campaign_name) as campaign_name
  , coalesce(n.spend_amount, o.spend_amount) as spend_amount
  , coalesce(n.installs, o.installs) as installs
from apple_search_ads.campaigns_fs as o
full outer join apple_search_ads.campaigns_batch_dedup as n
  using(spend_date, org_id, campaign_id)
;

create or replace table apple_search_ads.adgroups as
select
  coalesce(n.spend_date, o.spend_date) as spend_date
  , coalesce(n.org_id, o.org_id) as org_id
  , coalesce(n.campaign_id, o.campaign_id) as campaign_id
  , coalesce(n.adgroup_id, o.adgroup_id) as adgroup_id
  , coalesce(n.adgroup_name, o.adgroup_name) as adgroup_name
  , coalesce(n.spend_amount, o.spend_amount) as spend_amount
  , coalesce(n.installs, o.installs) as installs
from apple_search_ads.adgroups_fs as o
full outer join apple_search_ads.adgroups_batch_dedup as n
    using(spend_date, org_id, campaign_id, adgroup_id)
;

create or replace table apple_search_ads.keywords as
select
  coalesce(n.spend_date, o.spend_date) as spend_date
  , coalesce(n.adgroup_id, o.adgroup_id) as adgroup_id
  , coalesce(n.keyword_id, o.keyword_id) as keyword_id
  , coalesce(n.keyword, o.keyword) as keyword
  , coalesce(n.spend_amount, o.spend_amount) as spend_amount
  , coalesce(n.installs, o.installs) as installs
from apple_search_ads.keywords_fs as o
full outer join apple_search_ads.keywords_batch_dedup as n
  using(spend_date, adgroup_id, keyword_id)
;

-- valid coombos
create or replace table apple_search_ads.spend as
with campaigns as (
select
  campaign_id
  , app_name
from apple_search_ads.campaigns
group by 1, 2
), adgroups as (
select
  campaign_id
  , adgroup_id 
from apple_search_ads.adgroups
group by 1, 2
), keywords as (
select 
  spend_date
  , adgroup_id
  , keyword_id
  , sum(spend_amount) as spend
  , sum(installs) as installs
from apple_search_ads.keywords
group by 1, 2, 3
)
select
  cast(k.spend_date as date) as spend_date
  , c.app_name
  , c.campaign_id
  , a.adgroup_id
  , k.keyword_id
  , ifnull(k.spend, 0) as spend
  , ifnull(k.installs, 0) as installs
from campaigns as c
left join adgroups as a
  using(campaign_id)
left join keywords as k
  using(adgroup_id)
order by spend_date desc, app_name, campaign_id, adgroup_id, spend desc
;

-- v2 spend
-- with campaigns as (
-- select
--   app_name
--   , spend_date
--   , campaign_id
--   , sum(spend_amount) as spend
--   , sum(installs) as installs
-- from apple_search_ads.campaigns
-- group by 1, 2, 3
-- ), adgroups as (
-- select
--   spend_date
--   , campaign_id
--   , adgroup_id
--   , sum(spend_amount) as spend
--   , sum(installs) as installs
-- from apple_search_ads.adgroups
-- group by 1, 2, 3
-- ), keywords as (
-- select 
--   spend_date
--   , adgroup_id
--   , keyword_id
--   , sum(spend_amount) as spend
--   , sum(installs) as installs
-- from apple_search_ads.keywords
-- group by 1, 2, 3
-- )
-- select
--   c.app_name
--   , coalesce(c.spend_date, a.spend_date) as spend_date
--   , c.campaign_id
--   , c.spend as spend_camapign
--   , c.installs as installs_camapign
--   , coalesce(a.adgroup_id, k.adgroup_id) as adgroup_id
--   , a.spend as spend_adgroup
--   , a.installs as installs_adgroup
--   , k.keyword_id
--   , k.spend as spend_keyword
--   , k.installs as installs_keyword
-- from campaigns as c
-- left join adgroups as a
--   using(spend_date, campaign_id)
-- left join keywords as k
--   using(spend_date, adgroup_id)
-- order by app_name, spend_date desc, campaign_id, adgroup_id, spend_keyword desc
-- ;

-- names lookup table
create or replace table apple_search_ads.names
as
with campaigns as (
select
  org_id
  , campaign_id
  , campaign_name
  , max(spend_date) as latest_date
from apple_search_ads.campaigns
group by 1, 2, 3
qualify row_number() over(partition by org_id, campaign_id order by latest_date desc) = 1
), adgroups as (
select
  campaign_id
  , adgroup_id
  , adgroup_name
  , max(spend_date) as latest_date
from apple_search_ads.adgroups
group by 1, 2, 3
qualify row_number() over(partition by campaign_id, adgroup_id order by latest_date desc) = 1
), keywords as (
select
  adgroup_id
  , keyword_id
  , keyword
  , max(spend_date) as latest_date
from apple_search_ads.keywords
group by 1, 2, 3
qualify row_number() over(partition by adgroup_id, keyword_id order by latest_date desc) = 1
)
select
  c.org_id
  , c.campaign_id
  , c.campaign_name
  , a.adgroup_id
  , a.adgroup_name
  , k.keyword_id
  , k.keyword
from campaigns as c
left outer join adgroups as a
  using(campaign_id)
left outer join keywords as k
  using(adgroup_id)
;
