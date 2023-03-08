drop table if exists dimensions.app_country_pst_installs;

create table dimensions.app_country_pst_installs
partition by installed_date
cluster by app_name, country
options(
   description="dimension app-country installs in PST timezone"
   , require_partition_filter=false
)
as
select
  date(datetime(cast(installed_at as timestamp), "America/Los_Angeles")) as installed_date
  , 'instasize' as app_name
  , country
  , count(1) as installs
from dimensions.devices_instasize
where 
  datetime(cast(installed_at as timestamp), "America/Los_Angeles") >= '2020-08-01'
  and datetime(cast(installed_at as timestamp), "America/Los_Angeles") < current_date()
group by 1, 2, 3

union all

select
  date(datetime(cast(installed_at as timestamp), "America/Los_Angeles")) as installed_date
  , 'made' as app_name
  , country
  , count(1) as installs
from dimensions.devices_made
where 
  datetime(cast(installed_at as timestamp), "America/Los_Angeles") >= '2020-08-01'
  and datetime(cast(installed_at as timestamp), "America/Los_Angeles") < current_date()
group by 1, 2, 3

union all

select
  date(datetime(cast(installed_at as timestamp), "America/Los_Angeles")) as installed_date
  , 'selfiemade' as app_name
  , country
  , count(1) as installs
from dimensions.devices_selfiemade
where 
  datetime(cast(installed_at as timestamp), "America/Los_Angeles") >= '2020-08-01'
  and datetime(cast(installed_at as timestamp), "America/Los_Angeles") < current_date()
group by 1, 2, 3

union all

select
  date(datetime(cast(installed_at as timestamp), "America/Los_Angeles")) as installed_date
  , 'typeloop' as app_name
  , country
  , count(1) as installs
from dimensions.devices_typeloop
where 
  datetime(cast(installed_at as timestamp), "America/Los_Angeles") >= '2020-08-01'
  and datetime(cast(installed_at as timestamp), "America/Los_Angeles") < current_date()
group by 1, 2, 3
