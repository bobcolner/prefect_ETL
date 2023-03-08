create or replace table dimensions.instasize_ios_props_stg
as
with events as (
select
    date(event_date) as event_date
    , event_name
    , device_vendor_id
    , json_meta
-- from fastream.instasize_ios_prod tablesample system (20 percent)
from fastream.instasize_ios_prod
where 
  date(event_date) between 
    date_add(current_date(), interval - 3 day)
    and date_add(current_date(), interval - 1 day)
    -- '2022-09-20' and '2022-09-22'
), json_agg as (
select
    event_date
    , event_name
    , property
    , value
    , count(1) as total
    , count(distinct device_vendor_id) as unq_devices
    -- , approx_count_distinct(device_vendor_id) as unq_devices
from 
    events
    , unnest([struct(json_extract(json_meta, '$.') as json_obj)])
    , unnest(bqutil.fn.json_extract_keys(json_obj)) property with offset
join unnest(bqutil.fn.json_extract_values(json_obj)) value with offset
    using (offset)
where 
    property not in('memory_size', 'frame', 'background_filter', 'Album Name')
    and not ends_with(lower(property), 'id')
    and not starts_with(value, '/var/mobile/')
group by 1, 2, 3, 4
), eg_prop_value as (
select 
  event_name
  , property
  , max(value) as max_value
from json_agg
group by 1, 2
), infer_type as (
select
  *
  , case when safe_cast(max_value as timestamp) is not null then 'date/time'
        when safe_cast(max_value as numeric) is not null then 'numeric'
        else 'category' end as value_type
from eg_prop_value         
), json_agg_type as (
select 
  j.*
  , t.value_type
from json_agg j
join infer_type as t
  using(event_name, property)
), final_union as (
select 
  event_date
  , event_name
  , property
  , 'numeric_data' as value
  , max(value_type) as value_type
  , sum(total) as total
  , sum(unq_devices) as unq_devices
  , min(cast(value as numeric)) as min_value
  , avg(cast(value as numeric)) as avg_value
  , stddev(cast(value as numeric)) as std_value
  , max(cast(value as numeric)) as max_value
from json_agg_type
where value_type = 'numeric'
group by 1, 2, 3

union all

select 
  event_date
  , event_name
  , property
  , value
  , value_type
  , total
  , unq_devices
  , null as min_value
  , null as avg_value
  , null as std_value
  , null as max_value
from json_agg_type
where value_type = 'category'
)
select *
from final_union
;


merge into dimensions.instasize_ios_props as t
using dimensions.instasize_ios_props_stg as s
    on(t.event_date = s.event_date
      and t.event_name = s.event_name
      and t.property = s.property
      and t.value = s.value)
when matched then
    update set 
      total = s.total
      , unq_devices = s.unq_devices
      , min_value = s.min_value
      , avg_value = s.avg_value
      , std_value = s.std_value
      , max_value = s.max_value
when not matched by target then
    insert row
;


select 
  event_date
  , count(1) as total
from dimensions.instasize_ios_props
group by 1
order by 1
;

create or replace table dimensions.instasize_ios_props
as 
select * from dimensions.instasize_ios_props_stg
;