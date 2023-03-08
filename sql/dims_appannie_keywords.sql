drop table if exists dimensions.appannie_keywords;

create table dimensions.appannie_keywords
partition by event_date
cluster by app_name, country
options(
   description="dimension table for appannie keyword rankings"
   , require_partition_filter=false
)
as
select
    app_name
    , country
    , date(event_date) as event_date
    , keyword
    , rank
    , traffic_share
from `emerald-skill-201716.appannie.keywords_*`
;
