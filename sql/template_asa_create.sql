drop table if exists apple_search_ads.campaigns_{backfill_postfix};
create table apple_search_ads.campaigns_{backfill_postfix} (
    spend_date string
    , org_id integer
    , app_name string
    , campaign_id integer
    , campaign_name string
    , modification_at timestamp
    , inserted_at timestamp
    , spend_amount Float64
    , impressions integer
    , taps integer
    , installs integer
    , country string
    , json_payload json
);

drop table if exists apple_search_ads.adgroups_{backfill_postfix};
create table apple_search_ads.adgroups_{backfill_postfix} (
    spend_date string
    , org_id integer
    , campaign_id integer
    , adgroup_id integer
    , adgroup_name string
    , modification_at timestamp
    , inserted_at timestamp
    , spend_amount Float64
    , impressions integer
    , taps integer
    , installs integer
    , json_payload json
);

drop table if exists apple_search_ads.keywords_{backfill_postfix};
create table apple_search_ads.keywords_{backfill_postfix} (
    spend_date string
    , org_id integer
    , campaign_id integer
    , adgroup_id integer
    , keyword_id integer
    , keyword string
    , modification_at timestamp
    , inserted_at timestamp
    , spend_amount Float64
    , impressions integer
    , taps integer
    , installs integer
    , json_payload json
);
