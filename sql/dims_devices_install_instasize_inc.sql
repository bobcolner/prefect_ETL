create or replace table dimensions.devices_install_instasize_stg
as
select
    device_vendor_id
    , max(idfa) as idfa
    , max(subscriber_id) as subscriber_id
    , max(country) as country
    , min(event_time) as installed_at
    , min(if(event_name = 'onboarding_show_first_page', event_time, null)) as onboarding_at
    , min(if(premium_status = 'trial', event_time, null)) as trial_started_at
    , min(if(premium_status = 'active', event_time, null)) as paid_started_at
    , min(current_datetime()) as updated_at
    , 1 as updated_count
from fastream.instasize_ios_prod
where
    device_vendor_id is not null
    and date(event_date) >= datetime('2019-04-01')
    and date(event_date) between 
        date_add(current_date(), interval - 2 day)
        and date_add(current_date(), interval - 1 day)
group by 1
;

merge into dimensions.devices_install_instasize as t
using dimensions.devices_install_instasize_stg as s
    on t.device_vendor_id = s.device_vendor_id
when matched then
    update set
        idfa = coalesce(t.idfa, s.idfa)
        , subscriber_id = coalesce(t.subscriber_id, s.subscriber_id)
        , country = coalesce(t.country, s.country)
        , installed_at = least(t.installed_at, s.installed_at)
        , onboarding_at = coalesce(t.onboarding_at, s.onboarding_at)
        , trial_started_at = coalesce(t.trial_started_at, s.trial_started_at)
        , paid_started_at = coalesce(t.paid_started_at, s.paid_started_at)
        , updated_at = current_datetime()
        , updated_count = t.updated_count + 1
when not matched by target then
    insert row
;
