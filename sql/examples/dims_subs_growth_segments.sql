with monthly_usage as (
select
    subscriber_id as who_identifier
    , datetime_diff(datetime_trunc(event_time, month), datetime_trunc('2000-01-01', month), month) as time_period
    -- , datetime_diff(datetime_trunc(event_time, month), datetime_trunc(current_datetime(), month), month) as time_period
from events.subscriptions_instasize
where
    revenue_usd > 0
    and product_type = '{{ product }}'
group by 1, 2
), lag_lead as (
select
    who_identifier
    , time_period
    , lag(time_period, 1) over (partition by who_identifier order by who_identifier, time_period) as lag
    , lead(time_period, 1) over (partition by who_identifier order by who_identifier, time_period) as lead
from monthly_usage
), lag_lead_with_diffs as (
select
    who_identifier
    , time_period
    , lag
    , lead
    , time_period - lag as lag_size
    , lead - time_period as lead_size
from lag_lead
), calculated as (
select
    time_period
    , case when lag is null then 'New'
        when lag_size = 1 then 'Recurring'
        when lag_size > 1 then 'Re-activation'
        end as segment
    , case when (lead_size > 1 or lead_size is null) then 'churn' else null end as next_month_churn
    , count(distinct who_identifier) as total_subscribers
from lag_lead_with_diffs
group by 1, 2, 3
), tmp_union as (
select
    time_period
    , segment
    , sum(total_subscribers) as total_subscribers
from calculated
group by 1, 2

union all

select
    time_period + 1 as time_period
    , 'Churn' as segment
    , total_subscribers * -1 as total_subscribers
from calculated
where next_month_churn is not null
), tmp2 as (
select
  *
  , date_add(date_trunc('2000-01-01', month), interval time_period month) as event_month
  -- , date_add(date_trunc(current_date(), month), interval time_period month) as event_month
from tmp_union
)
select *
from tmp2
where
  datetime_trunc(event_month, {{ date_rollup }}) < datetime_trunc(datetime('{{ end_date }}'), {{ date_rollup }})
  and datetime_trunc(event_month, {{ date_rollup }}) >= datetime_trunc(datetime('{{ start_date }}'), {{ date_rollup }})
order by 1
;