with sub_table as (
select
  original_transaction_id as subscriber_id
  , original_product_type as product_name
  , original_purchase_date as sub_start_at
  , if(expires_at < current_datetime(), expires_at, null) as expired_at
from fastream.instasize_api_subscription_statuses_prod
where
  os = 'ios'
  and original_product_type = '{{ product }}'
  and trial is false
  -- and payments_count > 0
  and datetime_trunc(original_purchase_date, {{ date_rollup }}) >= datetime_trunc(datetime('{{ start_date }}'), {{ date_rollup }})
  and datetime_trunc(original_purchase_date, {{ date_rollup }}) < datetime_trunc(datetime('{{ end_date }}'), {{ date_rollup }})
)
select
  datetime_trunc(sub_start_at, month) as sub_start_date
  , if(expired_at is null, 'active', 'expired') as status
  , count(1) as total_subs
from sub_table
group by 1, 2
;

with sub_table as (
select
  original_transaction_id as subscriber_id
  , original_product_type as product_name
  , original_purchase_date as sub_start_at
  , if(expires_at < current_datetime(), expires_at, null) as expired_at
from fastream.instasize_api_subscription_statuses_prod
where
  os = 'ios'
  and original_product_type = '{{ product }}'
  and trial is false
  -- and payments_count > 0
  and datetime_trunc(original_purchase_date, {{ date_rollup }}) >= datetime_trunc(datetime('{{ start_date }}'), {{ date_rollup }})
  and datetime_trunc(original_purchase_date, {{ date_rollup }}) < datetime_trunc(datetime('{{ end_date }}'), {{ date_rollup }})
), monthly_activity as (
select
  datetime_trunc(sub_start_at, month) as sub_start_date
  , datetime_diff(datetime_trunc(expired_at, month), datetime_trunc(sub_start_at, month), month) + 1 as sub_length
  , if(expired_at is null, 'active', 'expired') as status
  , count(1) as total_subs
from sub_table
group by 1, 2, 3
), tmp as (
select
  *
  , sum(case when status = 'expired' then total_subs else 0 end) over (partition by sub_start_date order by sub_length) as subs_expired
  , sum(total_subs) over (partition by sub_start_date) as subs_month_total
from monthly_activity
)
select
  *
  , case when subs_month_total = 0 then 0 else subs_expired / subs_month_total end churn_rate
  , case when subs_month_total = 0 then 0 else 1 - (subs_expired / subs_month_total) end retention_rate
from tmp
where sub_length >= 0
order by 1, 2, 3, 4
;