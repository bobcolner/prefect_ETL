with user_cohorts as (
select
  subscriber_id
  , product_type
  , datetime_trunc(first_revenue_at, {{ date_rollup }}) as sub_start_month
from dimensions.prodsubs_ios_instasize
where
  first_revenue_at is not null
  -- and activation_at is not null
  -- and product_order = 1
  and product_duration = 'month'
  and product_type = '{{ product }}'
  and datetime_trunc(first_revenue_at, {{ date_rollup }}) >= datetime_trunc(datetime('{{ start_date }}'), {{ date_rollup }})
  and datetime_trunc(first_revenue_at, {{ date_rollup }}) <= datetime_trunc(datetime('{{ end_date }}'), {{ date_rollup }})
), order_month as (
select  
  subscriber_id
  , sub_start_month
  , date_diff(date_trunc(e.event_date, month), sub_start_month, month) + 1 as month_number  --offset by 1
  , min(product_type) as product_type
  , sum(e.revenue_usd) as revenue
from events.subscriptions_instasize as e
join user_cohorts 
  using(subscriber_id, product_type)
where 
  e.revenue_usd > 0
  and e.product_duration = 'month'
  and date_trunc(e.event_date, month) < date_trunc(current_date(), month)
group by 1, 2, 3
), cohort_size as (
select
  sub_start_month
  , product_type
  , count(1) as total_subs
from user_cohorts
group by 1, 2
), counts as (
select 
  sub_start_month
  , month_number
  , product_type
  , count(1) as total_subs
  , sum(revenue) as revenue
from order_month as o
group by 1, 2, 3
)
select 
  c.*
  , c.total_subs / s.total_subs as pct_retained
from counts as c
join cohort_size as s
  using(sub_start_month, product_type)
order by 1, 2
;