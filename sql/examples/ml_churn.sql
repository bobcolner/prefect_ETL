create or replace table ml.churn_subs as
select
	subscriber_id
	, min(event_time) as first_sub_active
	, max(event_time) as last_sub_active
	, extract(dayofweek from min(event_time)) as first_sub_active_dow
	, count(distinct device_vendor_id) as unq_idfv
	, max(language) as language
	, max(model) as model
from fastream.instasize_ios_prod
where 
	premium_status = 'active'
	and subscriber_id is not null
	and event_date <= current_date()
group by 1
;


create or replace table ml.churn_weeks as
with sub_weeks as (
select
	subscriber_id
	, date_trunc(event_date, week) as event_week
	, count(1) as total_events_count
	, count(if(event_name = 'edit_asset', 1, null)) as edit_asset_count
	, count(if(event_name = 'share_action', 1, null)) as share_action_count
from fastream.instasize_ios_prod
where 
	premium_status = 'active'
	and subscriber_id is not null
	and event_date <= current_date()
group by 1, 2
), sub_length as (
select 
	subscriber_id
	, count(1) as active_weeks
from sub_weeks
group by 1
)
select
	subscriber_id
	, event_week
	, row_number() over(partition by subscriber_id order by event_week) as week_num
	, total_events_count
	, edit_asset_count
	, share_action_count
from sub_weeks
where subscriber_id in (select subscriber_id from sub_length where active_weeks >= 8)
;


create or replace table ml.churn_baseline as
select 
	subscriber_id
	, avg(total_events_count) as total_events_baseline
	, avg(edit_asset_count) as edit_asset_baseline
	, avg(share_action_count) as share_action_baseline
from ml.churn_weeks
where week_num <= 6
group by 1
;


create or replace table ml.churn_weeks_pct as
select
	w.*
	, b.total_events_baseline
	, b.edit_asset_baseline
	, b.share_action_baseline
	, safe_divide(w.total_events_count, b.total_events_baseline) as total_events_pct
	, safe_divide(w.edit_asset_count, b.edit_asset_baseline) as edit_asset_pct
	, safe_divide(w.share_action_count, b.share_action_baseline) as share_action_pct
from ml.churn_weeks as w
join ml.churn_baseline as b
	on(w.subscriber_id = b.subscriber_id)
;


create or replace table ml.churn_weeks_wide as
select
	*
	-- total events
	, lag(total_events_count, 1) over(partition by subscriber_id order by event_week) as total_events_preweek_1
	, lag(total_events_count, 2) over(partition by subscriber_id order by event_week) as total_events_preweek_2
	, lag(total_events_count, 3) over(partition by subscriber_id order by event_week) as total_events_preweek_3
	, lag(total_events_count, 4) over(partition by subscriber_id order by event_week) as total_events_preweek_4
	, lag(total_events_count, 5) over(partition by subscriber_id order by event_week) as total_events_preweek_5
	, lag(total_events_count, 6) over(partition by subscriber_id order by event_week) as total_events_preweek_6
	, lag(total_events_count, 7) over(partition by subscriber_id order by event_week) as total_events_preweek_7
	-- edit assets
	, lag(edit_asset_count, 1) over(partition by subscriber_id order by event_week) as edit_asset_preweek_1
	, lag(edit_asset_count, 2) over(partition by subscriber_id order by event_week) as edit_asset_preweek_2
	, lag(edit_asset_count, 3) over(partition by subscriber_id order by event_week) as edit_asset_preweek_3
	, lag(edit_asset_count, 4) over(partition by subscriber_id order by event_week) as edit_asset_preweek_4
	, lag(edit_asset_count, 5) over(partition by subscriber_id order by event_week) as edit_asset_preweek_5
	, lag(edit_asset_count, 6) over(partition by subscriber_id order by event_week) as edit_asset_preweek_6
	, lag(edit_asset_count, 7) over(partition by subscriber_id order by event_week) as edit_asset_preweek_7
	-- share action
	, lag(share_action_count, 1) over(partition by subscriber_id order by event_week) as share_action_preweek_1
	, lag(share_action_count, 2) over(partition by subscriber_id order by event_week) as share_action_preweek_2
	, lag(share_action_count, 3) over(partition by subscriber_id order by event_week) as share_action_preweek_3
	, lag(share_action_count, 4) over(partition by subscriber_id order by event_week) as share_action_preweek_4
	, lag(share_action_count, 5) over(partition by subscriber_id order by event_week) as share_action_preweek_5
	, lag(share_action_count, 6) over(partition by subscriber_id order by event_week) as share_action_preweek_6
	, lag(share_action_count, 7) over(partition by subscriber_id order by event_week) as share_action_preweek_7

	-- total events
	, lag(total_events_pct, 1) over(partition by subscriber_id order by event_week) as total_events_preweek_pct_1
	, lag(total_events_pct, 2) over(partition by subscriber_id order by event_week) as total_events_preweek_pct_2
	, lag(total_events_pct, 3) over(partition by subscriber_id order by event_week) as total_events_preweek_pct_3
	, lag(total_events_pct, 4) over(partition by subscriber_id order by event_week) as total_events_preweek_pct_4
	, lag(total_events_pct, 5) over(partition by subscriber_id order by event_week) as total_events_preweek_pct_5
	, lag(total_events_pct, 6) over(partition by subscriber_id order by event_week) as total_events_preweek_pct_6
	, lag(total_events_pct, 7) over(partition by subscriber_id order by event_week) as total_events_preweek_pct_7
	-- edit assets
	, lag(edit_asset_pct, 1) over(partition by subscriber_id order by event_week) as edit_asset_preweek_pct_1
	, lag(edit_asset_pct, 2) over(partition by subscriber_id order by event_week) as edit_asset_preweek_pct_2
	, lag(edit_asset_pct, 3) over(partition by subscriber_id order by event_week) as edit_asset_preweek_pct_3
	, lag(edit_asset_pct, 4) over(partition by subscriber_id order by event_week) as edit_asset_preweek_pct_4
	, lag(edit_asset_pct, 5) over(partition by subscriber_id order by event_week) as edit_asset_preweek_pct_5
	, lag(edit_asset_pct, 6) over(partition by subscriber_id order by event_week) as edit_asset_preweek_pct_6
	, lag(edit_asset_pct, 7) over(partition by subscriber_id order by event_week) as edit_asset_preweek_pct_7
	-- share action
	, lag(share_action_pct, 1) over(partition by subscriber_id order by event_week) as share_action_preweek_pct_1
	, lag(share_action_pct, 2) over(partition by subscriber_id order by event_week) as share_action_preweek_pct_2
	, lag(share_action_pct, 3) over(partition by subscriber_id order by event_week) as share_action_preweek_pct_3
	, lag(share_action_pct, 4) over(partition by subscriber_id order by event_week) as share_action_preweek_pct_4
	, lag(share_action_pct, 5) over(partition by subscriber_id order by event_week) as share_action_preweek_pct_5
	, lag(share_action_pct, 6) over(partition by subscriber_id order by event_week) as share_action_preweek_pct_6
	, lag(share_action_pct, 7) over(partition by subscriber_id order by event_week) as share_action_preweek_pct_7
from ml.churn_weeks_pct
;

-- emerald-skill-201716
-- ml
-- churn_training
create or replace table ml.churn_training as
select 
	w.*
	, s.unq_idfv
	, s.model
	, s.language
	, s.first_sub_active_dow
	, date_diff(event_week, date_trunc(date(s.first_sub_active), week), week) as active_sub_weeks
	, if(event_week = date_trunc(date(s.last_sub_active), week), 'churn', 'retain') as churn_target
from ml.churn_weeks_wide as w
join ml.churn_subs as s
	on(w.subscriber_id = s.subscriber_id)
where 
	week_num >= 8
	and unq_idfv <= 5
	and first_sub_active >= datetime('2018-01-01')
	and last_sub_active <= datetime('2020-07-01')
;


create or replace table ml.churn_training_monthly as
with monthly_subs as (
select subscriber_id
from dimensions.subscribers_new
where 
  revenue_m1 < 5
  and renewal_count >= 2
  and first_revenue >= datetime('2018-01-01')
  and product_type = 'instasize_premium_plus_subscription'
)
select c.*
from ml.churn_training as c
inner join monthly_subs as m
	on(c.subscriber_id = m.subscriber_id)
;


select *
from ml.churn_training_monthly
order by 1, 2 desc
limit 999
