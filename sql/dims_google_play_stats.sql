create or replace table dimensions.google_play_subscription_stats
as
select *
from `google_play.import_*`
;