drop table if exists revenuecat.typeloop_etl;

LOAD DATA INTO revenuecat.typeloop_etl (
  rc_original_app_user_id string,
  rc_last_seen_app_user_id_alias string,
  country string,
  product_identifier string,
  start_time datetime,
  end_time datetime,
  store string,
  is_auto_renewable string,
  is_trial_period string,
  is_in_intro_offer_period string,
  is_sandbox string,
  price_in_usd float64,
  takehome_percentage float64,
  store_transaction_id string,
  original_store_transaction_id string,
  refunded_at datetime,
  unsubscribe_detected_at datetime,
  billing_issues_detected_at datetime,
  purchased_currency string,
  price_in_purchased_currency float64,
  entitlement_identifiers string,
  renewal_number int64,
  is_trial_conversion string,
  presented_offering string,
  reserved_subscriber_attributes string,
  custom_subscriber_attributes string,
  platform string,
)
FROM FILES(
  skip_leading_rows=1,
  format='CSV',
  uris = ['gs://revenuecat_daily_etl/type-loop/{latest_file}']
);
