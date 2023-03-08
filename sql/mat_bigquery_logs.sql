drop materialized view mat.mat.bigquery_logs;
create materialized view mat.bigquery_logs
options (enable_refresh = true, refresh_interval_minutes = 30)
as
select 
  protopayload_auditlog.authenticationInfo.principalEmail as user
  , timestamp as event_ts
  , insertId as insert_id
  , resource.type as resource_type
  , protopayload_auditlog.methodName as method
  , protopayload_auditlog.metadataJson as meta_json
  , coalesce(json_extract_scalar(protopayload_auditlog.metadataJson, '$.jobInsertion.job.jobConfig.queryConfig.query'), 
      json_extract_scalar(protopayload_auditlog.metadataJson, '$.jobChange.job.jobConfig.queryConfig.query')) as query
  , coalesce(json_extract_scalar(protopayload_auditlog.metadataJson, '$.jobInsertion.job.jobConfig.queryConfig.statementType'),
      json_extract_scalar(protopayload_auditlog.metadataJson, '$.jobChange.job.jobConfig.queryConfig.statementType')) as statement_type
  , coalesce(json_extract_scalar(protopayload_auditlog.metadatajson, "$.jobInsertion.job.jobconfig.queryconfig.priority"),
      json_extract_scalar(protopayload_auditlog.metadatajson, "$.jobchange.job.jobconfig.queryconfig.priority")) as querytype
  , coalesce(json_extract_scalar(protopayload_auditlog.metadataJson, '$.jobInsertion.job.jobConfig.queryConfig.destinationTable'),
      json_extract_scalar(protopayload_auditlog.metadataJson, '$.jobChange.job.jobConfig.queryConfig.destinationTable')) as dest_table
  , coalesce(json_extract(protopayload_auditlog.metadataJson, '$.jobInsertion.job.jobStats.queryStats.referencedTables'),
      json_extract(protopayload_auditlog.metadataJson, '$.jobChange.job.jobStats.queryStats.referencedTables')) as ref_tables
  , coalesce(cast(json_extract_scalar(protopayload_auditlog.metadataJson, '$.jobInsertion.job.jobStats.queryStats.outputRowCount') as int64),
      cast(json_extract_scalar(protopayload_auditlog.metadataJson, '$.jobChange.job.jobStats.queryStats.outputRowCount') as int64)) as output_row_count
  , coalesce(cast(json_extract_scalar(protopayload_auditlog.metadataJson, '$.jobInsertion.job.jobStats.createTime') as timestamp),
      cast(json_extract_scalar(protopayload_auditlog.metadataJson, '$.jobChange.job.jobStats.createTime') as timestamp)) as job_start_at
  , coalesce(cast(json_extract_scalar(protopayload_auditlog.metadataJson, '$.jobInsertion.job.jobStats.endTime') as timestamp),
      cast(json_extract_scalar(protopayload_auditlog.metadataJson, '$.jobChange.job.jobStats.endTime') as timestamp)) as job_end_at
  , coalesce(cast(json_extract_scalar(protopayload_auditlog.metadataJson, '$.jobInsertion.job.jobStats.queryStats.totalProcessedBytes') as int64),
      cast(json_extract_scalar(protopayload_auditlog.metadataJson, '$.jobChange.job.jobStats.queryStats.totalProcessedBytes') as int64)) / 1000000 as mb_processed
  , coalesce(cast(json_extract_scalar(protopayload_auditlog.metadataJson, '$.jobInsertion.job.jobStats.queryStats.totalBilledBytes') as int64),
      cast(json_extract_scalar(protopayload_auditlog.metadataJson, '$.jobChange.job.jobStats.queryStats.totalBilledBytes') as int64)) / 1000000 as mb_billed
  -- , array_reverse(split(json_extract_scalar(protopayload_auditlog.metadatajson, '$.jobinsertion.job.jobconfig.queryconfig.query'), '\n'))[safe_offset(0)] as t
from bigquery_logs.cloudaudit_googleapis_com_data_access 
where
  protopayload_auditlog.methodName not like 'jobservice.%'
