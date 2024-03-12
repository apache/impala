-- Used in test_workload_mgmt_init.py tests:
--   * test_upgrade_to_1_0_0_from_previous_binary
--   * test_upgrade_to_latest_from_previous_binary
CREATE EXTERNAL TABLE sys.impala_query_log (
  cluster_id STRING ,
  query_id STRING ,
  session_id STRING ,
  session_type STRING ,
  hiveserver2_protocol_version STRING ,
  db_user STRING ,
  db_user_connection STRING ,
  db_name STRING ,
  impala_coordinator STRING ,
  query_status STRING ,
  query_state STRING ,
  impala_query_end_state STRING ,
  query_type STRING ,
  network_address STRING ,
  start_time_utc TIMESTAMP ,
  total_time_ms DECIMAL(18,3) ,
  query_opts_config STRING ,
  resource_pool STRING ,
  per_host_mem_estimate BIGINT ,
  dedicated_coord_mem_estimate BIGINT ,
  per_host_fragment_instances STRING ,
  backends_count INT ,
  admission_result STRING ,
  cluster_memory_admitted BIGINT ,
  executor_group STRING ,
  executor_groups STRING ,
  exec_summary STRING ,
  num_rows_fetched BIGINT ,
  row_materialization_rows_per_sec BIGINT ,
  row_materialization_time_ms DECIMAL(18,3) ,
  compressed_bytes_spilled BIGINT ,
  event_planning_finished DECIMAL(18,3) ,
  event_submit_for_admission DECIMAL(18,3) ,
  event_completed_admission DECIMAL(18,3) ,
  event_all_backends_started DECIMAL(18,3) ,
  event_rows_available DECIMAL(18,3) ,
  event_first_row_fetched DECIMAL(18,3) ,
  event_last_row_fetched DECIMAL(18,3) ,
  event_unregister_query DECIMAL(18,3) ,
  read_io_wait_total_ms DECIMAL(18,3) ,
  read_io_wait_mean_ms DECIMAL(18,3) ,
  bytes_read_cache_total BIGINT ,
  bytes_read_total BIGINT ,
  pernode_peak_mem_min BIGINT ,
  pernode_peak_mem_max BIGINT ,
  pernode_peak_mem_mean BIGINT ,
  sql STRING ,
  plan STRING ,
  tables_queried STRING NULL
)

PARTITIONED BY SPEC
(
  cluster_id,
  HOUR(start_time_utc)
)

STORED AS ICEBERG

TBLPROPERTIES ('OBJCAPABILITIES'='EXTREAD,EXTWRITE', 'engine.hive.enabled'='true', 'external.table.purge'='TRUE', 'schema_version'='1.0.0', 'table_type'='ICEBERG', 'write.delete.mode'='merge-on-read', 'write.format.default'='parquet', 'write.merge.mode'='merge-on-read', 'write.parquet.compression-codec'='snappy', 'write.update.mode'='merge-on-read')
