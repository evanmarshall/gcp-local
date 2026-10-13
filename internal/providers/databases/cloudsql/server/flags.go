package server

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
)

// Flag is one Cloud SQL database flag, passed to the server at startup.
type Flag struct {
	Name, Value string
}

// FlagCheck classifies one requested flag.
type FlagCheck int

// Flag classifications: modeled with a valid value, documented by Cloud SQL
// but not modeled here (a coverage gap), unknown to Cloud SQL, or modeled
// with a value outside its type or range.
const (
	FlagValid FlagCheck = iota
	FlagUnmodeled
	FlagUnknown
	FlagInvalidValue
)

// flagKind is one modeled flag's value type and range, from Cloud SQL's
// supported-flags tables (docs.cloud.google.com/sql/docs/postgres/flags and
// /mysql/flags, retrieved 2026-10-12).
type flagKind struct {
	integer   bool
	low, high float64
	values    []string
}

var (
	onOff        = flagKind{values: []string{"on", "off"}}
	milliseconds = flagKind{integer: true, low: 0, high: 2147483647}
)

// postgresFlags are the modeled PostgreSQL flags.
var postgresFlags = map[string]flagKind{
	"max_connections":                     {integer: true, low: 14, high: 262143},
	"log_min_duration_statement":          {integer: true, low: -1, high: 2147483647},
	"log_statement":                       {values: []string{"none", "ddl", "mod", "all"}},
	"log_connections":                     onOff,
	"log_disconnections":                  onOff,
	"log_lock_waits":                      onOff,
	"log_checkpoints":                     onOff,
	"log_duration":                        onOff,
	"autovacuum":                          onOff,
	"log_temp_files":                      {integer: true, low: -1, high: 2147483647},
	"work_mem":                            {integer: true, low: 64, high: 2147483647},
	"maintenance_work_mem":                {integer: true, low: 1024, high: 2147483647},
	"idle_in_transaction_session_timeout": milliseconds,
	"lock_timeout":                        milliseconds,
	"max_locks_per_transaction":           {integer: true, low: 10, high: 2147483647},
}

// mysqlFlags are the modeled MySQL flags.
var mysqlFlags = map[string]flagKind{
	"max_connections":                 {integer: true, low: 1, high: 100000},
	"slow_query_log":                  onOff,
	"long_query_time":                 {low: 0, high: 30000000},
	"general_log":                     onOff,
	"log_output":                      {values: []string{"FILE", "TABLE", "NONE"}},
	"wait_timeout":                    {integer: true, low: 1, high: 31536000},
	"interactive_timeout":             {integer: true, low: 1, high: 31536000},
	"max_allowed_packet":              {integer: true, low: 16384, high: 1073741824},
	"innodb_lock_wait_timeout":        {integer: true, low: 1, high: 1073741824},
	"local_infile":                    onOff,
	"log_bin_trust_function_creators": onOff,
	"event_scheduler":                 onOff,
}

// CheckFlag classifies one flag for a databaseVersion. For an invalid value
// it also returns why, in GCP's words (observed live: cloudsql-mysql-backups-flags).
func CheckFlag(version string, flag Flag) (FlagCheck, string) {
	modeled, documented := postgresFlags, postgresDocumented
	if IsMySQL(version) {
		modeled, documented = mysqlFlags, mysqlDocumented
	}
	kind, ok := modeled[flag.Name]
	switch {
	case ok:
		if reason := kind.refusal(flag.Value); reason != "" {
			return FlagInvalidValue, reason
		}
		return FlagValid, ""
	case slices.Contains(strings.Fields(documented), flag.Name):
		return FlagUnmodeled, ""
	default:
		return FlagUnknown, ""
	}
}

// refusal says why value doesn't fit the flag's type or range, or "".
func (k flagKind) refusal(value string) string {
	switch {
	case len(k.values) > 0:
		return k.enumRefusal(value)
	case k.integer:
		return k.integerRefusal(value)
	default:
		return k.realRefusal(value)
	}
}

// enumRefusal says why value isn't one of the flag's values, or "".
func (k flagKind) enumRefusal(value string) string {
	switch {
	case slices.Contains(k.values, value):
		return ""
	case k.values[0] == "on":
		return value + " is not on/off."
	default:
		return value + " is not one of " + strings.Join(k.values, ", ") + "."
	}
}

// realRefusal says why value isn't a number in range, or "".
func (k flagKind) realRefusal(value string) string {
	number, err := strconv.ParseFloat(value, 64)
	switch {
	case err != nil:
		return value + " is not a number."
	case number < k.low || number > k.high:
		return fmt.Sprintf("%f is not between %f and %f.", number, k.low, k.high)
	default:
		return ""
	}
}

// integerRefusal says why value isn't an integer in range, or "".
func (k flagKind) integerRefusal(value string) string {
	number, err := strconv.ParseInt(value, 10, 64)
	switch {
	case err != nil:
		return value + " is not an integer."
	case float64(number) < k.low || float64(number) > k.high:
		return fmt.Sprintf("%d is not between %d and %d.", number, int64(k.low), int64(k.high))
	default:
		return ""
	}
}

// postgresDocumented are the PostgreSQL flag names Cloud SQL documents
// (docs.cloud.google.com/sql/docs/postgres/flags, retrieved 2026-10-12).
const postgresDocumented = `anon.algorithm anon.maskschema anon.restrict_to_trusted_schemas anon.salt anon.sourceschema
auto_explain.log_analyze auto_explain.log_buffers auto_explain.log_min_duration auto_explain.log_format auto_explain.log_level
auto_explain.log_nested_statements auto_explain.log_settings auto_explain.log_timing auto_explain.log_triggers auto_explain.log_wal
auto_explain.log_verbose auto_explain.sample_rate autovacuum autovacuum_analyze_scale_factor autovacuum_analyze_threshold
autovacuum_freeze_max_age autovacuum_max_workers autovacuum_multixact_freeze_max_age autovacuum_naptime autovacuum_vacuum_cost_delay
autovacuum_vacuum_cost_limit autovacuum_vacuum_insert_scale_factor autovacuum_vacuum_insert_threshold autovacuum_vacuum_max_threshold
autovacuum_vacuum_scale_factor autovacuum_vacuum_threshold autovacuum_work_mem autovacuum_worker_slots backend_flush_after bgwriter_delay
bgwriter_flush_after bgwriter_lru_maxpages bgwriter_lru_multiplier checkpoint_completion_target checkpoint_flush_after checkpoint_timeout
checkpoint_warning client_connection_check_interval cloudsql.allow_passwordless_local_connections cloudsql.enable_anon
cloudsql.enable_auto_explain cloudsql.enable_index_advisor cloudsql.enable_maintenance_mode cloudsql.enable_parameterized_secure_views
cloudsql.enable_pgaudit cloudsql.enable_pg_bigm cloudsql.enable_pg_cron cloudsql.enable_pg_hint_plan cloudsql.enable_pglogical
cloudsql.enable_pg_squeeze cloudsql.enable_pg_textsearch cloudsql.enable_pg_wait_sampling cloudsql.enable_vector_assist
cloudsql.iam_authentication cloudsql.logical_decoding cloudsql.max_failed_attempts_user cloudsql.pgaudit_mask_literals
cloudsql.pg_authid_select_role commit_delay commit_siblings constraint_exclusion cpu_index_tuple_cost cpu_operator_cost cpu_tuple_cost
cron.database_name cron.log_statement cron.log_run cron.max_running_jobs cron.log_min_messages cursor_tuple_fraction deadlock_timeout
default_statistics_target default_tablespace default_toast_compression default_transaction_deferrable default_transaction_isolation
effective_cache_size effective_io_concurrency enable_async_append enable_bitmapscan enable_distinct_reordering enable_gathermerge
enable_incremental_sort enable_memoize enable_parallel_append enable_parallel_hash enable_partition_pruning enable_partitionwise_aggregate
enable_partitionwise_join enable_hashagg enable_hashjoin enable_indexonlyscan enable_indexscan enable_material enable_mergejoin
enable_nestloop enable_self_join_elimination enable_seqscan enable_sort enable_tidscan force_parallel_mode from_collapse_limit geqo
geqo_effort geqo_generations geqo_pool_size geqo_seed geqo_selection_bias geqo_threshold gin_fuzzy_search_limit gin_pending_list_limit
hash_mem_multiplier hot_standby_feedback huge_pages idle_in_transaction_session_timeout io_max_concurrency io_method io_workers
ivfflat.probes join_collapse_limit local_preload_libraries lock_timeout log_autovacuum_min_duration log_checkpoints log_connections
log_disconnections log_duration log_error_verbosity log_executor_stats log_hostname log_line_prefix log_lock_failures log_lock_waits
log_min_duration_statement log_min_error_statement log_min_messages log_parameter_max_length log_parameter_max_length_on_error
log_parser_stats log_planner_stats log_recovery_conflict_waits log_replication_commands log_statement log_statement_stats log_temp_files
log_timezone logical_decoding_work_mem maintenance_io_concurrency maintenance_work_mem max_active_replication_origins max_connections
max_locks_per_transaction max_logical_replication_workers max_parallel_maintenance_workers max_parallel_workers
max_parallel_workers_per_gather max_pred_locks_per_page max_pred_locks_per_relation max_pred_locks_per_transaction
max_prepared_transactions max_replication_slots max_slot_wal_keep_size max_stack_depth max_standby_archive_delay
max_standby_streaming_delay max_sync_workers_per_subscription max_wal_senders max_wal_size max_worker_processes
min_parallel_index_scan_size min_parallel_table_scan_size min_parallel_relation_size min_wal_size old_snapshot_threshold
parallel_setup_cost parallel_tuple_cost password_encryption pg_bigm.enable_recheck pg_bigm.gin_key_limit pg_bigm.similarity_limit
pg_hint_plan.enable_hint pg_hint_plan.debug_print pg_hint_plan.parse_messages pg_hint_plan.message_level pg_hint_plan.enable_hint_table
pglogical.batch_inserts pglogical.conflict_log_level pglogical.conflict_resolution pglogical.extra_connection_options
pglogical.synchronous_commit pglogical.use_spi pg_stat_statements.max pg_stat_statements.save pg_stat_statements.track
pg_stat_statements.track_planning pg_stat_statements.track_utility pg_textsearch.bulk_load_threshold pg_textsearch.compress_segments
pg_textsearch.default_limit pg_textsearch.memtable_pages_threshold pg_textsearch.segments_per_level pgaudit.log pgaudit.log_catalog
pgaudit.log_client pgaudit.log_level pgaudit.log_parameter pgaudit.log_relation pgaudit.log_statement_once pgaudit.role pgaudit.log_rows
pgtt.enabled pg_wait_sampling.history_period pg_wait_sampling.history_size pg_wait_sampling.profile_period pg_wait_sampling.profile_pid
pg_wait_sampling.profile_queries random_page_cost plan_cache_mode rdkit.agent_FP_bit_ratio rdkit.avalon_fp_size rdkit.dice_threshold
rdkit.difference_FP_weight_agents rdkit.difference_FP_weight_nonagents rdkit.do_chiral_sss rdkit.do_enhanced_stereo_sss
rdkit.featmorgan_fp_size rdkit.hashed_atompair_fp_size rdkit.hashed_torsion_fp_size rdkit.ignore_reaction_agents rdkit.init_reaction
rdkit.layered_fp_size rdkit.morgan_fp_size rdkit.move_unmmapped_reactants_to_agents rdkit.rdkit_fp_size
rdkit.reaction_difference_fp_size rdkit.reaction_difference_fp_type rdkit.reaction_sss_fp_size rdkit.reaction_sss_fp_type
rdkit.sss_fp_size rdkit.tanimoto_threshold rdkit.threshold_unmapped_reactant_atoms recovery_prefetch replacement_sort_tuples
reserved_connections session_replication_role seq_page_cost shared_buffers squeeze.max_xlock_time squeeze.worker_autostart
squeeze.worker_role ssl_max_protocol_version ssl_min_protocol_version standard_conforming_strings synchronize_seqscans
tcp_keepalives_count tcp_keepalives_idle tcp_keepalives_interval temp_buffers temp_file_limit TimeZone trace_notify
trace_recovery_messages trace_sort track_activities track_activity_query_size track_commit_timestamp track_cost_delay_timing
track_counts track_functions track_io_timing vacuum_cost_delay vacuum_cost_limit vacuum_cost_page_dirty vacuum_cost_page_hit
vacuum_cost_page_miss vacuum_failsafe_age vacuum_freeze_min_age vacuum_freeze_table_age vacuum_max_eager_freeze_failure_rate
vacuum_multixact_failsafe_age vacuum_multixact_freeze_min_age vacuum_multixact_freeze_table_age vacuum_truncate wal_buffers
wal_compression wal_decode_buffer_size wal_receiver_timeout wal_sender_timeout wal_writer_delay wal_writer_flush_after work_mem`

// mysqlDocumented are the MySQL flag names Cloud SQL documents
// (docs.cloud.google.com/sql/docs/mysql/flags, retrieved 2026-10-12).
const mysqlDocumented = `activate_all_roles_on_login activate_mandatory_roles autocommit auto_increment_increment auto_increment_offset
automatic_sp_privileges back_log binlog_cache_size binlog_expire_logs_seconds binlog_group_commit_sync_delay
binlog_group_commit_sync_no_delay_count binlog_gtid_simple_recovery binlog_order_commits binlog_row_image binlog_row_metadata
binlog_row_value_options binlog_rows_query_log_events binlog_stmt_cache_size binlog_transaction_compression
binlog_transaction_compression_level_zstd binlog_transaction_dependency_history_size binlog_transaction_dependency_tracking
block_encryption_mode bulk_insert_buffer_size caching_sha2_password_storage_format caching_sha2_password_enforce_storage_format
caching_sha2_password_proxy_users collation_connection collation_server character_set_client character_set_connection
character_set_results character_set_server check_proxy_users cloudsql_allow_analyze_table cloudsql_avoid_parse_session_logging
cloudsql_iam_authentication cloudsql_ignore_innodb_encryption cloudsql_mysql_audit_data_masking_cmds
cloudsql_mysql_audit_data_masking_regex cloudsql_mysql_audit_log_write_period cloudsql_mysql_audit_max_query_length
cloudsql_parameterized_secure_view cloudsql_vector cloudsql_vector_iterative_filtering cloudsql_vector_iterative_filtering_max_neighbors
cloudsql_vector_max_mem_size completion_type concurrent_insert connect_timeout connection_memory_chunk_size connection_memory_limit
connection_memory_status_limit cte_max_recursion_depth default_authentication_plugin default_password_lifetime default_time_zone
default_tmp_storage_engine default_week_format delay_key_write disconnect_on_expired_password div_precision_increment
end_markers_in_json eq_range_index_dive_limit event_scheduler expire_logs_days explicit_defaults_for_timestamp flush_time
foreign_key_checks ft_max_word_len ft_min_word_len ft_query_expansion_limit ft_stopword_file general_log
generated_random_password_length global_connection_memory_limit global_connection_memory_status_limit
global_connection_memory_tracking group_concat_max_len gtid_executed_compression_period histogram_generation_max_mem_size init_connect
innodb_adaptive_hash_index innodb_adaptive_hash_index_parts innodb_adaptive_max_sleep_delay innodb_autoextend_increment
innodb_autoinc_lock_mode innodb_buffer_pool_chunk_size innodb_buffer_pool_dump_pct innodb_buffer_pool_dump_at_shutdown
innodb_buffer_pool_dump_now innodb_buffer_pool_instances innodb_buffer_pool_load_abort innodb_buffer_pool_load_at_startup
innodb_buffer_pool_load_now innodb_buffer_pool_size innodb_change_buffer_max_size innodb_change_buffering innodb_checksum_algorithm
innodb_cloudsql_managed_buffer_pool innodb_cloudsql_managed_buffer_pool_threshold_pct innodb_cloudsql_managed_buffer_pool_tuneup_pct
innodb_cloudsql_optimized_write innodb_cmp_per_index_enabled innodb_commit_concurrency innodb_compression_failure_threshold_pct
innodb_compression_level innodb_compression_pad_pct_max innodb_concurrency_tickets innodb_ddl_buffer_size innodb_ddl_threads
innodb_deadlock_detect innodb_disable_sort_file_cache innodb_doublewrite_batch_size innodb_doublewrite_files innodb_doublewrite_pages
innodb_file_per_table innodb_fill_factor innodb_flush_log_at_timeout innodb_flush_log_at_trx_commit innodb_flush_method
innodb_flush_neighbors innodb_flush_sync innodb_flushing_avg_loops innodb_fsync_threshold innodb_ft_aux_table innodb_ft_cache_size
innodb_ft_enable_diag_print innodb_ft_enable_stopword innodb_ft_max_token_size innodb_ft_min_token_size innodb_ft_num_word_optimize
innodb_ft_result_cache_limit innodb_ft_server_stopword_table innodb_ft_sort_pll_degree innodb_ft_total_cache_size
innodb_ft_user_stopword_table innodb_idle_flush_pct innodb_io_capacity innodb_io_capacity_max innodb_large_prefix
innodb_lock_wait_timeout innodb_log_buffer_size innodb_log_checksums innodb_log_file_size innodb_log_spin_cpu_abs_lwm
innodb_log_spin_cpu_pct_hwm innodb_log_wait_for_flush_spin_hwm innodb_log_write_ahead_size innodb_lru_scan_depth
innodb_max_dirty_pages_pct innodb_max_dirty_pages_pct_lwm innodb_max_purge_lag innodb_max_undo_log_size innodb_max_purge_lag_delay
innodb_monitor_disable innodb_monitor_enable innodb_monitor_reset innodb_monitor_reset_all innodb_native_foreign_keys
innodb_old_blocks_pct innodb_old_blocks_time innodb_online_alter_log_max_size innodb_open_files innodb_optimize_fulltext_only
innodb_page_cleaners innodb_parallel_read_threads innodb_print_all_deadlocks innodb_print_ddl_logs innodb_purge_batch_size
innodb_purge_rseg_truncate_frequency innodb_purge_threads innodb_random_read_ahead innodb_read_ahead_threshold innodb_read_io_threads
innodb_redo_log_capacity innodb_replication_delay innodb_rollback_on_timeout innodb_rollback_segments innodb_segment_reserve_factor
innodb_sort_buffer_size innodb_spin_wait_delay innodb_stats_auto_recalc innodb_stats_include_delete_marked innodb_stats_method
innodb_stats_on_metadata innodb_stats_persistent innodb_stats_persistent_sample_pages innodb_stats_sample_pages
innodb_stats_transient_sample_pages innodb_status_output innodb_status_output_locks innodb_strict_mode innodb_sync_array_size
innodb_sync_spin_loops innodb_table_locks innodb_thread_concurrency innodb_thread_sleep_delay innodb_undo_log_truncate
innodb_use_native_aio innodb_write_io_threads interactive_timeout internal_tmp_disk_storage_engine internal_tmp_mem_storage_engine
join_buffer_size keep_files_on_create key_buffer_size key_cache_age_threshold key_cache_block_size key_cache_division_limit
lc_times_names local_infile lock_wait_timeout log_bin_trust_function_creators log_output log_error_verbosity
log_queries_not_using_indexes log_slow_admin_statements log_slow_extra log_slow_replica_statements log_slow_slave_statements
log_throttle_queries_not_using_indexes log_timestamps long_query_time lower_case_table_names mandatory_roles master_verify_checksum
max_allowed_packet max_binlog_cache_size max_binlog_size max_binlog_stmt_cache_size max_connect_errors max_connections
max_digest_length max_error_count max_execution_time max_heap_table_size max_join_size max_length_for_sort_data max_points_in_geometry
max_prepared_stmt_count max_seeks_for_key max_sort_length max_sp_recursion_depth max_user_connections max_write_lock_count
min_examined_row_limit myisam_data_pointer_size myisam_max_sort_file_size myisam_mmap_size myisam_sort_buffer_size myisam_stats_method
myisam_use_mmap mysql_native_password_proxy_users net_buffer_length net_read_timeout net_retry_count net_write_timeout ngram_token_size
optimizer_prune_level optimizer_search_depth optimizer_switch optimizer_trace optimizer_trace_features optimizer_trace_max_mem_size
optimizer_trace_offset parser_max_mem_size password_history password_require_current password_reuse_interval performance_schema
performance_schema_accounts_size performance_schema_digests_size performance_schema_error_size
performance_schema_events_stages_history_long_size performance_schema_events_stages_history_size
performance_schema_events_statements_history_long_size performance_schema_events_statements_history_size
performance_schema_events_transactions_history_long_size performance_schema_events_transactions_history_size
performance_schema_events_waits_history_long_size performance_schema_events_waits_history_size performance_schema_hosts_size
performance_schema_max_cond_classes performance_schema_max_cond_instances performance_schema_max_digest_length
performance_schema_max_digest_sample_age performance_schema_max_file_classes performance_schema_max_file_handles
performance_schema_max_file_instances performance_schema_max_index_stat performance_schema_max_logger_classes
performance_schema_max_memory_classes performance_schema_max_metadata_locks performance_schema_max_mutex_classes
performance_schema_max_mutex_instances performance_schema_max_prepared_statements_instances performance_schema_max_program_instances
performance_schema_max_rwlock_classes performance_schema_max_rwlock_instances performance_schema_max_socket_classes
performance_schema_max_socket_instances performance_schema_max_sql_text_length performance_schema_max_stage_classes
performance_schema_max_statement_classes performance_schema_max_statement_stack performance_schema_max_table_handles
performance_schema_max_table_instances performance_schema_max_table_lock_stat performance_schema_max_thread_classes
performance_schema_max_thread_instances performance_schema_session_connect_attrs_size performance_schema_setup_actors_size
performance_schema_setup_objects_size performance_schema_show_processlist performance_schema_users_size preload_buffer_size
query_alloc_block_size query_cache_limit query_cache_min_res_unit query_cache_size query_cache_type query_cache_wlock_invalidate
query_prealloc_size range_alloc_block_size range_optimizer_max_mem_size read_buffer_size read_only read_rnd_buffer_size
regexp_stack_limit regexp_time_limit replica_checkpoint_group replica_checkpoint_period replica_compressed_protocol replica_net_timeout
replica_parallel_type replica_parallel_workers replica_pending_jobs_size_max replica_preserve_commit_order replica_skip_errors
replica_sql_verify_checksum replica_transaction_retries replica_type_conversions replicate_do_db replicate_do_table
replicate_ignore_db replicate_ignore_table replicate_wild_do_table replicate_wild_ignore_table rpl_read_size schema_definition_cache
session_track_gtids session_track_schema session_track_state_change session_track_transaction_info sha256_password_proxy_users
show_create_table_verbosity show_compatibility_56 skip_character_set_client_handshake skip_show_database slave_checkpoint_group
slave_checkpoint_period slave_compressed_protocol slave_net_timeout slave_parallel_type slave_parallel_workers
slave_pending_jobs_size_max slave_preserve_commit_order slave_skip_errors slave_sql_verify_checksum slave_transaction_retries
slave_type_conversions slow_launch_time slow_query_log sort_buffer_size source_verify_checksum sql_generate_invisible_primary_key
sql_mode sql_require_primary_key sql_select_limit stored_program_cache stored_program_definition_cache sync_binlog sync_master_info
sync_relay_log sync_relay_log_info sync_source_info sysdate_is_now table_definition_cache tablespace_definition_cache table_open_cache
table_open_cache_instances table_open_cache_triggers temptable_max_mmap temptable_max_ram thread_cache_size thread_stack tls_version
tmp_table_size transaction_alloc_block_size transaction_isolation transaction_prealloc_size transaction_write_set_extraction
unique_checks updatable_views_with_limit wait_timeout windowing_use_high_precision`
