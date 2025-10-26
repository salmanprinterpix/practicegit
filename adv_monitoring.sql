# PostgreSQL Advanced Monitoring Techniques

## 221. Custom pg_stat_statements Views

### Basic Setup

```sql
-- Enable pg_stat_statements extension
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

-- Create normalized top queries view
CREATE OR REPLACE VIEW v_top_queries AS
SELECT 
  query,
  calls,
 total_exec_time,
  mean_exec_time,
  max_exec_time,
  rows,
  100.0 * shared_blks_hit / nullif(shared_blks_hit + shared_blks_read, 0) AS cache_hit_ratio,
  stddev_plan_time
FROM pg_stat_statements
WHERE query NOT LIKE 'autovacuum%'
ORDER BY total_exec_time DESC
LIMIT 20;

-- Create user-specific queries view
CREATE OR REPLACE VIEW v_user_queries AS
SELECT 
  userid,
  usename,
  query,
  calls,
  total_time,
  mean_time,
  max_time,
  rows
FROM pg_stat_statements pss
JOIN pg_user u ON pss.userid = u.usesysid
WHERE userid != 10  -- exclude postgres user
ORDER BY total_time DESC;

-- Create query text extraction view (handles long queries)
CREATE OR REPLACE VIEW v_query_digests AS
SELECT 
  left(query, 100) AS query_prefix,
  md5(query) AS query_hash,
  COUNT(*) AS occurrences,
  SUM(calls) AS total_calls,
  SUM(total_time) AS total_duration_ms,
  AVG(mean_time) AS avg_mean_time
FROM pg_stat_statements
GROUP BY query_hash, query_prefix
ORDER BY total_duration_ms DESC;
```

### Reset and Maintenance

```sql
-- Reset statistics for all databases
SELECT pg_stat_statements_reset();

-- Reset for specific database
SELECT pg_stat_statements_reset(userid => NULL, dbid => 16384, queryid => NULL);

-- Monitor pg_stat_statements memory usage
SELECT 
  pg_size_pretty(pg_total_relation_size('pg_stat_statements'))
  AS stats_size;

-- View current configuration
SHOW pg_stat_statements.max;
SHOW pg_stat_statements.track;
SHOW pg_stat_statements.track_utility;
```

## 222. Dynamic log_min_duration_statement Configuration

### Runtime Adjustment

```sql
-- Set for current session only (no restart needed)
SET log_min_duration_statement = 1000;  -- log queries > 1 second

-- Set at connection level in connection string
psql "postgresql://user:pass@host/db?options=-c%20log_min_duration_statement=500"

-- Persist in postgresql.conf
ALTER SYSTEM SET log_min_duration_statement = 1000;
SELECT pg_reload_conf();  -- reload without restart

-- Create role-specific setting
ALTER ROLE application_user SET log_min_duration_statement = 500;
ALTER ROLE analytics_user SET log_min_duration_statement = 100;
```

### Adaptive Logging

```sql
-- Create function for dynamic threshold adjustment
CREATE OR REPLACE FUNCTION adjust_slow_query_threshold()
RETURNS void AS $$
DECLARE
  avg_query_time numeric;
  new_threshold int;
BEGIN
  SELECT AVG(mean_time)
  INTO avg_query_time
  FROM pg_stat_statements;
  
  new_threshold := GREATEST(500, (avg_query_time * 2)::int);
  
  ALTER SYSTEM SET log_min_duration_statement = new_threshold;
  PERFORM pg_reload_conf();
  
  RAISE NOTICE 'Threshold adjusted to % ms', new_threshold;
END;
$$ LANGUAGE plpgsql;

-- Schedule periodic adjustment
-- Add to cron job or external scheduler
SELECT adjust_slow_query_threshold();
```

### Log Analysis

```sql
-- Parse PostgreSQL logs for slow queries
-- Query CSV log format (requires log_statement = 'all')
CREATE TEMP TABLE slow_queries AS
SELECT 
  log_time,
  user_name,
  database_name,
  duration,
  query
FROM csv_read('postgresql.log', 
  types => 'timestamp, text, text, interval, text')
WHERE duration > INTERVAL '1 second'
ORDER BY duration DESC;
```

## 223. Monitor Query Execution Times Per User

### User Activity Tracking

```sql
-- Create extended view for per-user statistics
CREATE OR REPLACE VIEW v_user_performance AS
SELECT 
  u.usename,
  COUNT(DISTINCT ps.query) AS unique_queries,
  SUM(ps.calls) AS total_calls,
  SUM(ps.total_time) AS total_time_ms,
  AVG(ps.mean_time) AS avg_time_ms,
  MAX(ps.max_time) AS max_time_ms,
  SUM(ps.rows) AS rows_returned,
  SUM(ps.shared_blks_hit + ps.local_blks_hit) AS cache_hits,
  SUM(ps.shared_blks_read + ps.local_blks_read) AS cache_misses
FROM pg_stat_statements ps
JOIN pg_user u ON ps.userid = u.usesysid
GROUP BY ps.userid, u.usename
ORDER BY total_time_ms DESC;

-- Active sessions per user
CREATE OR REPLACE VIEW v_user_active_sessions AS
SELECT 
  u.usename,
  COUNT(*) AS active_connections,
  COUNT(CASE WHEN state = 'active' THEN 1 END) AS active_queries,
  COUNT(CASE WHEN state = 'idle in transaction' THEN 1 END) AS idle_in_transaction,
  MAX(NOW() - query_start) AS longest_query_duration,
  MAX(NOW() - xact_start) AS longest_transaction_duration
FROM pg_stat_activity psa
JOIN pg_user u ON psa.usesysid = u.usesysid
WHERE u.usename != 'postgres'
GROUP BY u.usename;
```

### Real-time Per-User Metrics

```sql
-- Live query times per user (requires logging)
CREATE TEMP TABLE user_query_times (
  query_time timestamp,
  username text,
  duration numeric,
  query text
);

-- Function to aggregate per-user stats
CREATE OR REPLACE FUNCTION get_user_query_stats(
  p_username text DEFAULT NULL)
RETURNS TABLE(
  username text,
  query_count bigint,
  total_duration numeric,
  avg_duration numeric,
  p95_duration numeric,
  p99_duration numeric
) AS $$
SELECT 
  u.usename,
  COUNT(*),
  SUM(ps.total_time),
  AVG(ps.mean_time),
  PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY ps.mean_time),
  PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY ps.mean_time)
FROM pg_stat_statements ps
JOIN pg_user u ON ps.userid = u.usesysid
WHERE (p_username IS NULL OR u.usename = p_username)
GROUP BY u.usename;
$$ LANGUAGE SQL;

SELECT * FROM get_user_query_stats('app_user');
```

## 224. Detect Functions Causing High CPU Load

### Function Performance Monitoring

```sql
-- View function execution statistics
CREATE OR REPLACE VIEW v_function_performance AS
SELECT 
  schemaname,
  funcname,
  calls,
  total_time,
  self_time,
  mean_time,
  stddev_time,
  max_time,
  ROUND(100.0 * self_time / total_time, 2) AS self_percent
FROM pg_stat_user_functions
WHERE calls > 0
ORDER BY self_time DESC;

-- Identify CPU-intensive functions
SELECT 
  schemaname,
  funcname,
  calls,
  total_time,
  self_time,
  mean_time
FROM pg_stat_user_functions
WHERE self_time > 1000  -- > 1 second
ORDER BY self_time DESC;

-- Compare function overhead
CREATE OR REPLACE VIEW v_function_efficiency AS
SELECT 
  schemaname,
  funcname,
  calls,
  total_time,
  self_time,
  (total_time - self_time) AS overhead_time,
  ROUND(100.0 * self_time / NULLIF(total_time, 0), 2) AS efficiency_percent,
  ROUND(self_time::numeric / calls, 2) AS per_call_time_ms
FROM pg_stat_user_functions
WHERE calls > 100
ORDER BY efficiency_percent ASC;
```

### Profiling Expensive Functions

```sql
-- Create function profiling table
CREATE TABLE function_profile_log (
  log_id serial,
  funcname text,
  start_time timestamp,
  end_time timestamp,
  duration interval,
  cpu_ticks bigint,
  memory_used bigint
);

-- Wrap function call with profiling
CREATE OR REPLACE FUNCTION profile_function_execution(
  p_schema text,
  p_func text)
RETURNS TABLE(
  func_name text,
  execution_count bigint,
  total_ms numeric,
  avg_ms numeric,
  max_ms numeric,
  cpu_intensive boolean
) AS $$
SELECT 
  p_schema || '.' || p_func,
  calls,
  total_time,
  mean_time,
  max_time,
  (self_time / NULLIF(total_time, 0)) > 0.8  -- > 80% self time
FROM pg_stat_user_functions
WHERE schemaname = p_schema AND funcname = p_func;
$$ LANGUAGE SQL;

-- Reset function statistics
SELECT pg_stat_reset_shared('functions');
```

## 225. Monitor Replication Delay (Bytes and Time)

### Replication Lag Monitoring

```sql
-- Primary side: check replica distance
CREATE OR REPLACE VIEW v_replication_lag AS
SELECT 
  client_addr,
  state,
  sync_state,
  write_lag,
  flush_lag,
  replay_lag,
  flush_lsn,
  replay_lsn,
  write_lsn,
  backend_start,
  NOW() - backend_start AS connection_duration
FROM pg_stat_replication;

-- Calculate byte distance (primary)
CREATE OR REPLACE VIEW v_replication_bytes_lag AS
SELECT 
  client_addr,
  state,
  sync_state,
  pg_wal_lsn_diff(pg_current_wal_lsn(), flush_lsn) AS bytes_behind_flush,
  pg_wal_lsn_diff(pg_current_wal_lsn(), replay_lsn) AS bytes_behind_replay,
  pg_wal_lsn_diff(flush_lsn, replay_lsn) AS bytes_between_flush_replay
FROM pg_stat_replication;

-- Replica side: check lag from primary
CREATE OR REPLACE VIEW v_standby_lag AS
SELECT 
  pg_last_wal_receive_lsn() AS last_received_lsn,
  pg_last_wal_replay_lsn() AS last_replayed_lsn,
  pg_wal_lsn_diff(
    pg_last_wal_receive_lsn(), 
    pg_last_wal_replay_lsn()
  ) AS bytes_lag,
  (SELECT now() - pg_postmaster_start_time()) AS uptime
FROM pg_is_in_recovery();
```

### Advanced Lag Analysis

```sql
-- Create replication lag history table
CREATE TABLE replication_lag_history (
  sample_time timestamp DEFAULT now(),
  server_name text,
  bytes_lag bigint,
  time_lag interval,
  slot_name text
);

-- Function to track lag over time
CREATE OR REPLACE FUNCTION track_replication_lag()
RETURNS void AS $$
INSERT INTO replication_lag_history (server_name, bytes_lag, time_lag, slot_name)
SELECT 
  client_addr::text,
  pg_wal_lsn_diff(pg_current_wal_lsn(), replay_lsn)::bigint,
  replay_lag,
  slot_name
FROM pg_stat_replication;
$$ LANGUAGE SQL;

-- Alert on high lag
CREATE OR REPLACE FUNCTION check_replication_alert()
RETURNS TABLE(alert_level text, message text, bytes_lag bigint, time_lag interval) AS $$
SELECT 
  CASE 
    WHEN pg_wal_lsn_diff(pg_current_wal_lsn(), replay_lsn) > 1073741824 THEN 'CRITICAL'
    WHEN pg_wal_lsn_diff(pg_current_wal_lsn(), replay_lsn) > 536870912 THEN 'WARNING'
    ELSE 'OK'
  END,
  'Replication lag detected',
  pg_wal_lsn_diff(pg_current_wal_lsn(), replay_lsn)::bigint,
  replay_lag
FROM pg_stat_replication;
$$ LANGUAGE SQL;
```

## 226. Monitor Lock Contention Continuously

### Lock Monitoring Views

```sql
-- Current lock situation
CREATE OR REPLACE VIEW v_locks_held AS
SELECT 
  pid,
  usename,
  datname,
  locktype,
  database,
  relation::regclass,
  page,
  tuple,
  virtualxid,
  mode,
  granted,
  query_start,
  NOW() - query_start AS query_duration
FROM pg_locks l
LEFT JOIN pg_stat_activity a ON l.pid = a.pid
ORDER BY query_start;

-- Identify blocking queries
CREATE OR REPLACE VIEW v_blocking_queries AS
SELECT 
  blocked_locks.pid AS blocked_pid,
  blocked_activity.usename AS blocked_user,
  blocking_locks.pid AS blocking_pid,
  blocking_activity.usename AS blocking_user,
  blocked_activity.query AS blocked_query,
  blocking_activity.query AS blocking_query,
  blocked_activity.application_name AS blocked_application,
  blocking_activity.application_name AS blocking_application,
  NOW() - blocking_activity.query_start AS blocking_duration
FROM pg_locks blocked_locks
JOIN pg_stat_activity blocked_activity ON blocked_activity.pid = blocked_locks.pid
JOIN pg_locks blocking_locks ON blocking_locks.locktype = blocked_locks.locktype
  AND blocking_locks.database IS NOT DISTINCT FROM blocked_locks.database
  AND blocking_locks.relation IS NOT DISTINCT FROM blocked_locks.relation
  AND blocking_locks.page IS NOT DISTINCT FROM blocked_locks.page
  AND blocking_locks.tuple IS NOT DISTINCT FROM blocked_locks.tuple
  AND blocking_locks.virtualxid IS NOT DISTINCT FROM blocked_locks.virtualxid
  AND blocking_locks.transactionid IS NOT DISTINCT FROM blocked_locks.transactionid
  AND blocking_locks.pid != blocked_locks.pid
JOIN pg_stat_activity blocking_activity ON blocking_activity.pid = blocking_locks.pid
WHERE NOT blocked_locks.granted;

-- Contention by table
CREATE OR REPLACE VIEW v_table_lock_contention AS
SELECT 
  schemaname,
  tablename,
  COUNT(*) AS total_locks,
  COUNT(CASE WHEN mode = 'AccessExclusiveLock' THEN 1 END) AS exclusive_locks,
  COUNT(CASE WHEN NOT granted THEN 1 END) AS waiting_locks,
  array_agg(DISTINCT mode) AS lock_modes
FROM pg_locks l
LEFT JOIN pg_stat_user_tables t ON l.relation = t.relid
WHERE schemaname IS NOT NULL
GROUP BY schemaname, tablename
HAVING COUNT(*) > 1
ORDER BY total_locks DESC;
```

### Continuous Lock Monitoring

```sql
-- Create lock history table
CREATE TABLE lock_contention_history (
  sample_time timestamp DEFAULT now(),
  blocked_pid integer,
  blocking_pid integer,
  blocked_query text,
  blocking_query text,
  duration_seconds numeric
);

-- Periodic logging function
CREATE OR REPLACE FUNCTION log_lock_contention()
RETURNS void AS $$
INSERT INTO lock_contention_history 
  (blocked_pid, blocking_pid, blocked_query, blocking_query, duration_seconds)
SELECT 
  blocked_pid,
  blocking_pid,
  blocked_query,
  blocking_query,
  EXTRACT(EPOCH FROM blocking_duration)
FROM v_blocking_queries
WHERE blocking_duration > INTERVAL '5 seconds';
$$ LANGUAGE SQL;

-- Get lock wait statistics
CREATE OR REPLACE FUNCTION get_lock_wait_stats()
RETURNS TABLE(
  blocked_user text,
  blocking_user text,
  total_waits bigint,
  avg_wait_seconds numeric,
  max_wait_seconds numeric
) AS $$
SELECT 
  blocked_user,
  blocking_user,
  COUNT(*),
  AVG(duration_seconds),
  MAX(duration_seconds)
FROM lock_contention_history
WHERE sample_time > now() - INTERVAL '1 hour'
GROUP BY blocked_user, blocking_user
ORDER BY total_waits DESC;
$$ LANGUAGE SQL;
```

## 227. Set Up Alerts for Autovacuum Freezes

### Freeze Monitoring

```sql
-- Monitor XID age
CREATE OR REPLACE VIEW v_xid_age_status AS
SELECT 
  datname,
  age(datfrozenxid) AS oldest_xid_age,
  2147483647 - txids_age AS xids_until_wraparound,
  ROUND(100.0 * age(datfrozenxid) / 2147483647, 2) AS wraparound_percentage,
  CASE 
    WHEN age(datfrozenxid) > 1900000000 THEN 'CRITICAL'
    WHEN age(datfrozenxid) > 1500000000 THEN 'WARNING'
    WHEN age(datfrozenxid) > 1000000000 THEN 'CAUTION'
    ELSE 'OK'
  END AS status
FROM pg_database
WHERE datname NOT IN ('template0', 'template1', 'postgres');

-- Per-table freeze age
CREATE OR REPLACE VIEW v_table_freeze_age AS
SELECT 
  s.schemaname,
  s.relname,
  age(c.relfrozenxid) AS table_xid_age,
  2147483647 - age(c.relfrozenxid) AS xids_until_freeze,
  s.n_live_tup,
  s.n_dead_tup,
  CASE 
    WHEN age(c.relfrozenxid) > 1900000000 THEN 'CRITICAL'
    WHEN age(c.relfrozenxid) > 1500000000 THEN 'WARNING'
    ELSE 'OK'
  END AS freeze_status,
  s.last_vacuum,
  s.last_autovacuum,
  NOW() - s.last_autovacuum AS time_since_autovacuum
FROM pg_stat_user_tables s
JOIN pg_class c ON c.oid = s.relid
ORDER BY table_xid_age DESC;
```

### Alert Configuration

```sql
-- Create alert table
CREATE TABLE autovacuum_alerts (
  alert_id serial PRIMARY KEY,
  alert_time timestamp DEFAULT now(),
  table_name text,
  current_age bigint,
  alert_type text,
  severity text,
  acknowledged boolean DEFAULT false
);

-- Alert function
CREATE OR REPLACE FUNCTION check_freeze_alerts()
RETURNS TABLE(
  severity text,
  table_name text,
  xid_age bigint,
  message text
) AS $$
SELECT 
  CASE 
    WHEN age(c.relfrozenxid) > 1900000000 THEN 'CRITICAL'
    WHEN age(c.relfrozenxid) > 1500000000 THEN 'WARNING'
    ELSE NULL
  END,
  s.schemaname || '.' || s.relname,
  age(c.relfrozenxid),
  'Table ' || s.schemaname || '.' || s.relname || 
    ' freeze XID age: ' || age(c.relfrozenxid)
FROM pg_stat_user_tables s
JOIN pg_class c ON c.oid = s.relid
WHERE age(c.relfrozenxid) > 1500000000
ORDER BY age(c.relfrozenxid) DESC;
$$ LANGUAGE SQL;

-- Force vacuum for critical tables
CREATE OR REPLACE PROCEDURE force_vacuum_critical_tables()
AS $$
DECLARE
  r RECORD;
BEGIN
  FOR r IN 
    SELECT s.schemaname, s.relname 
    FROM pg_stat_user_tables s
    JOIN pg_class c ON c.oid = s.relid
    WHERE age(c.relfrozenxid) > 1900000000
  LOOP
    EXECUTE 'VACUUM FREEZE ' || quote_ident(r.schemaname) || '.' || quote_ident(r.relname);
    RAISE NOTICE 'Forced VACUUM FREEZE on %.%', r.schemaname, r.relname;
  END LOOP;
END;
$$ LANGUAGE plpgsql;
```

## 228. Monitor Index Bloat Proactively

### Index Bloat Detection

```sql
-- Estimate index bloat using pgstattuple
CREATE EXTENSION IF NOT EXISTS pgstattuple;

-- Index bloat estimation (can be slow)
CREATE OR REPLACE FUNCTION get_index_bloat(
  p_schema text DEFAULT 'public',
  p_threshold_percent numeric DEFAULT 20)
RETURNS TABLE(
  schema_name text,
  table_name text,
  index_name text,
  index_size_mb numeric,
  dead_ratio_percent numeric,
  bloat_status text
) AS $$
SELECT 
  schemaname AS schema_name,
  relname AS table_name,
  indexrelname AS index_name,
  (pg_relation_size(indexrelid) / 1048576.0)::numeric AS index_size_mb,
  ROUND((100 - (pgstatindex(indexrelid::regclass)).avg_leaf_density)::numeric, 2) AS dead_ratio_percent,
  CASE 
    WHEN (100 - (pgstatindex(indexrelid::regclass)).avg_leaf_density) > 50 THEN 'CRITICAL'
    WHEN (100 - (pgstatindex(indexrelid::regclass)).avg_leaf_density) > 20 THEN 'HIGH'
    WHEN (100 - (pgstatindex(indexrelid::regclass)).avg_leaf_density) > 10 THEN 'MODERATE'
    ELSE 'LOW'
  END AS bloat_status
FROM pg_stat_user_indexes
WHERE pg_relation_size(indexrelid) > 1048576  -- > 1 MB
ORDER BY dead_ratio_percent DESC;
$$ LANGUAGE SQL;

-- Faster bloat estimation (heuristic)
CREATE OR REPLACE VIEW v_index_bloat_heuristic AS
SELECT 
  schemaname,
  relname AS tablename,
  indexrelname AS indexname,
  idx_scan,
  idx_tup_read,
  idx_tup_fetch,
  pg_size_pretty(pg_relation_size(indexrelid)) AS index_size,
  CASE 
    WHEN idx_scan = 0 THEN 'UNUSED - Remove'
    WHEN idx_tup_read = 0 THEN 'EMPTY'
    WHEN idx_scan > 0 AND (idx_tup_read::float / idx_scan) > 1000 THEN 'POOR_SELECTIVITY'
    ELSE 'OK'
  END AS status
FROM pg_stat_user_indexes
ORDER BY pg_relation_size(indexrelid) DESC;
```

### Proactive Reindexing

```sql
-- Index maintenance tracking
CREATE TABLE index_maintenance_log (
  log_id serial,
  index_name text,
  reindex_time timestamp,
  size_before_mb numeric,
  size_after_mb numeric,
  duration_seconds numeric
);

-- Function for safe concurrent reindex
CREATE OR REPLACE PROCEDURE reindex_bloated_indexes(
  p_bloat_threshold numeric DEFAULT 30)
AS $$
DECLARE
  r RECORD;
  v_start_time timestamp;
  v_size_before numeric;
  v_size_after numeric;
BEGIN
  FOR r IN 
    SELECT schemaname, indexname, indexrelname
    FROM pg_stat_user_indexes i
    JOIN pg_indexes ON schemaname = pg_indexes.schemaname 
      AND tablename = pg_indexes.tablename
    WHERE pg_relation_size(indexrelid) > 1048576
    ORDER BY pg_relation_size(indexrelid) DESC
  LOOP
    v_start_time := now();
    v_size_before := pg_relation_size(r.indexrelname) / 1048576.0;
    
    EXECUTE 'REINDEX INDEX CONCURRENTLY ' || r.indexname;
    
    v_size_after := pg_relation_size(r.indexrelname) / 1048576.0;
    
    INSERT INTO index_maintenance_log 
      (index_name, reindex_time, size_before_mb, size_after_mb, duration_seconds)
    VALUES 
      (r.indexname, v_start_time, v_size_before, v_size_after, 
       EXTRACT(EPOCH FROM (now() - v_start_time)));
    
    RAISE NOTICE 'Reindexed %: % MB -> % MB', r.indexname, v_size_before, v_size_after;
  END LOOP;
END;
$$ LANGUAGE plpgsql;

-- Get reindex recommendations
CREATE OR REPLACE FUNCTION get_reindex_candidates()
RETURNS TABLE(
  index_name text,
  current_size_mb numeric,
  bloat_status text,
  recommendation text
) AS $$
SELECT 
  indexname,
  pg_relation_size(indexrelid) / 1048576.0,
  CASE 
    WHEN idx_scan = 0 THEN 'DROP'
    WHEN (100 - (pgstatindex(indexrelid::regclass)).avg_leaf_density) > 30 THEN 'REINDEX'
    ELSE 'MONITOR'
  END,
  CASE 
    WHEN idx_scan = 0 THEN 'Unused - consider dropping'
    WHEN (100 - (pgstatindex(indexrelid::regclass)).avg_leaf_density) > 30 THEN 'Run REINDEX CONCURRENTLY'
    ELSE 'No action needed'
  END
FROM pg_stat_user_indexes
ORDER BY pg_relation_size(indexrelid) DESC;
$$ LANGUAGE SQL;
```

## 229. Detect Runaway Queries Before They Finish

### Early Query Detection

```sql
-- Monitor queries exceeding time threshold
CREATE OR REPLACE VIEW v_long_running_queries AS
SELECT 
  pid,
  usename,
  application_name,
  client_addr,
  query_start,
  NOW() - query_start AS query_duration,
  query,
  state,
  state_change,
  wait_event_type,
  wait_event,
  CASE 
    WHEN NOW() - query_start > INTERVAL '1 hour' THEN 'CRITICAL'
    WHEN NOW() - query_start > INTERVAL '30 minutes' THEN 'WARNING'
    WHEN NOW() - query_start > INTERVAL '5 minutes' THEN 'CAUTION'
    ELSE 'NORMAL'
  END AS severity
FROM pg_stat_activity
WHERE query_start IS NOT NULL 
  AND state IN ('active', 'idle in transaction')
  AND NOW() - query_start > INTERVAL '1 minute'
ORDER BY query_duration DESC;

-- Track query progress
CREATE OR REPLACE VIEW v_query_progress AS
SELECT 
  pid,
  usename,
  query_start,
  NOW() - query_start AS elapsed_time,
  backend_xmin,
  backend_xid,
  CASE 
    WHEN wait_event_type = 'IO' THEN 'Disk I/O'
    WHEN wait_event_type = 'Lock' THEN 'Lock Contention'
    WHEN wait_event_type = 'LWLock' THEN 'Light Lock'
    WHEN state = 'idle in transaction' THEN 'Idle in Transaction'
    WHEN state = 'active' THEN 'Processing'
    ELSE state
  END AS current_activity
FROM pg_stat_activity
WHERE query_start IS NOT NULL;
```

### Automatic Runaway Prevention

```sql
########################## Create alert on query exceeding threshold
CREATE OR REPLACE FUNCTION alert_on_runaway_queries()
RETURNS TABLE(
  alert_level text,
  pid integer,
  username text,
  duration interval,
  query text
) AS $$
SELECT 
  CASE 
    WHEN NOW() - query_start > INTERVAL '2 hours' THEN 'CRITICAL - KILL'
    WHEN NOW() - query_start > INTERVAL '1 hour' THEN 'CRITICAL'
    WHEN NOW() - query_start > INTERVAL '30 minutes' THEN 'WARNING'
    ELSE 'INFO'
  END,
  pid,
  usename,
  NOW() - query_start,
  query
FROM pg_stat_activity
WHERE query_start IS NOT NULL 
  AND state IN ('active', 'idle in transaction')
  AND NOW() - query_start > INTERVAL '30 minutes'
ORDER BY query_start;
$$ LANGUAGE SQL;

################# Safe query termination#####################################
CREATE OR REPLACE PROCEDURE terminate_runaway_query(
  p_pid integer,
  p_max_duration interval DEFAULT '2 hours')
AS $$
DECLARE
  v_duration interval;
BEGIN
  SELECT NOW() - query_start INTO v_duration
  FROM pg_stat_activity
  WHERE pid = p_pid;
  
  IF v_duration > p_max_duration THEN
    PERFORM pg_terminate_backend(p_pid);
    RAISE NOTICE 'Terminated query pid % running for %', p_pid, v_duration;
  ELSE
    RAISE NOTICE 'Query % only running for %, not terminating', p_pid, v_duration;
  END IF;
END;
$$ LANGUAGE plpgsql;

################# Auto-terminate critical runaway queries#####################################
CREATE OR REPLACE PROCEDURE auto_terminate_critical_queries(
  p_threshold interval DEFAULT '2 hours')
AS $$
DECLARE
  r RECORD;
BEGIN
  FOR r IN 
    SELECT pid 
    FROM pg_stat_activity
    WHERE query_start IS NOT NULL
      AND NOW() - query_start > p_threshold
  LOOP
    PERFORM pg_terminate_backend(r.pid);
    RAISE WARNING 'Terminated critical runaway query pid %', r.pid;
  END LOOP;
END;
$$ LANGUAGE plpgsql;
```

## 230. Analyze System Catalog Usage by Queries

### Catalog Access Monitoring

```sql
-- Track queries accessing system catalogs
CREATE OR REPLACE VIEW v_catalog_queries AS
SELECT 
  pid,
  usename,
  query_start,
  query,
  CASE 
    WHEN query ~* 'pg_class|pg_attribute|pg_index' THEN 'Schema Metadata'
    WHEN query ~* 'pg_proc|pg_type|pg_operator' THEN 'Function/Type Info'
    WHEN query ~* 'pg_namespace|pg_database' THEN 'Database Structure'
    WHEN query ~* 'pg_constraint|pg_trigger' THEN 'Constraint/Trigger'
    ELSE 'Other'
  END AS catalog_type
FROM pg_stat_activity
WHERE query ~* '(pg_class|pg_attribute|pg_index|pg_proc|pg_type|pg_namespace|pg_constraint)'
  AND query NOT LIKE '%pg_stat%'
ORDER BY query_start;

##########################catalog table access frequency#####################################
CREATE OR REPLACE VIEW v_catalog_access_frequency AS
SELECT 
  schemaname,
  relname AS tablename,
  seq_scan,
  seq_tup_read,
  idx_scan,
  idx_tup_fetch,
  n_tup_ins + n_tup_upd + n_tup_del AS modifications
FROM pg_stat_user_tables
ORDER BY seq_scan DESC;

##########################Identify expensive catalog lookups#####################################
CREATE OR REPLACE FUNCTION analyze_catalog_overhead()
RETURNS TABLE(
  query_pattern text,
  occurrence_count bigint,
  total_execution_time numeric,
  avg_execution_time numeric,
  catalog_table text
) AS $
SELECT 
  CASE 
    WHEN query ~* 'information_schema' THEN 'information_schema query'
    WHEN query ~* 'pg_class' THEN 'pg_class lookup'
    WHEN query ~* 'pg_attribute' THEN 'pg_attribute lookup'
    WHEN query ~* 'pg_proc' THEN 'pg_proc lookup'
    WHEN query ~* 'pg_type' THEN 'pg_type lookup'
    ELSE 'other catalog'
  END,
  COUNT(*),
  SUM(total_exec_time),
  AVG(mean_exec_time),
  'catalog'
FROM pg_stat_statements
WHERE query ILIKE ANY(ARRAY[
  '%information_schema%',
  '%pg_class%',
  '%pg_attribute%',
  '%pg_proc%',
  '%pg_type%'
])
  GROUP BY query
ORDER BY total_exec_time DESC;
$ LANGUAGE SQL;
```

### System Catalog Performance Tuning

```sql
-- Find inefficient catalog scans
CREATE OR REPLACE VIEW v_catalog_scan_efficiency AS
SELECT 
  schemaname,
  relname AS tablename,
  seq_scan,
  seq_tup_read,
  CASE 
    WHEN seq_scan = 0 THEN 0
    ELSE seq_tup_read::float / seq_scan
  END AS avg_rows_per_scan,
  idx_scan,
  CASE 
    WHEN seq_scan > idx_scan * 10 THEN 'Sequential dominates - consider index'
    WHEN idx_scan = 0 AND seq_scan > 0 THEN 'No indexes used'
    WHEN seq_scan > 0 THEN 'Mixed scan types'
    ELSE 'Index preferred'
  END AS recommendation
FROM pg_stat_user_tables
WHERE relname LIKE 'pg_%'
ORDER BY seq_scan DESC;

-- Monitor catalog table bloat
CREATE OR REPLACE VIEW v_catalog_bloat_status AS
SELECT 
  schemaname,
  tablename,
  pg_size_pretty(pg_relation_size(schemaname||'.'||tablename)) AS table_size,
  n_live_tup,
  n_dead_tup,
  ROUND(100.0 * n_dead_tup / NULLIF(n_live_tup + n_dead_tup, 0), 2) AS dead_ratio,
  last_vacuum,
  last_autovacuum,
  CASE 
    WHEN n_dead_tup::float / (n_live_tup + 1) > 0.2 THEN 'NEEDS VACUUM'
    WHEN n_dead_tup::float / (n_live_tup + 1) > 0.1 THEN 'MONITOR'
    ELSE 'OK'
  END AS vacuum_status
FROM pg_stat_user_tables
WHERE schemaname IN ('pg_catalog', 'information_schema')
ORDER BY n_dead_tup DESC;

-- Query compilation overhead tracking
CREATE OR REPLACE VIEW v_query_planning_overhead AS
SELECT 
  LEFT(query, 80) AS query_snippet,
  calls,
  total_plan_time,
  total_exec_time,
  mean_plan_time,
  mean_exec_time,
  ROUND(100.0 * total_plan_time / (total_plan_time + total_exec_time), 2) AS planning_percent
FROM pg_stat_statements
WHERE total_plan_time > 0
ORDER BY total_plan_time DESC
LIMIT 20;
```

### Optimization Strategies

```sql
-- Identify queries with high catalog dependency
CREATE OR REPLACE PROCEDURE optimize_catalog_queries()
AS $
DECLARE
  v_rec RECORD;
BEGIN
  -- Detect information_schema queries that could be replaced
  FOR v_rec IN 
    SELECT DISTINCT LEFT(query, 100) as query_sample
    FROM pg_stat_statements
    WHERE query ILIKE '%information_schema%'
      AND calls > 100
  LOOP
    RAISE NOTICE 'High-frequency catalog query: %', v_rec.query_sample;
    RAISE NOTICE 'Consider: caching results, moving to startup, or using pg_stat_* views';
  END LOOP;
  
  -- Analyze pg_class access patterns
  FOR v_rec IN 
    SELECT schemaname, tablename
    FROM pg_stat_user_tables
    WHERE seq_scan > idx_scan * 100
      AND tablename LIKE 'pg_%'
  LOOP
    RAISE NOTICE 'High sequential scan on %: consider indexes', v_rec.tablename;
  END LOOP;
END;
$ LANGUAGE plpgsql;

-- Cache frequently accessed catalog metadata
CREATE TABLE catalog_cache (
  cache_key text PRIMARY KEY,
  cache_value jsonb,
  last_updated timestamp,
  ttl_seconds integer
);

-- Function to manage catalog cache
CREATE OR REPLACE FUNCTION get_cached_catalog_info(
  p_key text,
  p_ttl_seconds integer DEFAULT 3600)
RETURNS jsonb AS $
DECLARE
  v_value jsonb;
  v_age interval;
BEGIN
  SELECT cache_value, NOW() - last_updated INTO v_value, v_age
  FROM catalog_cache
  WHERE cache_key = p_key;
  
  IF v_value IS NOT NULL AND v_age < (p_ttl_seconds || ' seconds')::interval THEN
    RETURN v_value;
  END IF;
  
  -- Cache miss - would fetch fresh data
  RETURN NULL;
END;
$ LANGUAGE plpgsql;
```

## Summary: Advanced Monitoring Best Practices

### Key Metrics to Track

- **pg_stat_statements**: Top queries by total time, CPU, and I/O
- **Per-user performance**: Connection count, query patterns, resource usage
- **Function overhead**: CPU-intensive functions and their efficiency
- **Replication lag**: Both in bytes and time for all replicas
- **Lock contention**: Blocking queries and their impact
- **XID age**: Distance to wraparound and freeze operations
- **Index health**: Bloat levels and unused indexes
- **Query duration**: Runaway detection with alerts
- **Catalog usage**: Optimization opportunities

### Monitoring Schedule

```sql
-- Example: Create monitoring job runs
-- Every 5 minutes: short-running checks
SELECT alert_on_runaway_queries();
SELECT get_lock_wait_stats();

-- Every 15 minutes: system health
SELECT * FROM v_long_running_queries;
SELECT * FROM check_freeze_alerts();

-- Hourly: detailed analysis
SELECT * FROM get_user_query_stats();
SELECT * FROM get_reindex_candidates();

-- Daily: capacity planning
SELECT * FROM v_table_freeze_age;
SELECT * FROM analyze_catalog_overhead();
```

### Alert Thresholds

- Query duration > 30 minutes: WARNING
- Query duration > 2 hours: CRITICAL
- Lock wait > 5 seconds: WARNING
- XID age > 1.5B: WARNING, > 1.9B: CRITICAL
- Replication lag > 1 GB: WARNING, > 5 GB: CRITICAL
- Index bloat > 20%: WARNING, > 50%: CRITICAL
- Cache hit ratio < 99%: Investigate
- Function self time > 80%: Optimization candidate