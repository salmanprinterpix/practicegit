171. Using Partial Indexes to Reduce Index Size
Partial indexes index only a subset of rows that match a WHERE condition, significantly reducing index size and maintenance overhead.
sql-- Index only active records
CREATE INDEX idx_users_active 
ON users (email) 
WHERE active = true;

-- Index only recent orders
CREATE INDEX idx_orders_recent 
ON orders (created_at, status) 
WHERE created_at >= '2024-01-01';

-- Index non-null values only
CREATE INDEX idx_products_sku 
ON products (sku) 
WHERE sku IS NOT NULL;

-- Index specific status values
CREATE INDEX idx_tasks_pending 
ON tasks (priority, created_at) 
WHERE status IN ('pending', 'in_progress');

-- Combining conditions
CREATE INDEX idx_accounts_premium 
ON accounts (user_id, expires_at) 
WHERE subscription_type = 'premium' 
  AND expires_at > CURRENT_DATE;
172. Tuning GIN Indexes for Full-Text Search
GIN (Generalized Inverted Index) indexes require careful tuning for optimal full-text search performance.
sql-- Basic GIN index for text search
CREATE INDEX idx_articles_search 
ON articles 
USING gin(to_tsvector('english', title || ' ' || content));

-- Configure GIN parameters
ALTER INDEX idx_articles_search 
SET (fastupdate = off);  -- Disable pending list for consistent performance

-- Set work memory for GIN operations
SET maintenance_work_mem = '256MB';  -- For index creation
SET gin_pending_list_limit = '4MB';  -- Size of pending list

-- Use gin_trgm_ops for trigram similarity
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE INDEX idx_names_trgm 
ON users 
USING gin(name gin_trgm_ops);

-- Optimize for phrase search
CREATE INDEX idx_documents_phrase 
ON documents 
USING gin(to_tsvector('english', content)) 
WITH (fastupdate = false);

-- Multi-column GIN index
CREATE INDEX idx_products_search 
ON products 
USING gin((
  setweight(to_tsvector('english', name), 'A') ||
  setweight(to_tsvector('english', description), 'B') ||
  setweight(to_tsvector('english', tags), 'C')
));
173. Using BRIN Indexes for Time-Series Data
BRIN (Block Range Index) indexes are perfect for large tables with naturally ordered data like time-series.
sql-- Basic BRIN index for timestamp
CREATE INDEX idx_logs_timestamp_brin 
ON logs 
USING brin(created_at);

-- Adjust pages per range for better granularity
CREATE INDEX idx_metrics_time_brin 
ON metrics 
USING brin(timestamp) 
WITH (pages_per_range = 32);  -- Default is 128

-- Multi-column BRIN index
CREATE INDEX idx_events_brin 
ON events 
USING brin(event_date, sensor_id);

-- BRIN with custom operator class
CREATE INDEX idx_temperatures_brin 
ON temperature_readings 
USING brin(reading_time timestamp_minmax_ops, temperature float8_minmax_ops);

-- Maintenance for BRIN indexes
VACUUM logs;  -- Update BRIN summary
ANALYZE logs;  -- Update statistics

-- Monitor BRIN effectiveness
SELECT 
  schemaname,
  tablename,
  attname,
  correlation 
FROM pg_stats 
WHERE tablename = 'logs' 
  AND attname = 'created_at';
174. Maintaining GIN Indexes for JSONB Queries
GIN indexes on JSONB columns require specific maintenance strategies.
sql-- Create GIN index for JSONB
CREATE INDEX idx_data_gin 
ON records 
USING gin(data);

-- GIN index for specific JSON path
CREATE INDEX idx_metadata_tags 
ON documents 
USING gin((metadata->'tags'));

-- jsonb_path_ops for faster containment queries
CREATE INDEX idx_config_path 
ON configurations 
USING gin(config jsonb_path_ops);

-- Maintenance routine for GIN indexes
-- Clean up pending list
SELECT gin_clean_pending_list('idx_data_gin'::regclass);

-- Monitor GIN index bloat
SELECT 
  schemaname,
  relname,
  indexrelname,
  pg_size_pretty(pg_relation_size(indexrelid)) AS index_size,
  idx_scan,
  idx_tup_read,
  idx_tup_fetch
FROM pg_stat_user_indexes
WHERE indexrelname LIKE '%gin%';

-- Rebuild bloated GIN index
REINDEX INDEX CONCURRENTLY idx_data_gin;
175. Detecting Unused Indexes
Identify indexes that are never or rarely used to reduce maintenance overhead.
sql-- Find completely unused indexes
SELECT 
  schemaname,
  relname,
  indexrelname,
  pg_size_pretty(pg_relation_size(indexrelid)) AS index_size,
  indexrelid::regclass AS index_oid
FROM pg_stat_user_indexes
WHERE idx_scan = 0
  AND indexrelname NOT LIKE 'pg_toast%'
ORDER BY pg_relation_size(indexrelid) DESC;

-- Find rarely used indexes (less than 100 scans)
SELECT 
  schemaname || '.' || relname AS qualified_table,
  indexrelname,
  pg_size_pretty(pg_relation_size(i.indexrelid)) AS index_size,
  idx_scan AS scans,
  idx_tup_read AS tuples_read,
  idx_tup_fetch AS tuples_fetched,
  pg_stat_get_blocks_fetched(i.indexrelid) - 
    pg_stat_get_blocks_hit(i.indexrelid) AS disk_reads
FROM pg_stat_user_indexes i
WHERE idx_scan < 100
ORDER BY pg_relation_size(i.indexrelid) DESC;

-- Check index usage ratio
WITH table_stats AS (
  SELECT 
    schemaname,
    relname,
    n_tup_ins + n_tup_upd + n_tup_del AS total_writes
  FROM pg_stat_user_tables
)
SELECT 
  i.schemaname,
  i.relname,
  i.indexrelname,
  i.idx_scan,
  t.total_writes,
  CASE 
    WHEN t.total_writes > 0 
    THEN ROUND(100.0 * i.idx_scan / t.total_writes, 2)
    ELSE 0 
  END AS read_write_ratio
FROM pg_stat_user_indexes i
JOIN table_stats t USING (schemaname, relname)
WHERE i.idx_scan < t.total_writes * 0.01  -- Less than 1% usage
ORDER BY t.total_writes DESC;
176. Dropping Redundant Indexes Safely
Safely remove redundant or duplicate indexes without impacting performance.
sql-- Identify duplicate indexes
WITH index_info AS (
  SELECT 
    schemaname,
    relname,
    indexrelname,
    array_agg(attname ORDER BY attnum) AS columns,
    pg_get_indexdef(indexrelid) AS indexdef,
    pg_size_pretty(pg_relation_size(indexrelid)) AS size
  FROM pg_stat_user_indexes
  JOIN pg_index USING (indexrelid)
  JOIN pg_attribute ON (attrelid = indrelid AND attnum = ANY(indkey))
  WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
  GROUP BY schemaname, relname, indexrelname, indexdef, indexrelid
)
SELECT 
  a.schemaname,
  a.relname,
  a.indexrelname AS redundant_index,
  b.indexrelname AS sufficient_index,
  a.size AS redundant_size,
  a.columns AS redundant_columns,
  b.columns AS sufficient_columns
FROM index_info a
JOIN index_info b ON (
  a.schemaname = b.schemaname 
  AND a.relname = b.relname
  AND a.indexrelname != b.indexrelname
  AND a.columns @> b.columns  -- a contains all columns of b
);

-- Safe dropping procedure
BEGIN;
-- Save current statistics
CREATE TEMP TABLE index_stats_backup AS
SELECT * FROM pg_stat_user_indexes 
WHERE indexrelname = 'idx_to_drop';

-- Drop the index
DROP INDEX CONCURRENTLY IF EXISTS idx_to_drop;

-- Monitor for performance issues
-- If problems occur, recreate:
-- CREATE INDEX CONCURRENTLY idx_to_drop ON ...

COMMIT;
177. Optimizing Composite Indexes for Query Workload
Design composite indexes based on actual query patterns and selectivity.
sql-- Analyze query patterns
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

-- Find common WHERE conditions
SELECT 
  query,
  calls,
  mean_exec_time,
  total_exec_time
FROM pg_stat_statements
WHERE query LIKE '%WHERE%'
ORDER BY calls DESC
LIMIT 20;

-- Analyze column selectivity
SELECT 
  attname,
  n_distinct,
  correlation,
  null_frac
FROM pg_stats
WHERE tablename = 'orders'
  AND attname IN ('customer_id', 'status', 'created_at');

-- Create optimized composite index
-- Order columns by selectivity (most selective first)
CREATE INDEX idx_orders_optimized 
ON orders (customer_id, created_at, status)
WHERE status != 'completed';  -- Partial index for common filter

-- Test different column orders
EXPLAIN (ANALYZE, BUFFERS) 
SELECT * FROM orders 
WHERE customer_id = 123 
  AND created_at >= '2024-01-01'
  AND status = 'pending';

-- Create covering index for index-only scans
CREATE INDEX idx_orders_covering 
ON orders (customer_id, status) 
INCLUDE (total_amount, created_at);
178. Building Indexes Concurrently Without Downtime
Create indexes without blocking writes to the table.
sql-- Basic concurrent index creation
CREATE INDEX CONCURRENTLY idx_large_table_column 
ON large_table (column_name);

-- Monitor concurrent index creation
SELECT 
  pid,
  now() - pg_stat_activity.query_start AS duration,
  query
FROM pg_stat_activity
WHERE query LIKE '%CREATE INDEX CONCURRENTLY%';

-- Check for invalid indexes (failed concurrent creation)
SELECT 
  n.nspname AS schemaname,
  c.relname AS tablename,
  i.relname AS indexrelname
FROM pg_index x
JOIN pg_class i ON i.oid = x.indexrelid
JOIN pg_class c ON c.oid = x.indrelid
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE NOT x.indisvalid;

-- Retry failed concurrent index
DROP INDEX CONCURRENTLY IF EXISTS idx_failed;
CREATE INDEX CONCURRENTLY idx_large_table_column 
ON large_table (column_name);

-- Script for safe concurrent index creation
DO $$
DECLARE
  v_start timestamp;
  v_end timestamp;
BEGIN
  v_start := clock_timestamp();
  
  -- Create index
  EXECUTE 'CREATE INDEX CONCURRENTLY idx_new ON big_table(column)';
  
  v_end := clock_timestamp();
  RAISE NOTICE 'Index created in %', v_end - v_start;
  
  -- Validate index
  IF EXISTS (
    SELECT 1
    FROM pg_class i
    JOIN pg_index x ON x.indexrelid = i.oid
    WHERE i.relname = 'idx_new'
      AND NOT x.indisvalid
  ) THEN
    RAISE EXCEPTION 'Index creation failed';
  END IF;
END $$;
179. Compressing Large Indexes for Space Savings
Techniques to reduce index size and improve cache efficiency.
sql-- Use BRIN instead of B-tree for large sequential data
-- BRIN index: ~0.01% of table size vs B-tree: ~20% of table size
DROP INDEX idx_logs_timestamp_btree;
CREATE INDEX idx_logs_timestamp_brin 
ON logs 
USING brin(timestamp) 
WITH (pages_per_range = 64);

-- Use partial indexes to exclude common values
-- Instead of indexing all rows
CREATE INDEX idx_status_partial 
ON orders (status, created_at) 
WHERE status NOT IN ('completed', 'cancelled');  -- 80% reduction

-- Use expression indexes for computed values
-- Store only necessary information
CREATE INDEX idx_email_domain 
ON users (split_part(email, '@', 2));  -- Index only domain part

-- Deduplicate B-tree indexes (PostgreSQL 13+)
CREATE INDEX idx_category_btree 
ON products (category_id) 
WITH (deduplicate_items = on);

-- Monitor index sizes
SELECT 
  schemaname,
  relname,
  indexrelname,
  pg_size_pretty(pg_total_relation_size(indexrelid)) AS total_size,
  pg_size_pretty(pg_relation_size(indexrelid)) AS index_size,
  idx_scan,
  ROUND(
    100.0 * pg_relation_size(indexrelid) / 
    pg_total_relation_size(to_regclass(format('%I.%I', schemaname, relname))), 2
  ) AS index_ratio
FROM pg_stat_user_indexes
ORDER BY pg_relation_size(indexrelid) DESC;

-- Rebuild bloated indexes
REINDEX INDEX CONCURRENTLY idx_bloated;
180. Analyzing Index Usage with pg_stat_all_indexes
Comprehensive monitoring and analysis of index performance.
sql-- Detailed index usage statistics
SELECT 
  schemaname,
  relname,
  indexrelname,
  idx_scan AS index_scans,
  idx_tup_read AS tuples_read,
  idx_tup_fetch AS tuples_fetched,
  pg_size_pretty(pg_relation_size(indexrelid)) AS index_size,
  CASE 
    WHEN idx_scan > 0 
    THEN ROUND(100.0 * idx_tup_fetch / idx_scan, 2)
    ELSE 0 
  END AS avg_tuples_per_scan
FROM pg_stat_all_indexes
WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
ORDER BY idx_scan DESC;

-- Index efficiency analysis
WITH index_stats AS (
  SELECT 
    schemaname,
    relname,
    indexrelname,
    idx_scan,
    idx_tup_read,
    idx_tup_fetch,
    pg_relation_size(indexrelid) AS index_bytes,
    pg_stat_get_blocks_fetched(indexrelid) - 
      pg_stat_get_blocks_hit(indexrelid) AS blocks_read
  FROM pg_stat_all_indexes
  WHERE idx_scan > 0
)
SELECT 
  schemaname,
  relname,
  indexrelname,
  idx_scan,
  pg_size_pretty(index_bytes) AS size,
  ROUND(100.0 * idx_tup_fetch / NULLIF(idx_tup_read, 0), 2) AS selectivity_pct,
  ROUND(blocks_read::numeric / NULLIF(idx_scan, 0), 2) AS avg_blocks_per_scan,
  CASE 
    WHEN idx_tup_fetch > 0 
    THEN ROUND(index_bytes::numeric / idx_tup_fetch, 2)
    ELSE 0 
  END AS bytes_per_tuple
FROM index_stats
ORDER BY idx_scan DESC;

-- Create monitoring view
CREATE OR REPLACE VIEW v_index_usage_analysis AS
WITH table_io AS (
  SELECT 
    schemaname,
    relname,
    heap_blks_read + heap_blks_hit AS heap_access
  FROM pg_statio_user_tables
)
SELECT 
  i.schemaname,
  i.relname,
  i.indexrelname,
  i.idx_scan,
  t.heap_access,
  CASE 
    WHEN t.heap_access > 0 
    THEN ROUND(100.0 * i.idx_scan / t.heap_access, 2)
    ELSE 0 
  END AS index_usage_pct,
  pg_size_pretty(pg_relation_size(i.indexrelid)) AS index_size,
  pg_stat_get_live_tuples(i.indexrelid) AS live_tuples,
  pg_stat_get_dead_tuples(i.indexrelid) AS dead_tuples
FROM pg_stat_user_indexes i
JOIN table_io t USING (schemaname, relname);

-- Regular monitoring query
SELECT 
  indexrelname,
  index_usage_pct,
  index_size,
  ROUND(100.0 * dead_tuples / NULLIF(live_tuples + dead_tuples, 0), 2) AS bloat_pct
FROM v_index_usage_analysis
WHERE index_usage_pct < 5  -- Rarely used indexes
   OR bloat_pct > 20  -- Bloated indexes
ORDER BY index_usage_pct;