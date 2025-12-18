-- ==========================================
-- DIAGNOSTIC QUERY FOR FACT ORDERS LOADING
-- Check data volumes and potential bottlenecks
-- ==========================================

-- Check staging table sizes
SELECT
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size,
    pg_total_relation_size(schemaname||'.'||tablename) as bytes,
    n_tup_ins as rows
FROM pg_stat_user_tables
WHERE schemaname = 'staging'
    AND tablename LIKE '%operations%'
ORDER BY n_tup_ins DESC;

-- Check if indexes exist
SELECT
    schemaname,
    tablename,
    indexname,
    pg_size_pretty(pg_relation_size(indexrelid)) as index_size
FROM pg_stat_user_indexes
WHERE schemaname = 'staging'
    AND tablename LIKE '%operations%'
ORDER BY pg_relation_size(indexrelid) DESC;

-- Check current locks
SELECT
    locktype,
    relation::regclass,
    mode,
    granted,
    pid,
    query_start,
    state
FROM pg_stat_activity a
JOIN pg_locks l ON a.pid = l.pid
WHERE l.locktype = 'relation'
    AND relation::regclass::text LIKE '%fact_orders%'
    AND a.state != 'idle';

-- Check active queries
SELECT
    pid,
    query_start,
    state,
    left(query, 100) as query_preview
FROM pg_stat_activity
WHERE state != 'idle'
    AND query LIKE '%fact_orders%'
ORDER BY query_start DESC;

-- Check temp table sizes during execution
SELECT
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size,
    n_tup_ins as rows
FROM pg_stat_user_tables
WHERE tablename LIKE 'temp_%'
ORDER BY n_tup_ins DESC;

