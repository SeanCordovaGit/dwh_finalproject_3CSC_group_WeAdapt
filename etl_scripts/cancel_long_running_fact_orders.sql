-- ==========================================
-- CANCEL LONG RUNNING FACT ORDERS QUERY
-- Use this if the fact_orders loading is stuck
-- ==========================================

-- Find the running fact_orders query
SELECT
    pid,
    query_start,
    now() - query_start as runtime,
    state,
    left(query, 100) as query_preview
FROM pg_stat_activity
WHERE state != 'idle'
    AND query LIKE '%fact_orders%'
ORDER BY query_start DESC;

-- Cancel the query (replace PID_NUMBER with actual PID from above)
-- SELECT pg_cancel_backend(PID_NUMBER);

-- Alternative: terminate the query if cancel doesn't work
-- SELECT pg_terminate_backend(PID_NUMBER);

-- Check if query was cancelled
SELECT
    pid,
    state,
    left(query, 50) as query_preview
FROM pg_stat_activity
WHERE pid = PID_NUMBER;

