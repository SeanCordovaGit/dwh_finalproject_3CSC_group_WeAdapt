import psycopg2
import os
from datetime import datetime

class FactOrdersDebugger:
    def __init__(self):
        self.db_config = {
            'host': os.getenv('DB_HOST', 'localhost'),  # Local host for debugging
            'port': os.getenv('DB_PORT', '5432'),
            'database': os.getenv('DB_NAME', 'weadapt_dwh'),
            'user': os.getenv('DB_USER', 'weadapt'),
            'password': os.getenv('DB_PASSWORD', 'weadapt123')
        }

    def connect(self):
        try:
            conn = psycopg2.connect(**self.db_config)
            conn.autocommit = True
            return conn
        except Exception as e:
            print(f"Failed to connect: {e}")
            return None

    def run_query(self, query, description=""):
        conn = self.connect()
        if not conn:
            return None

        try:
            cursor = conn.cursor()
            print(f"\n=== {description} ===")
            start_time = datetime.now()
            cursor.execute(query)
            results = cursor.fetchall()
            end_time = datetime.now()

            print(f"Query executed in: {(end_time - start_time).total_seconds():.2f} seconds")
            print(f"Rows returned: {len(results)}")

            # Print column headers if available
            if cursor.description:
                headers = [desc[0] for desc in cursor.description]
                print(" | ".join(headers))

            for row in results[:20]:  # Limit output
                print(row)

            if len(results) > 20:
                print(f"... and {len(results) - 20} more rows")

            cursor.close()
            conn.close()
            return results

        except Exception as e:
            print(f"Error running query: {e}")
            if conn:
                conn.close()
            return None

    def debug_simple_counts(self):
        """Check basic row counts"""
        queries = [
            ("Staging table sizes", """
                SELECT
                    'staging_operations_order_headers' as table_name,
                    COUNT(*) as row_count
                FROM staging.staging_operations_order_headers

                UNION ALL

                SELECT
                    'staging_operations_line_items_products' as table_name,
                    COUNT(*) as row_count
                FROM staging.staging_operations_line_items_products

                UNION ALL

                SELECT
                    'staging_operations_line_items_prices' as table_name,
                    COUNT(*) as row_count
                FROM staging.staging_operations_line_items_prices

                UNION ALL

                SELECT
                    'staging_enterprise_orders' as table_name,
                    COUNT(*) as row_count
                FROM staging.staging_enterprise_orders

                UNION ALL

                SELECT
                    'staging_marketing_transactions' as table_name,
                    COUNT(*) as row_count
                FROM staging.staging_marketing_transactions

                UNION ALL

                SELECT
                    'warehouse_fact_orders' as table_name,
                    COUNT(*) as row_count
                FROM warehouse.fact_orders

                ORDER BY row_count DESC;
            """),
            ("Dimension table sizes", """
                SELECT
                    'dim_customer' as table_name, COUNT(*) as row_count FROM warehouse.dim_customer
                UNION ALL
                SELECT 'dim_product', COUNT(*) FROM warehouse.dim_product
                UNION ALL
                SELECT 'dim_merchant', COUNT(*) FROM warehouse.dim_merchant
                UNION ALL
                SELECT 'dim_staff', COUNT(*) FROM warehouse.dim_staff
                UNION ALL
                SELECT 'dim_campaign', COUNT(*) FROM warehouse.dim_campaign
                UNION ALL
                SELECT 'dim_date', COUNT(*) FROM warehouse.dim_date
                ORDER BY row_count DESC;
            """)
        ]

        for desc, query in queries:
            self.run_query(query, desc)

    def debug_active_queries(self):
        """Check for running queries"""
        queries = [
            ("Active queries with fact_orders", """
                SELECT
                    pid,
                    query_start,
                    state,
                    left(query, 100) as query_preview
                FROM pg_stat_activity
                WHERE state != 'idle'
                    AND query LIKE '%fact_orders%'
                ORDER BY query_start DESC;
            """),
            ("Current locks on fact_orders", """
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
            """),
            ("Temp table sizes", """
                SELECT
                    schemaname,
                    tablename,
                    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size,
                    n_tup_ins as rows
                FROM pg_stat_user_tables
                WHERE tablename LIKE 'temp_%'
                ORDER BY n_tup_ins DESC;
            """)
        ]

        for desc, query in queries:
            self.run_query(query, desc)

    def debug_performance(self):
        """Check performance metrics"""
        queries = [
            ("Staging table sizes with indexes", """
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
            """),
            ("Index sizes on staging operations", """
                SELECT
                    schemaname,
                    tablename,
                    indexname,
                    pg_size_pretty(pg_relation_size(indexrelid)) as index_size
                FROM pg_stat_user_indexes
                WHERE schemaname = 'staging'
                    AND tablename LIKE '%operations%'
                ORDER BY pg_relation_size(indexrelid) DESC;
            """)
        ]

        for desc, query in queries:
            self.run_query(query, desc)

    def check_fact_orders_status(self):
        """Check current state of fact_orders table"""
        query = """
            SELECT
                COUNT(*) as existing_fact_rows,
                COUNT(DISTINCT order_id) as distinct_orders,
                MIN(updated_at) as oldest_record,
                MAX(updated_at) as newest_record,
                pg_size_pretty(pg_total_relation_size('warehouse.fact_orders')) as table_size
            FROM warehouse.fact_orders;
        """
        self.run_query(query, "Current fact_orders status")

def main():
    debugger = FactOrdersDebugger()

    print("=== FACT ORDERS DEBUG SESSION ===")
    print(f"Started at: {datetime.now()}")

    print("\n1. CHECKING BASIC COUNTS...")
    debugger.debug_simple_counts()

    print("\n2. CHECKING ACTIVE QUERIES AND LOCKS...")
    debugger.debug_active_queries()

    print("\n3. CHECKING PERFORMANCE METRICS...")
    debugger.debug_performance()

    print("\n4. CHECKING FACT ORDERS STATUS...")
    debugger.check_fact_orders_status()

    print(f"\nDebug session completed at: {datetime.now()}")

if __name__ == "__main__":
    main()

