#!/usr/bin/env python3
"""
weadapt Data Warehouse - Data Quality Checks

This script performs comprehensive data quality validation after ETL transformations.
Checks include null validations, foreign key integrity, duplicate detection, and
business rule validations.
"""

import psycopg2
import psycopg2.extras
import os
import sys
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DataQualityChecker:
    def __init__(self):
        self.db_config = {
            'host': os.getenv('DB_HOST', 'postgres'),  # Match ingestion script default
            'port': os.getenv('DB_PORT', '5432'),
            'database': os.getenv('DB_NAME', 'weadapt_dwh'),
            'user': os.getenv('DB_USER', 'weadapt'),
            'password': os.getenv('DB_PASSWORD', 'weadapt123')
        }
        self.connection = None
        self.cursor = None
        self.issues = []

    def connect(self):
        """Establish database connection"""
        try:
            self.connection = psycopg2.connect(**self.db_config)
            self.cursor = self.connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            logger.info("Connected to database successfully")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            return False

    def disconnect(self):
        """Close database connection"""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        logger.info("Database connection closed")

    def log_issue(self, check_name, severity, description, query=None, result=None):
        """Log a data quality issue"""
        issue = {
            'check_name': check_name,
            'severity': severity,  # CRITICAL, HIGH, MEDIUM, LOW
            'description': description,
            'query': query,
            'result': result,
            'timestamp': datetime.now()
        }
        self.issues.append(issue)
        logger.warning(f"[{severity}] {check_name}: {description}")

    def run_query(self, query, description="", params=None):
        """Execute a query and return results"""
        try:
            self.cursor.execute(query, params or ())
            if query.strip().upper().startswith('SELECT'):
                return self.cursor.fetchall()
            else:
                self.connection.commit()
                return None
        except Exception as e:
            logger.error(f"Query failed: {description} - {e}")
            return None

    def check_table_exists(self, schema, table):
        """Check if table exists"""
        query = """
        SELECT EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = %s AND table_name = %s
        );
        """
        result = self.run_query(query, f"Check table {schema}.{table} exists", params=(schema, table))
        return result and result[0]['exists']

    def check_row_count(self, schema, table, min_rows=1):
        """Check minimum row count in table"""
        query = f"SELECT COUNT(*) as row_count FROM {schema}.{table};"
        result = self.run_query(query, f"Count rows in {schema}.{table}")

        if result and result[0]['row_count'] < min_rows:
            self.log_issue(
                f"Row Count Check - {schema}.{table}",
                "CRITICAL",
                f"Table {schema}.{table} has only {result[0]['row_count']} rows, minimum required: {min_rows}",
                query,
                result[0]['row_count']
            )
            return False
        else:
            logger.info(f"✓ {schema}.{table} has {result[0]['row_count']} rows")
            return True

    def check_null_values(self, schema, table, columns):
        """Check for null values in critical columns"""
        for column in columns:
            query = f"""
            SELECT COUNT(*) as null_count
            FROM {schema}.{table}
            WHERE {column} IS NULL;
            """
            result = self.run_query(query, f"Check nulls in {schema}.{table}.{column}")

            if result and result[0]['null_count'] > 0:
                severity = "CRITICAL" if column.endswith('_key') else "HIGH"
                self.log_issue(
                    f"Null Check - {schema}.{table}.{column}",
                    severity,
                    f"Found {result[0]['null_count']} null values in {column}",
                    query,
                    result[0]['null_count']
                )
                return False

        logger.info(f"✓ No null values found in critical columns of {schema}.{table}")
        return True

    def check_duplicate_natural_keys(self, schema, table, natural_key_column):
        """Check for duplicate natural keys in dimension tables"""
        query = f"""
        SELECT {natural_key_column}, COUNT(*) as count
        FROM {schema}.{table}
        WHERE {natural_key_column} IS NOT NULL
        GROUP BY {natural_key_column}
        HAVING COUNT(*) > 1;
        """
        result = self.run_query(query, f"Check duplicates in {schema}.{table}.{natural_key_column}")

        if result and len(result) > 0:
            duplicates = sum(row['count'] - 1 for row in result)
            self.log_issue(
                f"Duplicate Check - {schema}.{table}.{natural_key_column}",
                "HIGH",
                f"Found {len(result)} duplicate natural keys affecting {duplicates} records",
                query,
                result
            )
            return False

        logger.info(f"✓ No duplicate natural keys in {schema}.{table}")
        return True

    def check_foreign_key_integrity(self, fact_table, fact_key, dim_table, dim_key):
        """Check foreign key integrity between fact and dimension tables"""
        query = f"""
        SELECT COUNT(*) as orphan_count
        FROM {fact_table} f
        LEFT JOIN {dim_table} d ON f.{fact_key} = d.{dim_key}
        WHERE d.{dim_key} IS NULL AND f.{fact_key} IS NOT NULL;
        """
        result = self.run_query(query, f"Check FK integrity {fact_table}.{fact_key} -> {dim_table}.{dim_key}")

        if result and result[0]['orphan_count'] > 0:
            self.log_issue(
                f"FK Integrity - {fact_table}.{fact_key}",
                "CRITICAL",
                f"Found {result[0]['orphan_count']} orphan records in fact table",
                query,
                result[0]['orphan_count']
            )
            return False

        logger.info(f"✓ Foreign key integrity OK: {fact_table}.{fact_key} -> {dim_table}.{dim_key}")
        return True

    def check_business_rules(self):
        """Check business-specific validation rules"""
        checks = [
            {
                'name': 'Negative Amounts',
                'query': """
                SELECT COUNT(*) as negative_count
                FROM warehouse.fact_orders
                WHERE net_amount < 0;
                """,
                'severity': 'HIGH',
                'description': 'Orders with negative amounts found'
            },
            {
                'name': 'Future Order Dates',
                'query': f"""
                SELECT COUNT(*) as future_count
                FROM warehouse.fact_orders fo
                JOIN warehouse.dim_date dd ON fo.order_date_key = dd.date_key
                WHERE dd.full_date > CURRENT_DATE;
                """,
                'severity': 'MEDIUM',
                'description': 'Orders with future dates found'
            },
            {
                'name': 'Zero Quantity Orders',
                'query': """
                SELECT COUNT(*) as zero_quantity_count
                FROM warehouse.fact_orders
                WHERE quantity <= 0;
                """,
                'severity': 'HIGH',
                'description': 'Orders with zero or negative quantity found'
            }
        ]

        all_passed = True
        for check in checks:
            result = self.run_query(check['query'], check['name'])
            if result and result[0][list(result[0].keys())[0]] > 0:
                count = result[0][list(result[0].keys())[0]]
                self.log_issue(
                    check['name'],
                    check['severity'],
                    f"{check['description']}: {count} records",
                    check['query'],
                    count
                )
                all_passed = False

        return all_passed

    def run_all_checks(self):
        """Run complete data quality assessment"""
        logger.info("Starting comprehensive data quality checks...")

        # Check if warehouse tables exist
        warehouse_tables = [
            'dim_customer', 'dim_product', 'dim_merchant', 'dim_staff',
            'dim_campaign', 'dim_date', 'fact_orders'
        ]

        for table in warehouse_tables:
            if not self.check_table_exists('warehouse', table):
                self.log_issue(
                    f"Table Existence - {table}",
                    "CRITICAL",
                    f"Warehouse table {table} does not exist",
                    None,
                    None
                )
                continue

            # Row count checks
            self.check_row_count('warehouse', table, 0)

        # Dimension table quality checks
        dimension_checks = [
            ('dim_customer', ['customer_id', 'name'], 'customer_id'),
            ('dim_product', ['product_id', 'product_name'], 'product_id'),
            ('dim_merchant', ['merchant_id'], 'merchant_id'),
            ('dim_staff', ['staff_id'], 'staff_id'),
            ('dim_campaign', ['campaign_id', 'campaign_name'], 'campaign_id'),
            ('dim_date', ['date_key', 'full_date'], 'date_key')
        ]

        for table, null_columns, natural_key in dimension_checks:
            if self.check_table_exists('warehouse', table):
                self.check_null_values('warehouse', table, null_columns)
                self.check_duplicate_natural_keys('warehouse', table, natural_key)

        # Fact table foreign key checks
        fk_checks = [
            ('warehouse.fact_orders', 'customer_key', 'warehouse.dim_customer', 'customer_key'),
            ('warehouse.fact_orders', 'product_key', 'warehouse.dim_product', 'product_key'),
            ('warehouse.fact_orders', 'merchant_key', 'warehouse.dim_merchant', 'merchant_key'),
            ('warehouse.fact_orders', 'staff_key', 'warehouse.dim_staff', 'staff_key'),
            ('warehouse.fact_orders', 'campaign_key', 'warehouse.dim_campaign', 'campaign_key'),
            ('warehouse.fact_orders', 'order_date_key', 'warehouse.dim_date', 'date_key'),
            ('warehouse.fact_orders', 'estimated_arrival_key', 'warehouse.dim_date', 'date_key')
        ]

        for fact_table, fact_key, dim_table, dim_key in fk_checks:
            if self.check_table_exists(fact_table.split('.')[1], fact_table.split('.')[1]):
                self.check_foreign_key_integrity(fact_table, fact_key, dim_table, dim_key)

        # Business rule checks
        self.check_business_rules()

        # Summary
        critical_issues = len([i for i in self.issues if i['severity'] == 'CRITICAL'])
        high_issues = len([i for i in self.issues if i['severity'] == 'HIGH'])

        logger.info(f"Data quality check completed. Issues found: {len(self.issues)}")
        logger.info(f"Critical: {critical_issues}, High: {high_issues}, Other: {len(self.issues) - critical_issues - high_issues}")

        return critical_issues == 0  # Return True if no critical issues

    def generate_report(self):
        """Generate a summary report of all issues"""
        report = []
        report.append("=== weadapt DATA WAREHOUSE QUALITY REPORT ===")
        report.append(f"Generated: {datetime.now()}")
        report.append(f"Total Issues: {len(self.issues)}")
        report.append("")

        for severity in ['CRITICAL', 'HIGH', 'MEDIUM', 'LOW']:
            severity_issues = [i for i in self.issues if i['severity'] == severity]
            if severity_issues:
                report.append(f"{severity} ISSUES ({len(severity_issues)}):")
                for issue in severity_issues:
                    report.append(f"  • {issue['check_name']}: {issue['description']}")
                report.append("")

        return "\n".join(report)

def main():
    """Main execution function"""
    checker = DataQualityChecker()

    if not checker.connect():
        sys.exit(1)

    try:
        success = checker.run_all_checks()
        report = checker.generate_report()

        print(report)

        if not success:
            logger.error("Data quality checks failed - critical issues found")
            sys.exit(1)
        else:
            logger.info("All data quality checks passed")
            sys.exit(0)

    finally:
        checker.disconnect()

if __name__ == "__main__":
    main()

