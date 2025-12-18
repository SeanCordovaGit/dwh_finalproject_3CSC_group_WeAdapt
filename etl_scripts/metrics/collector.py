import logging
from airflow.providers.postgres.hooks.postgres import PostgresHook


logger = logging.getLogger(__name__)


def collect_pipeline_metrics(postgres_conn_id, **context):
    """Collect and log comprehensive pipeline metrics."""
    logger.info("=" * 60)
    logger.info("COLLECTING PIPELINE METRICS")
    logger.info("=" * 60)

    try:
        pg = PostgresHook(postgres_conn_id=postgres_conn_id)
        conn = pg.get_conn()
        cur = conn.cursor()
    except Exception as e:
        logger.error("Unable to connect to Postgres for metrics collection")
        logger.exception(e)
        return False

    metrics = {}
    metrics_queries = {
        'total_customers': 'SELECT COUNT(*) FROM warehouse.dim_customer',
        'active_customers': 'SELECT COUNT(*) FROM warehouse.dim_customer WHERE is_active = TRUE',
        'total_products': 'SELECT COUNT(*) FROM warehouse.dim_product',
        'total_merchants': 'SELECT COUNT(*) FROM warehouse.dim_merchant',
        'total_orders': 'SELECT COUNT(*) FROM warehouse.fact_orders',
        'total_revenue': 'SELECT COALESCE(SUM(net_amount), 0) FROM warehouse.fact_orders',
        'todays_orders': "SELECT COUNT(*) FROM warehouse.fact_orders WHERE order_date = CURRENT_DATE",
    }

    for metric_name, query in metrics_queries.items():
        try:
            cur.execute(query)
            result = cur.fetchone()[0]
            metrics[metric_name] = result

            # Format large numbers nicely
            if metric_name.endswith('revenue'):
                logger.info(f"  {metric_name}: ${result:,.2f}")
            else:
                logger.info(f"  {metric_name}: {result:,}")

        except Exception as e:
            logger.warning(f"Could not collect {metric_name}: {str(e)}")
            metrics[metric_name] = None

    cur.close()
    conn.close()

    # Store metrics in XCom
    context['task_instance'].xcom_push(key='pipeline_metrics', value=metrics)

    logger.info("=" * 60)
    logger.info("✓ METRICS COLLECTION COMPLETED")
    logger.info("=" * 60)
    return True

