import logging
from airflow.providers.postgres.hooks.postgres import PostgresHook


logger = logging.getLogger(__name__)


def cleanup_on_failure(postgres_conn_id, **context):
    """Clean up staging data and log failure details."""
    logger.error("=" * 60)
    logger.error("PIPELINE FAILURE - RUNNING CLEANUP")
    logger.error("=" * 60)

    try:
        pg = PostgresHook(postgres_conn_id=postgres_conn_id)
        conn = pg.get_conn()
        cur = conn.cursor()

        # Truncate staging tables to prevent stale data
        cleanup_queries = [
            "TRUNCATE TABLE staging.customers CASCADE;",
            "TRUNCATE TABLE staging.products CASCADE;",
            "TRUNCATE TABLE staging.orders CASCADE;",
        ]

        for query in cleanup_queries:
            try:
                cur.execute(query)
                logger.info(f"Executed: {query}")
            except Exception as e:
                logger.warning(f"Cleanup query failed: {str(e)}")

        conn.commit()
        cur.close()
        conn.close()

        logger.info("✓ Staging cleanup completed")

    except Exception as e:
        logger.error("Failed to cleanup staging tables")
        logger.exception(e)

    # Log failed task info
    failed_tasks = context.get('task_instance').get_dagrun().get_task_instances(state='failed')
    if failed_tasks:
        logger.error("Failed tasks:")
        for task in failed_tasks:
            logger.error(f"  - {task.task_id}: {task.state}")

