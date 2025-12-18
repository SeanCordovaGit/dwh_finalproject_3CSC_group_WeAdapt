import logging


logger = logging.getLogger(__name__)


def send_success_notification(**context):
    """Send success notification with metrics."""
    logger.info("=" * 60)
    logger.info("🎉 PIPELINE COMPLETED SUCCESSFULLY")
    logger.info("=" * 60)

    # Retrieve metrics from XCom
    ti = context['task_instance']
    metrics = ti.xcom_pull(task_ids='collect_pipeline_metrics', key='pipeline_metrics')

    if metrics:
        logger.info("Pipeline Summary:")
        logger.info(f"  Total Orders: {metrics.get('total_orders', 'N/A'):,}")
        logger.info(f"  Total Revenue: ${metrics.get('total_revenue', 0):,.2f}")
        logger.info(f"  Active Customers: {metrics.get('active_customers', 'N/A'):,}")

    # In production, send email/Slack notification here
    return "Pipeline successful"

