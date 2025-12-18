import logging


logger = logging.getLogger(__name__)


def send_failure_notification(**context):
    """Send failure notification with error details."""
    logger.error("=" * 60)
    logger.error("❌ PIPELINE FAILED")
    logger.error("=" * 60)

    # Get execution date
    execution_date = context['execution_date']
    logger.error(f"Execution Date: {execution_date}")

    # Get failed task instance
    dag_run = context['dag_run']
    failed_tasks = [ti.task_id for ti in dag_run.get_task_instances() if ti.state == 'failed']

    if failed_tasks:
        logger.error(f"Failed Tasks: {', '.join(failed_tasks)}")

    # In production, send email/Slack/PagerDuty alert here
    return "Pipeline failed"

