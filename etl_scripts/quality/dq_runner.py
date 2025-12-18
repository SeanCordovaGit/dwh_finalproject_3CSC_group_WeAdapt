import logging
from quality.dq_checks import DataQualityChecker


logger = logging.getLogger(__name__)


def run_data_quality_checks(**context):
    """Run comprehensive data quality validation."""
    logger.info("=" * 60)
    logger.info("STARTING DATA QUALITY VALIDATION")
    logger.info("=" * 60)

    checker = DataQualityChecker()

    try:
        checker.connect()
        success = checker.run_all_checks()
        report = checker.generate_report()

        logger.info("=" * 60)
        logger.info("DATA QUALITY REPORT")
        logger.info("=" * 60)
        logger.info(report)

        # Store report in XCom for downstream access
        context['task_instance'].xcom_push(key='dq_report', value=report)

        if not success:
            logger.error("=" * 60)
            logger.error("DATA QUALITY CHECKS FAILED")
            logger.error("=" * 60)
            raise Exception("Data quality validation failed - see report above")

        logger.info("✓ All data quality checks passed")
        return True

    finally:
        checker.disconnect()

