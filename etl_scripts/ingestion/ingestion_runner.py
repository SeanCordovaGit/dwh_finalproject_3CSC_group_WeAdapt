import logging
from ingest.ingest_to_postgres import weadaptIngestion


logger = logging.getLogger(__name__)


def run_data_ingestion(data_dir, **context):
    """Run ingestion with comprehensive error handling and reporting."""
    logger.info("=" * 60)
    logger.info("STARTING DATA INGESTION")
    logger.info("=" * 60)

    try:
        ingestion = weadaptIngestion()
        results = ingestion.run_all_ingestions(data_dir)

        successful = sum(1 for result in results.values() if result)
        total = len(results)

        logger.info("=" * 60)
        logger.info(f"INGESTION SUMMARY: {successful}/{total} sources successful")
        logger.info("=" * 60)

        for source, status in results.items():
            status_icon = "✓" if status else "✗"
            logger.info(f"  {status_icon} {source}: {'SUCCESS' if status else 'FAILED'}")

        if successful < total:
            failed_sources = [k for k, v in results.items() if not v]
            logger.warning(f"Failed sources: {', '.join(failed_sources)}")
            # Push failed sources to XCom for downstream handling
            context['task_instance'].xcom_push(key='failed_sources', value=failed_sources)

        # Store metrics in XCom
        context['task_instance'].xcom_push(key='ingestion_metrics', value={
            'successful': successful,
            'total': total,
            'success_rate': (successful / total) * 100 if total > 0 else 0
        })

        return results

    except Exception as e:
        logger.error("=" * 60)
        logger.error("CRITICAL ERROR IN INGESTION")
        logger.error("=" * 60)
        logger.exception(e)
        raise

