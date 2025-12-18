import logging
import os
from airflow.providers.postgres.hooks.postgres import PostgresHook


logger = logging.getLogger(__name__)


def validate_environment(postgres_conn_id, data_dir, staging_file_path, **context):
    """Validate DB connectivity, data directory, and required imports."""
    logger.info("=" * 60)
    logger.info("VALIDATING PIPELINE ENVIRONMENT")
    logger.info("=" * 60)

    validation_errors = []

    # Check database connection
    try:
        pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute("SELECT version();")
        version = cursor.fetchone()[0]
        logger.info("✓ Database connection successful")
        logger.info(f"  PostgreSQL version: {version}")
        cursor.close()
        conn.close()
    except Exception as e:
        error_msg = f"Database connection failed: {str(e)}"
        logger.error(error_msg)
        validation_errors.append(error_msg)

    # Check data directory
    if not os.path.isdir(data_dir):
        logger.warning(f"Data directory {data_dir} does not exist; attempting to create it")
        try:
            os.makedirs(data_dir, exist_ok=True)
            logger.info(f"✓ Created data directory: {data_dir}")
        except Exception as e:
            error_msg = f"Could not create data directory: {str(e)}"
            logger.error(error_msg)
            validation_errors.append(error_msg)
    else:
        logger.info(f"✓ Data directory exists: {data_dir}")

    # Check required imports
    try:
        from ingest.ingest_to_postgres import weadaptIngestion
        logger.info("✓ weadaptIngestion class imported successfully")
    except ImportError:
        error_msg = "weadaptIngestion class not importable"
        logger.error(error_msg)
        validation_errors.append(error_msg)

    # Check staging file path (though it's a file sensor, validate existence)
    if not os.path.exists(staging_file_path):
        logger.warning(f"Staging file path {staging_file_path} does not exist yet - this is expected for file sensor")
    else:
        logger.info(f"✓ Staging file path exists: {staging_file_path}")

    # Report results
    if validation_errors:
        logger.error("=" * 60)
        logger.error("VALIDATION FAILED")
        logger.error("=" * 60)
        for error in validation_errors:
            logger.error(f"  - {error}")
        return False

    logger.info("=" * 60)
    logger.info("✓ ALL VALIDATIONS PASSED")
    logger.info("=" * 60)
    return True

