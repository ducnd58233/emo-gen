from typing import Optional
from pyspark.sql import SparkSession
from core.logger import get_logger
from core.config import config
from core.decorator import singleton

logger = get_logger("processing.spark.session")


@singleton
class SparkSessionManager:
    """Singleton manager for SparkSession"""
    _session: Optional[SparkSession] = None

    @property
    def session(self) -> SparkSession:
        """Get or create SparkSession"""
        if self._session is None:
            self._create_session()
        return self._session

    def _create_session(self) -> None:
        """Create SparkSession"""
        logger.info("Creating SparkSession")

        app_name = config.spark.app_name
        master = config.spark.master

        builder = SparkSession.builder.appName(app_name)

        if master:
            builder = builder.master(master)

        connection_configs = {
            # Increase timeout and retry settings
            "spark.network.timeout": "800s",
            "spark.executor.heartbeatInterval": "60s",

            # Memory settings to prevent OOM errors
            "spark.driver.memory": "2g",
            "spark.executor.memory": "2g",

            # Improve Py4J stability
            "spark.python.worker.reuse": "true",
            "spark.python.profile": "false",

            # GC settings
            "spark.eventLog.gcMetrics.youngGenerationGarbageCollectors": "G1 Young Generation",
            "spark.eventLog.gcMetrics.oldGenerationGarbageCollectors": "G1 Old Generation"
        }

        for key, value in connection_configs.items():
            builder = builder.config(key, value)

        for key, value in config.spark.configs.items():
            builder = builder.config(key, value)

        try:
            self._session = builder.getOrCreate()
            logger.info(f"Created SparkSession: {app_name}")
        except Exception as e:
            logger.error(f"Failed to create SparkSession: {e}")
            logger.warning("Attempting to create minimal local SparkSession")
            builder = SparkSession.builder.appName(app_name).master("local[1]")
            for key, value in connection_configs.items():
                builder = builder.config(key, value)
            self._session = builder.getOrCreate()

    def stop(self) -> None:
        """Stop SparkSession"""
        if self._session:
            self._session.stop()
            self._session = None
            logger.info("Stopped SparkSession")
