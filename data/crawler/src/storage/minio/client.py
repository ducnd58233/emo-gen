import io
from datetime import timedelta
from logging import getLogger
from typing import BinaryIO, Dict, List, Optional, Union

import urllib3
from core.config import config
from core.decorator import retry, timer
from minio import Minio

logger = getLogger("storage.minio.client")


class MinioClient:
    def __init__(
        self,
        endpoint: str = config.minio.endpoint,
        access_key: str = config.minio.access_key,
        secret_key: str = config.minio.secret_key,
        secure: bool = config.minio.secure,
        max_pool_size: int = config.minio.max_pool_size,
        max_connections: int = config.minio.max_connections,
        connect_timeout: int = config.minio.connect_timeout,
        response_timeout: int = config.minio.response_timeout,
    ):
        self.endpoint = endpoint
        self.access_key = access_key
        self.secret_key = secret_key
        self.secure = secure
        self.max_pool_size = max_pool_size
        self.max_connections = max_connections
        self.connect_timeout = connect_timeout
        self.response_timeout = response_timeout
        self._client = None

        self.bucket_map = {
            "image": config.minio.bucket_name + "-image",
            "document": config.minio.bucket_name + "-document",
            "video": config.minio.bucket_name + "-video",
            "audio": config.minio.bucket_name + "-audio",
            "other": config.minio.bucket_name + "-other",
        }

        self.mime_to_bucket = {
            "image/": config.minio.bucket_name + "-image",
            "video/": config.minio.bucket_name + "-video",
            "audio/": config.minio.bucket_name + "-audio",
            "application/pdf": config.minio.bucket_name + "-document",
            "text/": config.minio.bucket_name + "-document",
        }

    @property
    def client(self) -> Minio:
        """Get or create the Minio client"""
        if self._client is None:
            logger.info(f"Initializing MinIO client to {self.endpoint}")
            try:
                # Configure HTTP client with connection pooling
                http_client = urllib3.PoolManager(
                    maxsize=self.max_pool_size,  # Maximum connections per host
                    num_pools=self.max_connections,  # Maximum number of connection pools
                    timeout=urllib3.Timeout(
                        connect=self.connect_timeout, read=self.response_timeout
                    ),
                    retries=urllib3.Retry(
                        total=5,
                        backoff_factor=0.2,
                        status_forcelist=[500, 502, 503, 504],
                    ),
                )

                self._client = Minio(
                    endpoint=self.endpoint,
                    access_key=self.access_key,
                    secret_key=self.secret_key,
                    secure=self.secure,
                    http_client=http_client,
                )
                self._ensure_buckets_exist()
                logger.info(
                    f"MinIO client initialized successfully with pool size {self.max_pool_size}"
                )
            except Exception as e:
                logger.error(f"Failed to initialize MinIO client: {e}")
                raise

        return self._client

    def _ensure_buckets_exist(self):
        """Ensure all required buckets exist"""
        for bucket_name in set(self.bucket_map.values()):
            try:
                if not self.client.bucket_exists(bucket_name):
                    logger.info(f"Creating bucket: {bucket_name}")
                    self.client.make_bucket(bucket_name)
                    logger.info(f"Successfully created bucket: {bucket_name}")
                else:
                    logger.info(f"Bucket already exists: {bucket_name}")
            except Exception as e:
                logger.error(f"Error ensuring bucket {bucket_name} exists: {e}")

    def _get_bucket_for_key(self, key: str) -> str:
        """Determine appropriate bucket based on key and content type"""
        if "/" in key:
            content_type = key.split("/")[0].lower()
            if content_type in self.bucket_map:
                return self.bucket_map[content_type]

        return self.bucket_map["other"]

    def _get_bucket_for_mime(self, mime_type: str) -> str:
        """Determine appropriate bucket based on MIME type"""
        if mime_type:
            for prefix, bucket_type in self.mime_to_bucket.items():
                if mime_type.startswith(prefix):
                    return bucket_type

        logger.debug(
            f"No bucket mapping found for MIME type: {mime_type}, using default bucket"
        )
        return self.bucket_map["other"]

    def _normalize_key(self, key: str) -> str:
        """Normalize object key by removing leading slashes"""
        return key.lstrip("/")

    @timer(name="Minio - Store object")
    @retry(max_attempts=3, delay=1, backoff=2, exceptions=(Exception,))
    def store(
        self,
        data: Union[bytes, bytearray, BinaryIO],
        key: str,
        metadata: Optional[Dict] = None,
    ) -> str:
        """
        Store data in MinIO

        Args:
            data: Binary data (bytes, bytearray) or file-like object
            key: Object key/path
            metadata: Optional metadata (includes content_type)

        Returns:
            Full object URI
        """
        try:
            key = self._normalize_key(key)

            if not hasattr(data, "seek") or not hasattr(data, "read"):
                if isinstance(data, (bytes, bytearray)):
                    data = io.BytesIO(data)
                else:
                    try:
                        data = io.BytesIO(bytes(data))
                    except Exception as e:
                        logger.error(f"Failed to convert data to BytesIO: {e}")
                        raise ValueError(
                            f"Data must be bytes, bytearray, or a file-like object with seek method, got {type(data)}"
                        )

            content_type = None
            if metadata and "content_type" in metadata:
                content_type = metadata["content_type"]

            bucket = (
                self._get_bucket_for_mime(content_type)
                if content_type
                else self._get_bucket_for_key(key)
            )

            try:
                data.seek(0, io.SEEK_END)
                size = data.tell()
                data.seek(0)
            except Exception as e:
                logger.error(f"Error getting file size: {e}")
                raise ValueError(f"Could not determine size of data object: {e}")

            # Ensure bucket exists before storing
            if not self.client.bucket_exists(bucket):
                logger.info(f"Bucket {bucket} doesn't exist, creating it")
                self.client.make_bucket(bucket)

            # Upload to MinIO
            self.client.put_object(
                bucket_name=bucket,
                object_name=key,
                data=data,
                length=size,
                content_type=content_type,
                metadata=metadata,
            )

            logger.info(f"Stored object in bucket '{bucket}' with key '{key}'")
            return f"minio://{bucket}/{key}"

        except Exception as e:
            logger.error(f"Error storing object with key '{key}': {e}")
            import traceback

            logger.error(traceback.format_exc())
            raise

    @timer(name="Minio - Retrieve object")
    @retry(max_attempts=3, delay=1, backoff=2, exceptions=(Exception,))
    def retrieve(self, key: str) -> io.BytesIO:
        """
        Retrieve object from MinIO

        Args:
            key: Object key or full URI

        Returns:
            BytesIO containing object data
        """
        try:
            if key.startswith("minio://"):
                _, bucket, key = key.split("/", 2)
            else:
                key = self._normalize_key(key)
                bucket = self._get_bucket_for_key(key)

            # Ensure bucket exists
            if not self.client.bucket_exists(bucket):
                raise ValueError(f"Bucket '{bucket}' does not exist")

            response = self.client.get_object(bucket, key)
            data = io.BytesIO(response.read())
            response.close()
            response.release_conn()

            logger.info(f"Retrieved object from bucket '{bucket}' with key '{key}'")
            return data

        except Exception as e:
            logger.error(f"Error retrieving object with key '{key}': {e}")
            raise

    @timer(name="Minio - Remove object")
    @retry(max_attempts=3, delay=1, backoff=2, exceptions=(Exception,))
    def remove(self, key: str) -> bool:
        """
        Remove object from MinIO

        Args:
            key: Object key or full URI

        Returns:
            True if successful
        """
        try:
            if key.startswith("minio://"):
                _, bucket, key = key.split("/", 2)
            else:
                # Normalize key
                key = self._normalize_key(key)
                bucket = self._get_bucket_for_key(key)

            # Ensure bucket exists
            if not self.client.bucket_exists(bucket):
                logger.warning(f"Bucket '{bucket}' does not exist, nothing to remove")
                return False

            # Remove object
            self.client.remove_object(bucket, key)

            logger.info(f"Removed object from bucket '{bucket}' with key '{key}'")
            return True

        except Exception as e:
            logger.error(f"Error removing object with key '{key}': {e}")
            return False

    @timer(name="Minio - List objects")
    @retry(max_attempts=3, delay=1, backoff=2, exceptions=(Exception,))
    def list_objects(self, prefix: str = "") -> List[str]:
        """
        List objects with given prefix

        Args:
            prefix: Key prefix

        Returns:
            List of object keys
        """
        try:
            # If bucket is specified in prefix (minio://bucket/prefix)
            if prefix.startswith("minio://"):
                _, bucket, prefix = prefix.split("/", 2)
                if self.client.bucket_exists(bucket):
                    objects = self.client.list_objects(
                        bucket, prefix=prefix, recursive=True
                    )
                    return [f"minio://{bucket}/{obj.object_name}" for obj in objects]
                else:
                    logger.warning(f"Bucket '{bucket}' does not exist")
                    return []

            results = []
            for bucket in set(self.bucket_map.values()):
                if self.client.bucket_exists(bucket):
                    objects = self.client.list_objects(
                        bucket, prefix=prefix, recursive=True
                    )
                    results.extend(
                        [f"minio://{bucket}/{obj.object_name}" for obj in objects]
                    )

            logger.info(f"Listed {len(results)} objects with prefix '{prefix}'")
            return results

        except Exception as e:
            logger.error(f"Error listing objects with prefix '{prefix}': {e}")
            return []

    @timer(name="Minio - Generate presigned URL")
    @retry(max_attempts=3, delay=1, backoff=2, exceptions=(Exception,))
    def get_presigned_url(self, key: str, expires: int = 3600) -> str:
        """
        Generate a presigned URL for an object

        Args:
            key: Object key or full URI
            expires: Expiration time in seconds

        Returns:
            Presigned URL
        """
        try:
            if key.startswith("minio://"):
                _, bucket, key = key.split("/", 2)
            else:
                key = self._normalize_key(key)
                bucket = self._get_bucket_for_key(key)

            # Ensure bucket exists
            if not self.client.bucket_exists(bucket):
                logger.warning(f"Bucket '{bucket}' does not exist, cannot generate URL")
                return ""

            # Generate presigned URL
            url = self.client.presigned_get_object(
                bucket_name=bucket, object_name=key, expires=timedelta(seconds=expires)
            )

            logger.info(f"Generated presigned URL for '{key}' (expires in {expires}s)")
            return url

        except Exception as e:
            logger.error(f"Error generating presigned URL for '{key}': {e}")
            return ""
