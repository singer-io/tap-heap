import unittest
from unittest.mock import patch, MagicMock

from botocore.exceptions import ClientError

from tap_heap.discover import discover_streams, _check_stream_access, _apply_access_checks
from tap_heap.exceptions import HeapForbiddenError


class TestCheckStreamAccess(unittest.TestCase):
    """Tests for _check_stream_access()"""

    def setUp(self):
        self.bucket = "test-bucket"
        self.manifests = {
            100: {
                "users": {
                    "files": ["s3://test-bucket/sync_100/users/part-00000.avro"],
                    "incremental": False,
                    "columns": ["user_id", "email"]
                },
                "sessions": {
                    "files": ["s3://test-bucket/sync_100/sessions/part-00000.avro"],
                    "incremental": True,
                    "columns": ["session_id", "user_id"]
                }
            }
        }

    @patch("tap_heap.discover.boto3.client")
    def test_accessible_stream_returns_true(self, mock_boto_client):
        """Stream with accessible files returns True"""
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3
        mock_s3.head_object.return_value = {}

        result = _check_stream_access(self.bucket, "users", self.manifests)
        self.assertTrue(result)
        mock_s3.head_object.assert_called_once_with(
            Bucket="test-bucket", Key="sync_100/users/part-00000.avro"
        )

    @patch("tap_heap.discover.boto3.client")
    def test_forbidden_stream_returns_false(self, mock_boto_client):
        """Stream returning 403 AccessDenied returns False"""
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3
        mock_s3.head_object.side_effect = ClientError(
            {"Error": {"Code": "403", "Message": "Forbidden"}}, "HeadObject"
        )

        result = _check_stream_access(self.bucket, "users", self.manifests)
        self.assertFalse(result)

    @patch("tap_heap.discover.boto3.client")
    def test_access_denied_code_returns_false(self, mock_boto_client):
        """Stream returning AccessDenied error code returns False"""
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3
        mock_s3.head_object.side_effect = ClientError(
            {"Error": {"Code": "AccessDenied", "Message": "Access Denied"}}, "HeadObject"
        )

        result = _check_stream_access(self.bucket, "users", self.manifests)
        self.assertFalse(result)

    @patch("tap_heap.discover.boto3.client")
    def test_non_permission_error_reraises(self, mock_boto_client):
        """Non-permission ClientErrors are re-raised"""
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3
        mock_s3.head_object.side_effect = ClientError(
            {"Error": {"Code": "404", "Message": "Not Found"}}, "HeadObject"
        )

        with self.assertRaises(ClientError):
            _check_stream_access(self.bucket, "users", self.manifests)

    def test_stream_with_no_files_returns_true(self):
        """Stream with no files in manifests is considered accessible"""
        result = _check_stream_access(self.bucket, "nonexistent_table", self.manifests)
        self.assertTrue(result)


class TestApplyAccessChecks(unittest.TestCase):
    """Tests for _apply_access_checks()"""

    def setUp(self):
        self.bucket = "test-bucket"
        self.manifests = {
            100: {
                "users": {
                    "files": ["s3://test-bucket/sync_100/users/part-00000.avro"],
                    "incremental": False,
                    "columns": ["user_id", "email"]
                },
                "sessions": {
                    "files": ["s3://test-bucket/sync_100/sessions/part-00000.avro"],
                    "incremental": True,
                    "columns": ["session_id", "user_id"]
                },
                "events": {
                    "files": ["s3://test-bucket/sync_100/events/part-00000.avro"],
                    "incremental": True,
                    "columns": ["event_id", "user_id"]
                }
            }
        }
        self.streams = [
            {"stream": "users", "tap_stream_id": "users", "schema": {}, "metadata": []},
            {"stream": "sessions", "tap_stream_id": "sessions", "schema": {}, "metadata": []},
            {"stream": "events", "tap_stream_id": "events", "schema": {}, "metadata": []},
        ]

    @patch("tap_heap.discover._check_stream_access")
    def test_all_streams_accessible(self, mock_check):
        """All streams remain when all are accessible"""
        mock_check.return_value = True

        result = _apply_access_checks(self.bucket, self.streams, self.manifests)
        self.assertEqual(len(result), 3)
        self.assertEqual(
            [s['tap_stream_id'] for s in result],
            ["users", "sessions", "events"]
        )

    @patch("tap_heap.discover._check_stream_access")
    def test_partial_access_excludes_forbidden_streams(self, mock_check):
        """Inaccessible streams are excluded from the result"""
        mock_check.side_effect = lambda bucket, table, manifests, **kwargs: table != "sessions"

        result = _apply_access_checks(self.bucket, self.streams, self.manifests)
        self.assertEqual(len(result), 2)
        stream_ids = [s['tap_stream_id'] for s in result]
        self.assertIn("users", stream_ids)
        self.assertIn("events", stream_ids)
        self.assertNotIn("sessions", stream_ids)

    @patch("tap_heap.discover._check_stream_access")
    def test_all_inaccessible_raises_forbidden_error(self, mock_check):
        """HeapForbiddenError raised when no streams are accessible"""
        mock_check.return_value = False

        with self.assertRaises(HeapForbiddenError) as context:
            _apply_access_checks(self.bucket, self.streams, self.manifests)

        self.assertIn("No streams are accessible", str(context.exception))
        self.assertIn("read permission", str(context.exception))

    @patch("tap_heap.discover._check_stream_access")
    def test_partial_access_logs_warning(self, mock_check):
        """Warning is logged for excluded streams"""
        mock_check.side_effect = lambda bucket, table, manifests, **kwargs: table != "events"

        with patch("tap_heap.discover.LOGGER") as mock_logger:
            result = _apply_access_checks(self.bucket, self.streams, self.manifests)
            mock_logger.warning.assert_called_once()
            warning_msg = mock_logger.warning.call_args[0][0]
            self.assertIn("excluded due to HTTP-Error-Code:403", warning_msg)


class TestDiscoverStreamsWithAccessChecks(unittest.TestCase):
    """Integration tests for discover_streams with access checking"""

    @patch("tap_heap.discover._check_stream_access")
    @patch("tap_heap.discover.manifest.generate_manifests")
    def test_discover_excludes_inaccessible_streams(self, mock_manifests, mock_check):
        """discover_streams excludes streams that fail access check"""
        mock_manifests.return_value = {
            100: {
                "users": {
                    "files": ["s3://bucket/sync_100/users/part-00000.avro"],
                    "incremental": False,
                    "columns": ["user_id", "email"]
                },
                "sessions": {
                    "files": ["s3://bucket/sync_100/sessions/part-00000.avro"],
                    "incremental": True,
                    "columns": ["session_id", "user_id"]
                }
            }
        }
        mock_check.side_effect = lambda bucket, table, manifests, **kwargs: table == "users"

        streams = discover_streams("test-bucket")
        self.assertEqual(len(streams), 1)
        self.assertEqual(streams[0]['tap_stream_id'], "users")

    @patch("tap_heap.discover._check_stream_access")
    @patch("tap_heap.discover.manifest.generate_manifests")
    def test_discover_all_accessible(self, mock_manifests, mock_check):
        """discover_streams returns all streams when all accessible"""
        mock_manifests.return_value = {
            100: {
                "users": {
                    "files": ["s3://bucket/sync_100/users/part-00000.avro"],
                    "incremental": False,
                    "columns": ["user_id", "email"]
                },
                "sessions": {
                    "files": ["s3://bucket/sync_100/sessions/part-00000.avro"],
                    "incremental": True,
                    "columns": ["session_id", "user_id"]
                }
            }
        }
        mock_check.return_value = True

        streams = discover_streams("test-bucket")
        self.assertEqual(len(streams), 2)

    @patch("tap_heap.discover._check_stream_access")
    @patch("tap_heap.discover.manifest.generate_manifests")
    def test_discover_no_streams_raises_error(self, mock_manifests, mock_check):
        """discover_streams raises HeapForbiddenError when all inaccessible"""
        mock_manifests.return_value = {
            100: {
                "users": {
                    "files": ["s3://bucket/sync_100/users/part-00000.avro"],
                    "incremental": False,
                    "columns": ["user_id"]
                }
            }
        }
        mock_check.return_value = False

        with self.assertRaises(HeapForbiddenError):
            discover_streams("test-bucket")
