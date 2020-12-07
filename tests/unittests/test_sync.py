import unittest
import json
from tap_heap.sync import filter_manifests_to_sync
from tap_heap.sync import get_files_to_sync

class TestFilterManifests(unittest.TestCase):

    def setUp(self):
        self.manifests = {
            123: {"table1": {"files": ["file1", "file2", "file3"],
                             "incremental": False,
                             "columns": ["column1", "column2", "column3"]},
                  "table2": {"files": ["file1", "file2", "file3"],
                             "incremental": True,
                             "columns": ["column1", "column2", "column3"]},
                  "table3": {"files": ["file1", "file2", "file3"],
                             "incremental": True,
                             "columns": ["column1", "column2", "column3"]}},
            124: {"table1": {"files": ["file1", "file2", "file3"],
                             "incremental": True,
                             "columns": ["column1", "column2", "column3"]},
                  "table2": {"files": ["file1", "file2", "file3"],
                             "incremental": True,
                             "columns": ["column1", "column2", "column3"]},
                  "table3": {"files": ["file1", "file2", "file3"],
                             "incremental": True,
                             "columns": ["column1", "column2", "column3"]}},
            125: {"table1": {"files": ["file1", "file2", "file3"],
                             "incremental": True,
                             "columns": ["column1", "column2", "column3"]},
                  "table2": {"files": ["file1", "file2", "file3"],
                             "incremental": True,
                             "columns": ["column1", "column2", "column3"]},
                  "table3": {"files": ["file1", "file2", "file3"],
                             "incremental": True,
                             "columns": ["column1", "column2", "column3"]}},
        }
        self.maxDiff = None

    def test_one_false_incremental_no_bookmark(self):
        self.manifests[123]["table1"]["incremental"] = False

        expected_table_manifests = {
            123: {"files": ["file1", "file2", "file3"],
                  "incremental": False,
                  "columns": ["column1", "column2", "column3"]},
            124: {"files": ["file1", "file2", "file3"],
                  "incremental": True,
                  "columns": ["column1", "column2", "column3"]},
            125: {"files": ["file1", "file2", "file3"],
                  "incremental": True,
                  "columns": ["column1", "column2", "column3"]},
        }

        expected_should_send_activate_version = True

        actual_value = filter_manifests_to_sync(self.manifests, "table1", {})

        self.assertDictEqual(expected_table_manifests, actual_value[0])
        self.assertEqual(expected_should_send_activate_version, actual_value[1])

    def test_two_false_incremental_no_bookmark(self):
        self.manifests[123]["table1"]["incremental"] = False
        self.manifests[125]["table1"]["incremental"] = False

        expected_table_manifests = {
            125: {"files": ["file1", "file2", "file3"],
                  "incremental": False,
                  "columns": ["column1", "column2", "column3"]},
        }

        expected_should_send_activate_version = True

        actual_value = filter_manifests_to_sync(self.manifests, "table1", {})

        self.assertDictEqual(expected_table_manifests, actual_value[0])
        self.assertEqual(expected_should_send_activate_version, actual_value[1])

    def test_has_bookmark_after_false_dump_id(self):
        self.manifests[123]["table1"]["incremental"] = False

        expected_table_manifests = {
            124: {"files": ["file1", "file2", "file3"],
                  "incremental": True,
                  "columns": ["column1", "column2", "column3"]},
            125: {"files": ["file1", "file2", "file3"],
                  "incremental": True,
                  "columns": ["column1", "column2", "column3"]},
        }

        expected_should_send_activate_version = False
        state = {
            "bookmarks": {
                "table1": {
                    "file": "sync_124/table1/part-00001-GUID.avro",
                    "version": 1607032341846
                }
            }
        }
        actual_value = filter_manifests_to_sync(self.manifests, "table1", state)

        self.assertDictEqual(expected_table_manifests, actual_value[0])
        self.assertEqual(expected_should_send_activate_version, actual_value[1])

    def test_has_bookmark_before_false_dump_id(self):

        self.manifests[123]["table1"]["incremental"] = False
        self.manifests[125]["table1"]["incremental"] = False

        expected_table_manifests = {
            125: {"files": ["file1", "file2", "file3"],
                  "incremental": False,
                  "columns": ["column1", "column2", "column3"]},
        }

        expected_should_send_activate_version = True
        state = {
            "bookmarks": {
                "table1": {
                    "file": "sync_124/table1/part-00001-GUID.avro",
                    "version": 1607032341846
                }
            }
        }
        actual_value = filter_manifests_to_sync(self.manifests, "table1", state)

        self.assertDictEqual(expected_table_manifests, actual_value[0])
        self.assertEqual(expected_should_send_activate_version, actual_value[1])

    def test_has_bookmark_on_false_dump_id(self):
        self.manifests[123]["table1"]["incremental"] = False
        self.manifests[124]["table1"]["incremental"] = False

        expected_table_manifests = {
            124: {"files": ["file1", "file2", "file3"],
                  "incremental": False,
                  "columns": ["column1", "column2", "column3"]},
            125: {"files": ["file1", "file2", "file3"],
                  "incremental": True,
                  "columns": ["column1", "column2", "column3"]},
        }

        expected_should_send_activate_version = False
        state = {
            "bookmarks": {
                "table1": {
                    "file": "sync_124/table1/part-00001-GUID.avro",
                    "version": 1607032341846
                }
            }
        }
        actual_value = filter_manifests_to_sync(self.manifests, "table1", state)

        self.assertDictEqual(expected_table_manifests, actual_value[0])
        self.assertEqual(expected_should_send_activate_version, actual_value[1])

class TestGetFilesToSync(unittest.TestCase):

    def setUp(self):
        """Note: `fileX` here would look like `"sync_123/table1/part_00013/GUID.avro"`"""
        self.manifests = {
            123: {"files": ["s3://bucket1/sync_123/file1", "s3://bucket1/sync_123/file2", "s3://bucket1/sync_123/file3"],
                  "incremental": False,
                  "columns": ["column1", "column2", "column3"]},
            124: {"files": ["s3://bucket1/sync_123/file4", "s3://bucket1/sync_123/file5", "s3://bucket1/sync_123/file6"],
                  "incremental": True,
                  "columns": ["column1", "column2", "column3"]},
            125: {"files": ["s3://bucket1/sync_123/file7", "s3://bucket1/sync_123/file8", "s3://bucket1/sync_123/file9"],
                  "incremental": True,
                  "columns": ["column1", "column2", "column3"]},
        }
        self.maxDiff = None

    def test_no_bookmark(self):
        expected_value = ["sync_123/file1",
                          "sync_123/file2",
                          "sync_123/file3",
                          "sync_123/file4",
                          "sync_123/file5",
                          "sync_123/file6",
                          "sync_123/file7",
                          "sync_123/file8",
                          "sync_123/file9"]
        state = {}
        actual_value = get_files_to_sync(self.manifests, "table1", state, "bucket1")

        self.assertListEqual(expected_value, actual_value)

    def test_has_bookmark(self):
        expected_value = ["sync_123/file5",
                          "sync_123/file6",
                          "sync_123/file7",
                          "sync_123/file8",
                          "sync_123/file9"]
        state = {
            "bookmarks": {
                "table1": {
                    "file": "sync_123/file4",
                    "version": 1607032341846
                }
            }
        }
        actual_value = get_files_to_sync(self.manifests, "table1", state, "bucket1")

        self.assertListEqual(expected_value, actual_value)
