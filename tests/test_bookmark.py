import datetime
import dateutil.parser
from functools import reduce
import os
import pytz
import unittest

from base import TapHeapBaseCase
from tap_tester import runner, menagerie, connections, LOGGER
from tap_tester.base_suite_tests.base_case import BaseCase

class TapHeapBookmarksTest(TapHeapBaseCase):
    @staticmethod
    def name():
        return "heap_bookmarks"

    def streams_to_test(self):
        return self.expected_stream_names()

    def expected_pks(self):
        return {k: v.get(BaseCase.PRIMARY_KEYS) for k, v in self.expected_metadata().items()}

    def streams_to_selected_fields(self):
        return {}

    def test_bookmarks(self):
        """A Parametrized Bookmarks Test"""
        conn_id = connections.ensure_connection(self)

        found_catalogs = self.run_and_verify_check_mode(conn_id)
        self.catalogs = found_catalogs

        # Select our catalogs
        our_catalogs = [c for c in found_catalogs if c.get('tap_stream_id') in self.streams_to_test()]

        self.perform_and_verify_table_and_field_selection(conn_id, our_catalogs)

        # Clear state before our run
        menagerie.set_state(conn_id, {})

        # Run a sync job using orchestrator
        self.run_and_verify_sync_mode(conn_id)

        # Verify actual rows were synced
        record_count_by_stream = runner.examine_target_output_file(self, conn_id, self.streams_to_test(), self.expected_pks())
        replicated_row_count = reduce(lambda accum,c : accum + c, record_count_by_stream.values())
        self.assertGreater(replicated_row_count, 0, msg="failed to replicate any data: {}".format(record_count_by_stream))

        state = menagerie.get_state(conn_id)
        self.assertNotEqual(state or {}, {}, f'the state should not be empty {state}')

        # Run another Sync
        sync_job_name = runner.run_sync_mode(self, conn_id)
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        # Check that we synced new records.
        record_count_by_stream = runner.examine_target_output_file(self, conn_id, self.streams_to_test(), self.expected_pks())
        replicated_row_count = reduce(lambda accum,c : accum + c, record_count_by_stream.values(), 0)
        self.assertEqual(replicated_row_count, 0, f'sync with bookmark should replicate 0 rows, but replicated {replicated_row_count} rows')
