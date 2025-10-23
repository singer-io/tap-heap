import unittest
from base import TapHeapBaseCase
from tap_tester.base_suite_tests.automatic_fields_test import MinimumSelectionTest

class TapHeapAutomaticFieldsTest(MinimumSelectionTest, TapHeapBaseCase):
    """Standard Automatic Fields Test"""

    @staticmethod
    def name():
        return 'heap_automatic_fields'

    def streams_to_test(self):
        return self.expected_stream_names()

    @unittest.skip('TODO - figure this out')
    def test_records_primary_key_is_unique(self):
        super().test_records_primary_key_is_unique()
