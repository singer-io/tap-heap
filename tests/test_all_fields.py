import unittest
from base import TapHeapBaseCase
from tap_tester.base_suite_tests.all_fields_test import AllFieldsTest

class TapHeapDiscoveryTest(AllFieldsTest, TapHeapBaseCase):
    """Standard All Fields Test"""

    @staticmethod
    def name():
        return 'heap_discovery'

    def streams_to_test(self):
        return self.expected_stream_names()
