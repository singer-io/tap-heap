import os
import random
from tap_tester import connections, menagerie, runner, LOGGER
from tap_tester.base_suite_tests.base_case import BaseCase

class TapHeapBaseCase(BaseCase):
    start_date = "2021-04-07T00:00:00Z"

    @staticmethod
    def tap_name():
        return 'tap-heap'

    @staticmethod
    def get_type():
        return 'platform.heap'

    @staticmethod
    def get_credentials():
        return {}

    @staticmethod
    def expected_metadata():
        return {
            'users': {
                BaseCase.PRIMARY_KEYS: {'user_id'}
            },
            'user_migrations': {
                BaseCase.PRIMARY_KEYS: {'from_user_id'}
            },
            'click_on_anything': {
                BaseCase.PRIMARY_KEYS: {'event_id'}
            },
            'pageviews': {
                BaseCase.PRIMARY_KEYS: {'event_id'}
            }
        }

    def get_properties(self):
        return {
            'start_date': self.start_date,
            'bucket': 'com-stitchdata-dev-tap-heap',
            'account_id': '218546966473',
            'role_name': f'stitch_heap_{random.randint(1000,9999)}',
            'external_id': f'stitch_connection_{random.randint(1000,9999)}'
        }
