from unittest import TestCase, main

from tap_heap.manifest import merge

class MergeTestCase(TestCase):
    def test_merge_simple(self):
        self.assertEqual({'foo': 'bar'}, merge({}, {'foo': 'bar'}))
        self.assertEqual({'foo': 'bar'}, merge({'foo': 'bar'}, {}))
        self.assertEqual({'foo': 'bar', 'bat': 'bin'},
                         merge({'bat': 'bin'}, {'foo': 'bar'}))
        self.assertEqual({'bat': {'bar': {'boo', 'bin'}}},
                         merge({'bat': {'bar': {'bin'}}}, {'bat': {'bar': {'boo'}}}))
        self.assertEqual({'bat': {'bar': {'boo'}}},
                         merge({}, {'bat': {'bar': {'boo'}}}))

if __name__ == '__main__':
    main()
