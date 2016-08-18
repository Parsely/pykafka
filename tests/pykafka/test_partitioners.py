import unittest2

from hashlib import sha1
from pykafka.partitioners import GroupHashingPartitioner


class TestGroupHashingPartitioner(unittest2.TestCase):

    def test_valid_inputs_success(self):
        # Test data; 1st element is group size, 2nd is total partition count, 3rd is the key
        key = 'foo'.encode('utf-8')
        data = [[1, 16, key],
                [2, 16, key],
                [4, 16, key],
                [16, 16, key],
                [1, 1, key]]
        for row in data:
            self._run_test(row[0], row[1], row[2])

    def test_invalid_inputs_error(self):
        key = 'foo'.encode('utf-8')
        data = [[0, 16, key],
                [17, 16, key]]
        for row in data:
            with self.assertRaises(ValueError):
                self._run_test(row[0], row[1], row[2])
                continue

    def test_create_with_zero_group_size_raises_error(self):
        with self.assertRaises(ValueError):
            GroupHashingPartitioner(hash_func=None, group_size=0)

    def test_create_with_negative_group_size_raises_error(self):
        with self.assertRaises(ValueError):
            GroupHashingPartitioner(hash_func=None, group_size=-1)

    def test_missing_hash_function_raises_error(self):
        with self.assertRaises(ValueError):
            GroupHashingPartitioner(hash_func=None, group_size=1)

    def _run_test(self, group_size, total_partition_count, key):
        def hash_func(k): return int(sha1(k).hexdigest(), 16)
        # Instead of one hash for each key, generate a list of n possible hashes, where n=group_size
        hashed_keys = [abs(hash_func(key) + x) for x in range(group_size)]
        # Obtain a list of valid partitions
        valid_partitions = list(map(lambda x: x % total_partition_count, hashed_keys))
        self.assertEquals(len(valid_partitions), group_size)

        # Call the partitioner and check that the returned partition is in the valid list
        partitioner = GroupHashingPartitioner(hash_func, group_size)
        x = partitioner.__call__(list(range(total_partition_count)), key)
        self.assertTrue(x in valid_partitions)
