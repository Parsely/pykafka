from tests.pykafka import test_simpleconsumer
from pykafka.rdkafka import RdKafkaSimpleConsumer


class TestRdKafkaSimpleConsumer(test_simpleconsumer.TestSimpleConsumer):
    def _get_simple_consumer(self, **kwargs):
        # This enables automatic reuse of all tests from test_simpleconsumer
        topic = self.client.topics[self.topic_name]
        return RdKafkaSimpleConsumer(
            topic=topic,
            cluster=topic._cluster,
            **kwargs)

    def test_offset_commit_agrees(self):
        """Check rdkafka-obtained offsets arrive correctly

        In RdKafkaSimpleConsumer.consume we bypass most of the internals of
        simpleconsumer.OwnedPartition, but then expect it to still commit
        offsets for us correctly.  This warrants very explicit testing.
        """
        consumer = self._get_simple_consumer(
            consumer_group='test_offset_commit_agrees')
        latest_offs = _latest_partition_offsets_by_reading(consumer, 100)
        consumer.commit_offsets()

        # We can only compare partitions we've consumed from, so filter:
        retrieved_offs = {r[0]: r[1].offset for r in consumer.fetch_offsets()
                          if r[0] in latest_offs}

        self.assertEquals(retrieved_offs, latest_offs)
        consumer.stop()

    def test_offset_resume_agrees(self):
        """Check the rdkafka consumer returns messages at specified offset

        Make sure reads from the underlying rdkafka consumer really do start
        at the offsets dictated by SimpleConsumer
        """
        consumer = self._get_simple_consumer(
            consumer_group='test_offset_resume_agrees')
        latest_offs = _latest_partition_offsets_by_reading(consumer, 100)
        consumer.commit_offsets()
        consumer.stop()

        consumer = self._get_simple_consumer(
            consumer_group='test_offset_resume_agrees')
        while latest_offs: # check each partition, then tick it off:
            msg = consumer.consume()
            if msg.partition_id not in latest_offs:
                # ie we didn't get to this partition previously
                continue
            expected_offset = latest_offs[msg.partition_id] + 1
            self.assertEquals(msg.offset, expected_offset)
            del latest_offs[msg.partition_id]
        consumer.stop()


def _latest_partition_offsets_by_reading(consumer, n_reads):
    """Obtain message offsets from consumer, return grouped by partition"""
    latest_offs = {}
    for _ in xrange(n_reads):
        msg = consumer.consume()
        latest_offs[msg.partition_id] = msg.offset
    return latest_offs
