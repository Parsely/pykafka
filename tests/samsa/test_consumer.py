import mock

from kazoo.testing import KazooTestCase

from samsa.cluster import Cluster
from samsa.topics import Topic
from samsa import consumer


class TestPartitionOwnerRegistry(KazooTestCase):

    def setUp(self):
        super(TestPartitionOwnerRegistry, self).setUp()
        self.c = Cluster(self.client)

        self.consumer = mock.Mock()
        self.consumer.id  = 1234
        self.topic = mock.Mock()
        self.topic.name = 'topic'

        self.por = consumer.PartitionOwnerRegistry(
            self.consumer,
            self.c,
            self.topic,
            'group'
        )

        self.partitions = [consumer.PartitionName(str(i), '0') for i in
                           xrange(5)]

    def test_crd(self):
        self.por.add(self.partitions[:3])
        self.assertEquals(
            self.por.get(),
            set(self.partitions[:3])
        )

        self.por.remove([self.partitions[0]])
        self.assertEquals(
            self.por.get(),
            set(self.partitions[1:3])
        )



class TestConsumer(KazooTestCase):

    def setUp(self):
        super(TestConsumer, self).setUp()
        self.c = Cluster(self.client)

    def _register_fake_brokers(self, n=1):
        self.client.ensure_path("/brokers/ids")
        for i in xrange(n):
            path = "/brokers/ids/%d" % i
            data = "127.0.0.1"
            self.client.create(path, data)

    def test_assigns_partitions(self):
        self._register_fake_brokers(5)
        t = Topic(self.c, 'mwhooker')

        c = t.subscribe('group1')

        c2 = t.subscribe('group1')

        self.assertEquals(len(c.partitions) + len(c2.partitions), 5)

        print "c partitions: ", c.partitions
        print "c2 partitions: ", c2.partitions
        """
        self.assertEquals(
            c.partitions,
            set([consumer.PartitionName(broker_id=0, partition_id=0),
                 consumer.PartitionName(broker_id=1, partition_id=0),
                 consumer.PartitionName(broker_id=2, partition_id=0)])
        )
        """
