import mock

from kazoo.testing import KazooTestCase

from samsa.cluster import Cluster
from samsa.topics import Topic
from samsa.partitions import Partition
from samsa import consumer


class TestPartitionOwnerRegistry(KazooTestCase):

    def setUp(self):
        super(TestPartitionOwnerRegistry, self).setUp()
        self.c = Cluster(self.client)
        self.c.brokers = mock.MagicMock()
        broker = mock.Mock()
        broker.id = 1
        self.c.brokers.__getitem__.return_value = broker

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

        self.partitions = []
        for i in xrange(5):
            self.partitions.append(
                Partition(self.c, self.topic, broker, i)
            )

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

    def test_watch(self):
        self.por.add(self.partitions)

        por2 = consumer.PartitionOwnerRegistry(
            self.consumer,
            self.c,
            self.topic,
            'group'
        )

        self.assertEquals(self.por.get(), por2.get())
        self.assertEquals(self.por.get(), set(self.partitions))

    def test_grows(self):

        partitions = self.por.get()
        self.assertEquals(len(partitions), 0)

        self.por.add(self.partitions)
        self.assertEquals(len(partitions), len(self.partitions))


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
        """
        Test rebalance

        Adjust n_* to see how rebalancing performs.
        """

        n_partitions = 10
        n_consumers = 3
        self._register_fake_brokers(n_partitions)
        t = Topic(self.c, 'mwhooker')

        consumers = [t.subscribe('group1') for i in xrange(n_consumers)]

        partitions = []
        for c in consumers:
            partitions.extend(c.partitions)

        # test that there are no duplicates.
        self.assertEquals(len(partitions), n_partitions)
        # test that every partitions is represented.
        self.assertEquals(len(set(partitions)), n_partitions)
