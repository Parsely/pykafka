import mock
import unittest2

from samsa.topics import TopicMap, Topic


class TopicMapTest(unittest2.TestCase):
    def test_get_topic(self):
        topics = TopicMap(cluster=mock.Mock())
        topic = topics.get('topic-1')
        self.assertIsInstance(topic, Topic)

        # Retrieving the topic again should return the same object instance.
        self.assertIs(topic, topics.get('topic-1'))
