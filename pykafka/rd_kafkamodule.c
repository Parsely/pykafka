#include <Python.h>
#include <librdkafka/rdkafka.h>


static rd_kafka_t *rk;
static rd_kafka_topic_t *rkt;
static char *brokers = "kafka:9092";
static char *topic = "hello";
static char errstr[512];


static PyObject *
consume(PyObject *self, PyObject *args) {
    rd_kafka_message_t *rkmessage;
    rkmessage = rd_kafka_consume(rkt, 0, 1000); // TODO partition
    if (!rkmessage) {
        // TODO exception
        return NULL;
    }
    PyObject *retval = Py_BuildValue("s#s#i",
                                     rkmessage->payload, rkmessage->len,
                                     rkmessage->key, rkmessage->key_len,
                                     rkmessage->offset);
    rd_kafka_message_destroy(rkmessage);
    return retval;
}


static PyMethodDef RdkMethods[] = {
    {"consume", consume, METH_VARARGS, "Consume from kafka."},
    {NULL, NULL, 0, NULL}
};


PyMODINIT_FUNC
initrd_kafka(void) {
    (void) Py_InitModule("rd_kafka", RdkMethods);

    rd_kafka_conf_t *conf = rd_kafka_conf_new();
    rd_kafka_topic_conf_t *topic_conf = rd_kafka_topic_conf_new();

    rk = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr));
    if (!rk) {
        // TODO clean up conf, topic_conf, rk
        // TODO raise suitable exception
        return;
    }

    if (rd_kafka_brokers_add(rk, brokers) == 0) {
        // TODO clean up + except
        return;
    }

    rkt = rd_kafka_topic_new(rk, topic, topic_conf);
    if (rd_kafka_consume_start(rkt, 0, 0) == -1) { // TODO partition, offset
        // TODO clean up + except
        return;
    }
}
