#include <Python.h>
#include <librdkafka/rdkafka.h>


typedef struct {
    PyObject_HEAD
    rd_kafka_t *rdk_handle;
    rd_kafka_topic_t *rdk_topic_handle;
} Consumer;


static void
Consumer_dealloc(Consumer *self) {
    if (self->rdk_topic_handle != NULL) {
        rd_kafka_consume_stop(self->rdk_topic_handle, 0); // TODO partitions
        rd_kafka_topic_destroy(self->rdk_topic_handle);
    }
    if (self->rdk_handle != NULL) rd_kafka_destroy(self->rdk_handle);
    self->ob_type->tp_free((PyObject*)self);
}


static int
Consumer_init(Consumer *self, PyObject *args, PyObject *kwds) {
    char *keywords[] = {"brokers", "topic_name", NULL};
    const char *brokers = NULL;
    const char *topic_name = NULL;
    if (! PyArg_ParseTupleAndKeywords(
                args, kwds, "ss", keywords, &brokers, &topic_name)) return -1;

    rd_kafka_conf_t *conf = rd_kafka_conf_new();
    char errstr[512];
    self->rdk_handle = rd_kafka_new(
            RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr));
    if (! self->rdk_handle) return 0;  // TODO set exception, return -1
    if (rd_kafka_brokers_add(self->rdk_handle, brokers) == 0) {
        // TODO set exception, return -1
        // XXX add brokers via conf setting instead?
        return 0;
    }

    rd_kafka_topic_conf_t *topic_conf = rd_kafka_topic_conf_new();
    self->rdk_topic_handle =
        rd_kafka_topic_new(self->rdk_handle, topic_name, topic_conf);
    if (rd_kafka_consume_start(self->rdk_topic_handle, 0, 0) == -1) {  // TODO partition, offset
        // TODO set exception
        return 0;
    }

    return 0;
}


static PyObject *
Consumer_consume(PyObject *self, PyObject *args) {
    rd_kafka_message_t *rkmessage;
    rkmessage = rd_kafka_consume(((Consumer *)self)->rdk_topic_handle, 0, 1000); // TODO partition, timeout
    if (!rkmessage) {
        // TODO exception
        return NULL;
    }
    // TODO check rkmessage->err
    PyObject *retval = Py_BuildValue("s#s#i",
                                     rkmessage->payload, rkmessage->len,
                                     rkmessage->key, rkmessage->key_len,
                                     rkmessage->offset);
    rd_kafka_message_destroy(rkmessage);
    return retval;
}


static PyMethodDef Consumer_methods[] = {
    {"consume", Consumer_consume, METH_VARARGS, "Consume from kafka."},
    {NULL, NULL, 0, NULL}
};


static PyTypeObject ConsumerType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "pykafka.rd_kafka.Consumer",
    sizeof(Consumer),
    0,                             /*tp_itemsize*/
    (destructor)Consumer_dealloc,  /*tp_dealloc*/
    0,                             /*tp_print*/
    0,                             /*tp_getattr*/
    0,                             /*tp_setattr*/
    0,                             /*tp_compare*/
    0,                             /*tp_repr*/
    0,                             /*tp_as_number*/
    0,                             /*tp_as_sequence*/
    0,                             /*tp_as_mapping*/
    0,                             /*tp_hash */
    0,                             /*tp_call*/
    0,                             /*tp_str*/
    0,                             /*tp_getattro*/
    0,                             /*tp_setattro*/
    0,                             /*tp_as_buffer*/
    Py_TPFLAGS_DEFAULT,            /*tp_flags*/
    0,                             /* tp_doc */
    0,                             /* tp_traverse */
    0,                             /* tp_clear */
    0,                             /* tp_richcompare */
    0,                             /* tp_weaklistoffset */
    0,                             /* tp_iter */
    0,                             /* tp_iternext */
    Consumer_methods,              /* tp_methods */
    0,                             /* tp_members */
    0,                             /* tp_getset */
    0,                             /* tp_base */
    0,                             /* tp_dict */
    0,                             /* tp_descr_get */
    0,                             /* tp_descr_set */
    0,                             /* tp_dictoffset */
    (initproc)Consumer_init,       /* tp_init */
};


PyMODINIT_FUNC
initrd_kafka(void) {
    PyObject *mod = Py_InitModule("rd_kafka", NULL);
    if (mod == NULL) return;

    ConsumerType.tp_new = PyType_GenericNew;
    if (PyType_Ready(&ConsumerType) != 0) return;
    Py_INCREF(&ConsumerType);

    PyModule_AddObject(mod, "Consumer", (PyObject *)&ConsumerType);
}
