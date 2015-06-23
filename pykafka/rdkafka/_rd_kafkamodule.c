#include <Python.h>
#include <structseq.h>

#include <errno.h>

#include <librdkafka/rdkafka.h>


/**
 * Exception types
 */

static PyObject *ConsumerStoppedException;
static PyObject *PyRdKafkaError;


static void
set_PyRdKafkaError(rd_kafka_resp_err_t err, const char *extra_msg) {
    // Raise an exception, carrying the error code at Exception.args[0]:
    PyObject *err_obj = Py_BuildValue("lss", (long)err,
                                             rd_kafka_err2str(err),
                                             extra_msg);
    PyErr_SetObject(PyRdKafkaError, err_obj);
    Py_DECREF(err_obj);
}


/**
 * Message type
 */

// The PyStructSequence we will use here is the C API equivalent of namedtuple;
// it is available in python 2.7 even though undocumented until python 3.3
static PyTypeObject MessageType;


static PyStructSequence_Field Message_fields[] = {
    // field names compatible with pykafka.protocol.Message:
    {"value", "message payload"},
    {"partition_key", "message key (used for partitioning)"},
    {"partition_id", "partition that message originates from"},
    {"offset", "message offset within partition"},
    {NULL}
};


static PyStructSequence_Desc Message_desc = {
    "pykafka.rdkafka.Message",
    NULL,  // TODO docstring
    Message_fields,
    4
};


/**
 * Consumer type
 */


typedef struct {
    PyObject_HEAD
    rd_kafka_t *rdk_handle;
    rd_kafka_conf_t *rdk_conf;
    rd_kafka_queue_t *rdk_queue_handle;
    rd_kafka_topic_t *rdk_topic_handle;
    rd_kafka_topic_conf_t *rdk_topic_conf;
    PyObject *partition_ids;
} Consumer;


static PyObject *
Consumer_stop(Consumer *self, PyObject *args) {
    // Call stop on all partitions, then destroy all handles

    PyObject *retval = Py_None;
    if (self->rdk_topic_handle != NULL) {
        Py_ssize_t i, len = PyList_Size(self->partition_ids);
        for (i = 0; i != len; ++i) {
            // Error handling here is a bit poor; we cannot bail out directly
            // if we want to clean up as much as we can.  TODO logging
            long part_id = PyInt_AsLong(PyList_GetItem(self->partition_ids, i));
            if (part_id == -1) {
                retval = NULL;
                continue;
            }
            if (-1 == rd_kafka_consume_stop(self->rdk_topic_handle, part_id)) {
                set_PyRdKafkaError(rd_kafka_errno2err(errno), NULL);
                retval = NULL;
                continue;
            }
        }
        Py_CLEAR(self->partition_ids);
        rd_kafka_topic_destroy(self->rdk_topic_handle);
        self->rdk_topic_handle = NULL;
    }
    if (self->rdk_queue_handle != NULL) {
        rd_kafka_queue_destroy(self->rdk_queue_handle);
        self->rdk_queue_handle = NULL;
    }
    if (self->rdk_handle != NULL) {
        rd_kafka_destroy(self->rdk_handle);
        self->rdk_handle = NULL;
    }
    if (self->rdk_conf) {
        rd_kafka_conf_destroy(self->rdk_conf);
        self->rdk_conf = NULL;
    }
    if (self->rdk_topic_conf) {
        rd_kafka_topic_conf_destroy(self->rdk_topic_conf);
        self->rdk_topic_conf = NULL;
    }
    Py_XINCREF(retval);
    return retval;
}


static void
Consumer_dealloc(PyObject *self) {
    PyObject *stop_result = Consumer_stop((Consumer *)self, NULL);
    if (!stop_result) {
        // TODO log exception but do not re-raise
    } else {
        Py_DECREF(stop_result);
    }
    self->ob_type->tp_free(self);
}


static const char Consumer_configure__doc__[] =
"Set up and populate the rd_kafka_(topic_)conf_t\n"
"\n"
"Somewhat inelegantly (for the benefit of code reuse, whilst avoiding some\n"
"harrowing partial binding for C functions) this requires that you call it\n"
"twice, once with a `conf` list only, and again with `topic_conf` only.\n"
"\n"
"Repeated calls work incrementally; you can wipe configuration completely\n"
"by calling Consumer_stop()\n";
static PyObject *
Consumer_configure(Consumer *self, PyObject *args, PyObject *kwds) {
    char *keywords[] = {"conf", "topic_conf", NULL};
    PyObject *conf = NULL;
    PyObject *topic_conf = NULL;
    if (! PyArg_ParseTupleAndKeywords(args,
                                      kwds,
                                      "|OO",
                                      keywords,
                                      &conf,
                                      &topic_conf)) return NULL;

    if ((conf && topic_conf) || (!conf && !topic_conf)) {
        set_PyRdKafkaError(
            RD_KAFKA_RESP_ERR__FAIL,
            "You need to specify *either* `conf` *or* `topic_conf`.");
        return NULL;
    }
    if (self->rdk_handle) {
        set_PyRdKafkaError(
            RD_KAFKA_RESP_ERR__FAIL,
            "Cannot configure: seems instance was started already?");
        return NULL;
    }

    if (! self->rdk_conf) self->rdk_conf = rd_kafka_conf_new();
    if (! self->rdk_topic_conf) {
        self->rdk_topic_conf = rd_kafka_topic_conf_new();
    }

    PyObject *conf_or_topic_conf = topic_conf ? topic_conf : conf;
    Py_ssize_t i, len = PyList_Size(conf_or_topic_conf);
    for (i = 0; i != len; ++i) {
        PyObject *conf_pair = PyList_GetItem(conf_or_topic_conf, i);
        const char *name = NULL;
        const char *value =  NULL;
        if (! PyArg_ParseTuple(conf_pair, "ss", &name, &value)) return NULL;

        char errstr[512];
        rd_kafka_conf_res_t res;
        if (topic_conf) {
            res = rd_kafka_topic_conf_set(
                    self->rdk_topic_conf, name, value, errstr, sizeof(errstr));
        } else {
            res = rd_kafka_conf_set(
                    self->rdk_conf, name, value, errstr, sizeof(errstr));
        }
        if (res != RD_KAFKA_CONF_OK) {
            set_PyRdKafkaError(RD_KAFKA_RESP_ERR__FAIL, errstr);
            return NULL;
        }
    }
    Py_INCREF(Py_None);
    return Py_None;
}


static PyObject *
Consumer_start(Consumer *self, PyObject *args, PyObject *kwds) {
    char *keywords[] = {
        "brokers",
        "topic_name",
        "partition_ids",
        "start_offsets",  // same order as partition_ids
        NULL};
    const char *brokers = NULL;
    const char *topic_name = NULL;
    PyObject *partition_ids = NULL;
    PyObject *start_offsets= NULL;
    if (! PyArg_ParseTupleAndKeywords(args,
                                      kwds,
                                      "ssOO",
                                      keywords,
                                      &brokers,
                                      &topic_name,
                                      &partition_ids,
                                      &start_offsets)) {
        return NULL;
    }

    // We'll keep our own copy of partition_ids, because the one handed to us
    // might be mutable, and weird things could happen if the list used on init
    // is different than that on dealloc
    if (self->partition_ids) {
        set_PyRdKafkaError(RD_KAFKA_RESP_ERR__FAIL, "Already started.");
        return NULL;
    }
    self->partition_ids = PySequence_List(partition_ids);
    if (! self->partition_ids) return NULL;

    // Configure and start a new RD_KAFKA_CONSUMER
    char errstr[512];
    self->rdk_handle = rd_kafka_new(
            RD_KAFKA_CONSUMER, self->rdk_conf, errstr, sizeof(errstr));
    self->rdk_conf = NULL;  // deallocated by rd_kafka_new()
    if (! self->rdk_handle) {
        set_PyRdKafkaError(RD_KAFKA_RESP_ERR__FAIL, errstr);
        return NULL;
    }
    if (rd_kafka_brokers_add(self->rdk_handle, brokers) == 0) {
        // XXX add brokers via conf setting instead?
        set_PyRdKafkaError(RD_KAFKA_RESP_ERR__FAIL, "adding brokers failed");
        goto fail;
    }

    // Configure and take out a topic handle
    self->rdk_topic_handle =
        rd_kafka_topic_new(self->rdk_handle, topic_name, self->rdk_topic_conf);
    self->rdk_topic_conf = NULL;  // deallocated by rd_kafka_topic_new()
    if (! self->rdk_topic_handle) {
        set_PyRdKafkaError(rd_kafka_errno2err(errno), NULL);
        goto fail;
    }

    // Start a queue and add all partition_ids to it
    self->rdk_queue_handle = rd_kafka_queue_new(self->rdk_handle);
    if (! self->rdk_queue_handle) {
        set_PyRdKafkaError(RD_KAFKA_RESP_ERR__FAIL, "could not get queue");
        goto fail;
    }
    Py_ssize_t i, len = PyList_Size(self->partition_ids);
    for (i = 0; i != len; ++i) {
        // We don't do much type-checking on partition_ids/start_offsets as this
        // module is intended solely for use with the py class that wraps it
        int32_t part_id = PyInt_AsLong(PyList_GetItem(self->partition_ids, i));
        if (part_id == -1 && PyErr_Occurred()) goto fail;
        PyObject *offset_obj = PySequence_GetItem(start_offsets, i);
        if (! offset_obj) goto fail;  // shorter seq than partition_ids?
        int64_t offset = PyLong_AsLongLong(offset_obj);
        if (-1 == rd_kafka_consume_start_queue(self->rdk_topic_handle,
                                               part_id,
                                               offset,
                                               self->rdk_queue_handle)) {
            set_PyRdKafkaError(rd_kafka_errno2err(errno), NULL);
            goto fail;
        }
    }
    Py_INCREF(Py_None);
    return Py_None;

fail:  ;
    PyObject *err_type, *err_value, *err_traceback;
    PyErr_Fetch(&err_type, &err_value, &err_traceback);

    PyObject *stop_result = Consumer_stop(self, NULL);
    // Consumer_stop is likely to raise exceptions, since init was incomplete:
    if (! stop_result) PyErr_Clear();
    else Py_DECREF(stop_result);

    PyErr_Restore(err_type, err_value, err_traceback);
    return NULL;
}


static PyObject *
Consumer_consume(Consumer *self, PyObject *args) {
    int timeout_ms = 0;
    if (! PyArg_ParseTuple(args, "i", &timeout_ms)) return NULL;
    if (! self->rdk_queue_handle) {
        PyErr_SetNone(ConsumerStoppedException);
        return NULL;
    }

    rd_kafka_message_t *rkmessage;
    rkmessage = rd_kafka_consume_queue(self->rdk_queue_handle, timeout_ms);
    if (!rkmessage) {
        // Either ETIMEDOUT or ENOENT occurred, but the latter would imply we
        // forgot to call rd_kafka_consume_start_queue, which is unlikely in
        // this setup.  We'll assume it was ETIMEDOUT then:
        Py_INCREF(Py_None);
        return Py_None;
    }
    PyObject *retval = NULL;
    if (rkmessage->err == RD_KAFKA_RESP_ERR_NO_ERROR) {
        retval = PyStructSequence_New(&MessageType);
        PyStructSequence_SET_ITEM(retval, 0, PyBytes_FromStringAndSize(
                                  rkmessage->payload, rkmessage->len));
        PyStructSequence_SET_ITEM(retval, 1, PyBytes_FromStringAndSize(
                                  rkmessage->key, rkmessage->key_len));
        PyStructSequence_SET_ITEM(retval, 2, PyLong_FromLong(
                                  rkmessage->partition));
        PyStructSequence_SET_ITEM(retval, 3, PyLong_FromLongLong(
                                  rkmessage->offset));
    } else if (rkmessage->err == RD_KAFKA_RESP_ERR__PARTITION_EOF) {
        // Whenever we get to the head of a partition, we get this.  There may
        // be messages available in other partitions, so if we want to match
        // pykafka.SimpleConsumer behaviour, we ought to avoid breaking any
        // iteration loops, and simply skip over this one altogether:
        retval = Consumer_consume(self, args);
    } else {
        set_PyRdKafkaError(rkmessage->err, NULL);
    }
    rd_kafka_message_destroy(rkmessage);
    return retval;
}


static PyMethodDef Consumer_methods[] = {
    {"consume", (PyCFunction)Consumer_consume,
        METH_VARARGS, "Consume from kafka."},
    {"stop", (PyCFunction)Consumer_stop, METH_NOARGS, "Destroy consumer."},
    {"configure", (PyCFunction)Consumer_configure,
        METH_KEYWORDS, Consumer_configure__doc__},
    {"start", (PyCFunction)Consumer_start, METH_VARARGS | METH_KEYWORDS, NULL},
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
    0,                             /* tp_init */
};


/**
 * Debugging helpers
 */

static PyObject *
debug_thread_cnt(PyObject *self, PyObject *args) {
    return PyLong_FromLong(rd_kafka_thread_cnt());
}


static PyObject *
debug_wait_destroyed(PyObject *self, PyObject *arg) {
    int timeout_ms = PyLong_AsLong(arg);
    if (timeout_ms == -1 && PyErr_Occurred()) return NULL;
    int res = rd_kafka_wait_destroyed(timeout_ms);
    if (res == -1) {
        set_PyRdKafkaError(RD_KAFKA_RESP_ERR__FAIL,
                           "rd_kafka_wait_destroyed timed out");
        return NULL;
    }
    Py_INCREF(Py_None);
    return Py_None;
}


/**
 * Module init
 */

static PyMethodDef pyrdk_methods[] = {
    {"_thread_cnt", debug_thread_cnt, METH_NOARGS, NULL},
    {"_wait_destroyed", debug_wait_destroyed, METH_O, NULL},
    {NULL, NULL, 0, NULL}
};


PyMODINIT_FUNC
init_rd_kafka(void) {
    PyObject *mod = Py_InitModule("pykafka.rdkafka._rd_kafka", pyrdk_methods);
    if (mod == NULL) return;

    ConsumerStoppedException = PyErr_NewException(
            "pykafka.rdkafka.ConsumerStoppedException", NULL, NULL);
    if (! ConsumerStoppedException) return;
    Py_INCREF(ConsumerStoppedException);
    PyModule_AddObject(
            mod, "ConsumerStoppedException", ConsumerStoppedException);
    PyRdKafkaError = PyErr_NewException("pykafka.rdkafka.Error", NULL, NULL);
    if (!PyRdKafkaError) return; // TODO goto error handler
    Py_INCREF(PyRdKafkaError);
    PyModule_AddObject(mod, "Error", PyRdKafkaError);

    if (MessageType.tp_name == NULL) {
        PyStructSequence_InitType(&MessageType, &Message_desc);
    }
    Py_INCREF(&MessageType);
    PyModule_AddObject(mod, "Message", (PyObject *)&MessageType);

    ConsumerType.tp_new = PyType_GenericNew;
    if (PyType_Ready(&ConsumerType) != 0) return;
    Py_INCREF(&ConsumerType);
    PyModule_AddObject(mod, "Consumer", (PyObject *)&ConsumerType);
}
