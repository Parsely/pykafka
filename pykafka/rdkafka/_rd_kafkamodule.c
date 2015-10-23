#define PY_SSIZE_T_CLEAN

#include <Python.h>
#include <structmember.h>
#include <structseq.h>

#include <errno.h>
#include <syslog.h>

#include <librdkafka/rdkafka.h>


/**
 * Logging
 */

static PyObject *logger;


static void
logging_callback(const rd_kafka_t *rk,
                 int level,
                 const char *fac,
                 const char *buf)
{
    /* Map syslog levels to python logging levels */
    char *lvl = NULL;
    if (level == LOG_DEBUG) lvl = "debug";
    else if (level == LOG_INFO || level == LOG_NOTICE) lvl = "info";
    else if (level == LOG_WARNING) lvl = "warning";
    else if (level == LOG_ERR) lvl = "error";
    else lvl = "critical";

    /* NB librdkafka docs say that rk may be NULL, so check that */
    const char *rk_name = rk ? rd_kafka_name(rk) : "rk_handle null";
    const char *format = "%s [%s] %s";  /* format rk_name + fac + buf */

    /* Grab the GIL, as rdkafka callbacks may come from non-python threads */
    PyGILState_STATE gstate = PyGILState_Ensure();

    PyObject *res = PyObject_CallMethod(
            logger, lvl, "ssss", format, rk_name, fac, buf);
    /* Any errors here we'll just have to swallow: we're probably on some
       background thread, and we can't log either (logging just failed!) */
    if (! res) PyErr_Clear();
    else Py_DECREF(res);

    PyGILState_Release(gstate);
}


/**
 * Exception types
 */

static PyObject *pykafka_exceptions;


/* Raise an exception from pykafka.exceptions (always returns NULL, to allow
 * shorthand `return set_pykafka_error("Exception", "error message")`) */
static PyObject *
set_pykafka_error(const char *err_name, const char *err_msg)
{
    PyObject *error = PyObject_GetAttrString(pykafka_exceptions, err_name);
    if (! error) return NULL;
    PyErr_SetString(error, err_msg);
    Py_DECREF(error);
    return NULL;
}


/* Given an error code, find the most fitting class from pykafka.exceptions */
static PyObject *
find_pykafka_error(rd_kafka_resp_err_t err)
{
    PyObject *error_codes = NULL;
    PyObject *errcode = NULL;
    PyObject *Exc = NULL;

    /* See if there's a standard Kafka error for this */
    error_codes = PyObject_GetAttrString(pykafka_exceptions, "ERROR_CODES");
    if (! error_codes) goto cleanup;
    errcode = PyLong_FromLong(err);
    if (! errcode) goto cleanup;
    Exc = PyObject_GetItem(error_codes, errcode);

    if (! Exc) {  /* raise a generic exception instead */
        PyErr_Clear();
        Exc = PyObject_GetAttrString(pykafka_exceptions, "RdKafkaException");
    }
cleanup:
    Py_XDECREF(error_codes);
    Py_XDECREF(errcode);
    return Exc;
}


/* Given an error code, set a suitable exception; or, if return_error is not
 * NULL, pass the exception instance back through return_error instead */
static void
set_pykafka_error_from_code(rd_kafka_resp_err_t err, PyObject **return_error)
{
    PyObject *error = NULL;
    PyObject *err_args = NULL;

    error = find_pykafka_error(err);
    if (! error) goto cleanup;
    err_args = Py_BuildValue("ls", (long)err, rd_kafka_err2str(err));
    if (! err_args) goto cleanup;

    if (! return_error) PyErr_SetObject(error, err_args);
    else (*return_error) = PyObject_CallObject(error, err_args);
cleanup:
    Py_XDECREF(error);
    Py_XDECREF(err_args);
}


/**
 * Message type
 */

PyDoc_STRVAR(MessageType__doc__,
"A kafka message with field names compatible with pykafka.protocol.Message\n"
"\n"
"In addition to value, partition_key, offset, this offers partition_id.");


/* The PyStructSequence we use here is the C-API equivalent of namedtuple; it
   is available in python 2.7 even though undocumented until python 3.3 */
static PyTypeObject MessageType;


static PyStructSequence_Field Message_fields[] = {
    {"value", "message payload"},
    {"partition_key", "message key (used for partitioning)"},
    {"partition_id", "partition that message originates from"},
    {"offset", "message offset within partition"},
    {NULL}
};


static PyStructSequence_Desc Message_desc = {
    "pykafka.rdkafka.Message",
    MessageType__doc__,
    Message_fields,
    4
};


/**
 * Shared bits of Producer and Consumer types
 */


/* Note that with this RdkHandle, we hold a separate rd_kafka_t handle for each
 * rd_kafka_topic_t, whereas librdkafka would allow sharing the same rd_kafka_t
 * handle between many topic handles, which would be far more efficient.  The
 * problem with that is that it would require the same rd_kafka_conf_t settings
 * across all class instances sharing a handle, which is somewhat incompatible
 * with the current pykafka API. */
typedef struct {
    PyObject_HEAD
    rd_kafka_t *rdk_handle;
    rd_kafka_conf_t *rdk_conf;
    rd_kafka_topic_t *rdk_topic_handle;
    rd_kafka_topic_conf_t *rdk_topic_conf;

    /* Consumer-specific fields */
    rd_kafka_queue_t *rdk_queue_handle;
    PyObject *partition_ids;

    /* Producer-specific fields; see Producer_produce for details */
    PyObject *pending_futures;
    size_t  pending_futures_uid;
} RdkHandle;


/* Only for inspection; if you'd manipulate these from outside the module, we
 * have no error checking in place to protect from ensuing mayhem */
static PyMemberDef RdkHandle_members[] = {
    {"_partition_ids", T_OBJECT_EX, offsetof(RdkHandle, partition_ids),
                       READONLY, "Partitions fetched from by this consumer"},
    {"_pending_futures", T_OBJECT_EX, offsetof(RdkHandle, pending_futures),
                         READONLY, "Futures pending a delivery report"},
    {NULL}
};


static PyObject *
RdkHandle_outq_len(RdkHandle *self) {
    if (! self->rdk_handle) {
        return set_pykafka_error("ProducerStoppedException", "");
    }
    int outq_len = -1;
    Py_BEGIN_ALLOW_THREADS  /* avoid callbacks deadlocking */
        outq_len = rd_kafka_outq_len(self->rdk_handle);
    Py_END_ALLOW_THREADS
    return Py_BuildValue("i", outq_len);
}


static PyObject *
RdkHandle_poll(RdkHandle *self, PyObject *args, PyObject *kwds)
{
    char *keywords[] = {"timeout_ms", NULL};
    int timeout_ms = 0;
    if (! PyArg_ParseTupleAndKeywords(args, kwds, "i", keywords, &timeout_ms)) {
            return NULL;
    }
    if (! self->rdk_handle) {
        return set_pykafka_error("ProducerStoppedException", "");
    }
    int n_events = 0;
    Py_BEGIN_ALLOW_THREADS  /* avoid callbacks deadlocking */
        n_events = rd_kafka_poll(self->rdk_handle, timeout_ms);
    Py_END_ALLOW_THREADS
    return Py_BuildValue("i", n_events);
}


static PyObject *
RdkHandle_stop(RdkHandle *self)
{
    /* NB Consumer_stop assumes this never raises exceptions, ie always returns
     * Py_None */

    Py_BEGIN_ALLOW_THREADS  /* avoid callbacks deadlocking */
        if (self->rdk_queue_handle) {
            rd_kafka_queue_destroy(self->rdk_queue_handle);
            self->rdk_queue_handle = NULL;
        }
        if (self->rdk_topic_handle) {
            rd_kafka_topic_destroy(self->rdk_topic_handle);
            self->rdk_topic_handle = NULL;
        }
        if (self->rdk_handle) {
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
    Py_END_ALLOW_THREADS
    Py_CLEAR(self->partition_ids);
    Py_CLEAR(self->pending_futures);

    Py_INCREF(Py_None);
    return Py_None;
}


static void
RdkHandle_dealloc(PyObject *self, PyObject *(*stop_func) (RdkHandle *))
{
    PyObject *stop_result = stop_func((RdkHandle *)self);
    if (!stop_result) {
        /* We'll swallow the exception, so let's try to log info first */
        PyObject *res = PyObject_CallMethod(
                logger, "exception", "s", "In dealloc: stop() failed.");
        PyErr_Clear();
        Py_XDECREF(res);
    } else {
        Py_DECREF(stop_result);
    }
    self->ob_type->tp_free(self);
}


PyDoc_STRVAR(RdkHandle_configure__doc__,
"Set up and populate the rd_kafka_(topic_)conf_t\n"
"\n"
"Somewhat inelegantly (for the benefit of code reuse, whilst avoiding some\n"
"harrowing partial binding for C functions) this requires that you call it\n"
"twice, once with a `conf` list only, and again with `topic_conf` only.\n"
"\n"
"Repeated calls work incrementally; you can wipe configuration completely\n"
"by calling Consumer_stop()\n");

static PyObject *
RdkHandle_configure(RdkHandle *self, PyObject *args, PyObject *kwds)
{
    char *keywords[] = {"conf", "topic_conf", NULL};
    PyObject *conf = NULL;
    PyObject *topic_conf = NULL;
    if (! PyArg_ParseTupleAndKeywords(args,
                                      kwds,
                                      "|OO",
                                      keywords,
                                      &conf,
                                      &topic_conf)) {
        return NULL;
    }

    if ((conf && topic_conf) || (!conf && !topic_conf)) {
        return set_pykafka_error(
            "RdKafkaException",
            "You need to specify *either* `conf` *or* `topic_conf`.");
    }
    if (self->rdk_handle) {
        return set_pykafka_error(
            "RdKafkaException",
            "Cannot configure: seems instance was started already?");
    }

    Py_BEGIN_ALLOW_THREADS  /* avoid callbacks deadlocking */
        if (! self->rdk_conf) self->rdk_conf = rd_kafka_conf_new();
        if (! self->rdk_topic_conf) {
            self->rdk_topic_conf = rd_kafka_topic_conf_new();
        }
    Py_END_ALLOW_THREADS

    PyObject *conf_or_topic_conf = topic_conf ? topic_conf : conf;
    Py_ssize_t i, len = PyList_Size(conf_or_topic_conf);
    for (i = 0; i != len; ++i) {
        PyObject *conf_pair = PyList_GetItem(conf_or_topic_conf, i);
        const char *name = NULL;
        const char *value =  NULL;
        if (! PyArg_ParseTuple(conf_pair, "ss", &name, &value)) return NULL;

        char errstr[512];
        rd_kafka_conf_res_t res;
        Py_BEGIN_ALLOW_THREADS  /* avoid callbacks deadlocking */
            if (topic_conf) {
                res = rd_kafka_topic_conf_set(
                    self->rdk_topic_conf, name, value, errstr, sizeof(errstr));
            } else {
                res = rd_kafka_conf_set(
                    self->rdk_conf, name, value, errstr, sizeof(errstr));
            }
        Py_END_ALLOW_THREADS
        if (res != RD_KAFKA_CONF_OK) {
            return set_pykafka_error("RdKafkaException", errstr);
        }
    }
    Py_INCREF(Py_None);
    return Py_None;
}


/* Cleanup helper for *_start(), returns NULL to allow shorthand in use */
static PyObject *
RdkHandle_start_fail(RdkHandle *self, PyObject *(*stop_func) (RdkHandle *))
{
    /* Something went wrong so we expect an exception has been set */
    PyObject *err_type, *err_value, *err_traceback;
    PyErr_Fetch(&err_type, &err_value, &err_traceback);

    PyObject *stop_result = stop_func(self);

    /* stop_func is likely to raise exceptions, as start was incomplete */
    if (! stop_result) PyErr_Clear();
    else Py_DECREF(stop_result);

    PyErr_Restore(err_type, err_value, err_traceback);
    return NULL;
}


static PyObject *
RdkHandle_start(RdkHandle *self,
                rd_kafka_type_t rdk_type,
                const char *brokers,
                const char *topic_name)
{
    /* Configure and start rdk_handle */
    char errstr[512];
    Py_BEGIN_ALLOW_THREADS  /* avoid callbacks deadlocking */
        self->rdk_handle = rd_kafka_new(
                rdk_type, self->rdk_conf, errstr, sizeof(errstr));
        self->rdk_conf = NULL;  /* deallocated by rd_kafka_new() */
    Py_END_ALLOW_THREADS
    if (! self->rdk_handle) {
        return set_pykafka_error("RdKafkaException", errstr);
    }

    /* Set logger and brokers */
    int brokers_added;
    Py_BEGIN_ALLOW_THREADS  /* avoid callbacks deadlocking */
        rd_kafka_set_logger(self->rdk_handle, logging_callback);
        brokers_added = rd_kafka_brokers_add(self->rdk_handle, brokers);
    Py_END_ALLOW_THREADS
    if (brokers_added == 0) {
        set_pykafka_error("RdKafkaException", "adding brokers failed");
        return RdkHandle_start_fail(self, RdkHandle_stop);
    }

    /* Configure and take out a topic handle */
    Py_BEGIN_ALLOW_THREADS  /* avoid callbacks deadlocking */
        self->rdk_topic_handle = rd_kafka_topic_new(self->rdk_handle,
                                                    topic_name,
                                                    self->rdk_topic_conf);
        self->rdk_topic_conf = NULL;  /* deallocated by rd_kafka_topic_new() */
    Py_END_ALLOW_THREADS
    if (! self->rdk_topic_handle) {
        set_pykafka_error_from_code(rd_kafka_errno2err(errno), NULL);
        return RdkHandle_start_fail(self, RdkHandle_stop);
    }

    Py_INCREF(Py_None);
    return Py_None;
}


/**
 * Producer type
 */


/* Import from concurrent.futures */
static PyObject *Future;


/* NB this doesn't check if RdkHandle_outq_len is zero, and generally assumes
 * the wrapping python class will take care of ensuring any such preconditions
 * for a clean termination */
static void
Producer_dealloc(PyObject *self)
{
    RdkHandle_dealloc(self, RdkHandle_stop);
}


/* Helper function for Producer_delivery_report_callback.  Because it is a
 * non-python callback, it cannot raise exceptions.  We therefore try to log
 * the traceback, then clear the exception */
static void
log_clear_exception(const char *msg)
{
    Py_XDECREF(PyObject_CallMethod(logger, "exception", "s", msg));
    PyErr_Clear();
}


/* Helper function for Producer_delivery_report_callback: find an exception
 * corresponding to `err`, and set that on `future`.  Returns -1 on failure,
 * or 0 on success */
static int
Producer_delivery_report_set_exception(PyObject *future,
                                       rd_kafka_resp_err_t err)
{
    int retval = 0;
    PyObject *exc = NULL;

    set_pykafka_error_from_code(err, &exc);
    if (! exc) goto cleanup;
    PyObject *res = PyObject_CallMethod(future, "set_exception", "O", exc);
    if (! res) goto cleanup;
    else Py_DECREF(res);

cleanup:
    if (PyErr_Occurred()) {
        log_clear_exception("Couldn't pass error to Future.set_exception");
        retval = -1;
    }
    Py_XDECREF(exc);
    return retval;
}


static void
Producer_delivery_report_callback(rd_kafka_t *rk,
                                  const rd_kafka_message_t *rkmessage,
                                  void *opaque)
{
    PyObject *key = NULL;
    PyObject *future = NULL;
    PyGILState_STATE gstate = PyGILState_Ensure();

    /* Producer_produce stored dict key in msg_opaque == rkmessage->_private */
    key = PyLong_FromSize_t((size_t)(rkmessage->_private));
    if (! key) goto cleanup;
    future = PyObject_GetItem((PyObject *)opaque, key);
    if (! future) goto cleanup;

    /* Drop future from pending_futures, which kept it alive till now */
    if (-1 == PyDict_DelItem((PyObject *)opaque, key)) goto cleanup;
    Py_DECREF(key);

    if (rkmessage->err == RD_KAFKA_RESP_ERR_NO_ERROR) {
        PyObject *res = PyObject_CallMethod(future, "set_result", "z", NULL);
        if (! res) log_clear_exception("Failure in Future.set_result");
        else Py_DECREF(res);
    } else {
        Producer_delivery_report_set_exception(future, rkmessage->err);
    }
    Py_DECREF(future);
cleanup:
    if (PyErr_Occurred()) {
        log_clear_exception("Problem retrieving future from pending_futures");
        Py_XDECREF(key);
        Py_XDECREF(future);
    }
    PyGILState_Release(gstate);
}


/* Starts the underlying rdkafka producer after configuring delivery-reporting.
 * Note that following start, you _must_ ensure that Producer.poll() is called
 * regularly, or delivery reports and unfulfilled Futures may pile up */
static PyObject *
Producer_start(RdkHandle *self, PyObject *args, PyObject *kwds)
{
    char *keywords[] = {"brokers", "topic_name", NULL};
    PyObject *brokers = NULL;
    PyObject *topic_name = NULL;
    if (! PyArg_ParseTupleAndKeywords(
            args, kwds, "SS", keywords, &brokers, &topic_name)) {
        return NULL;
    }

    /* Configure delivery-reporting */
    if (! self->rdk_conf) {
        return set_pykafka_error("RdKafkaException",
                                 "Please run configure() before starting.");
    }
    rd_kafka_conf_set_dr_msg_cb(self->rdk_conf,
                                Producer_delivery_report_callback);
    self->pending_futures = PyDict_New();
    if (! self->pending_futures) return NULL;
    rd_kafka_conf_set_opaque(self->rdk_conf, self->pending_futures);

    return RdkHandle_start(
            self,
            RD_KAFKA_PRODUCER,
            PyBytes_AS_STRING(brokers),
            PyBytes_AS_STRING(topic_name));
}


static PyObject *
Producer_produce(RdkHandle *self, PyObject *args)
{
    char *message = NULL;
    Py_ssize_t message_len = 0;
    char *partition_key= NULL;
    Py_ssize_t partition_key_len = 0;
    int partition_id = -1;
    if (! PyArg_ParseTuple(args,
                           "z#z#i",
                           &message, &message_len,
                           &partition_key, &partition_key_len,
                           &partition_id)) {
        return NULL;
    }

    /* Add a new Future and keep it alive until the delivery-callback runs;
     * the keep-alive is saved in a dict indexed by `pending_futures_uid`, and
     * it's the latter that we pass as a librdkafka msg_opaque value.  We'd
     * be fine just passing refs to `future` instead, if it weren't for pypy,
     * which may opt to invalidate the pointer in the mean time */
    PyObject *future = PyObject_CallObject(Future, NULL);
    if (! future) goto failed;
    PyObject *key = PyLong_FromSize_t(self->pending_futures_uid++);
    if (! key) goto failed;
    if (-1 == PyDict_SetItem(self->pending_futures, key, future)) goto failed;

    int res = 0;
    Py_BEGIN_ALLOW_THREADS
        res = rd_kafka_produce(self->rdk_topic_handle,
                               partition_id,
                               RD_KAFKA_MSG_F_COPY,
                               message, message_len,
                               partition_key, partition_key_len,
                               (void *)(self->pending_futures_uid - 1));
    Py_END_ALLOW_THREADS
    if (res == -1) {
        rd_kafka_resp_err_t err = rd_kafka_errno2err(errno);
        if (err == RD_KAFKA_RESP_ERR__QUEUE_FULL) {
            /* Remove `future` as we're not going to return it */
            if (-1 == PyDict_DelItem(self->pending_futures, key)) goto failed;
            set_pykafka_error("ProducerQueueFullError", "");
            goto failed;
        } else {
            /* Any other errors should be returned through `future`, because
             * that's where pykafka.Producer would put them */
            if (-1 == Producer_delivery_report_set_exception(future, err)) {
                set_pykafka_error("RdKafkaException", "see log for details");
                goto failed;
            }
        }
    }

    Py_DECREF(key);
    return future;

failed:
    Py_XDECREF(future);
    Py_XDECREF(key);
    return NULL;
}


static PyMethodDef Producer_methods[] = {
    {"produce", (PyCFunction)Producer_produce,
        METH_VARARGS, "Produce to kafka."},
    {"stop", (PyCFunction)RdkHandle_stop, METH_NOARGS, "Destroy producer."},
    {"configure", (PyCFunction)RdkHandle_configure,
        METH_VARARGS | METH_KEYWORDS, RdkHandle_configure__doc__},
    {"start", (PyCFunction)Producer_start, METH_VARARGS | METH_KEYWORDS, NULL},
    {"outq_len", (PyCFunction)RdkHandle_outq_len, METH_NOARGS, NULL},
    {"poll", (PyCFunction)RdkHandle_poll, METH_VARARGS | METH_KEYWORDS, NULL},
    {NULL, NULL, 0, NULL}
};


static PyTypeObject ProducerType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "pykafka.rd_kafka.Producer",
    sizeof(RdkHandle),
    0,                             /* tp_itemsize */
    (destructor)Producer_dealloc,  /* tp_dealloc */
    0,                             /* tp_print */
    0,                             /* tp_getattr */
    0,                             /* tp_setattr */
    0,                             /* tp_compare */
    0,                             /* tp_repr */
    0,                             /* tp_as_number */
    0,                             /* tp_as_sequence */
    0,                             /* tp_as_mapping */
    0,                             /* tp_hash */
    0,                             /* tp_call */
    0,                             /* tp_str */
    0,                             /* tp_getattro */
    0,                             /* tp_setattro */
    0,                             /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT,            /* tp_flags */
    0,                             /* tp_doc */
    0,                             /* tp_traverse */
    0,                             /* tp_clear */
    0,                             /* tp_richcompare */
    0,                             /* tp_weaklistoffset */
    0,                             /* tp_iter */
    0,                             /* tp_iternext */
    Producer_methods,              /* tp_methods */
    RdkHandle_members,             /* tp_members */
    0,                             /* tp_getset */
    0,                             /* tp_base */
    0,                             /* tp_dict */
    0,                             /* tp_descr_get */
    0,                             /* tp_descr_set */
    0,                             /* tp_dictoffset */
    0,                             /* tp_init */
};


/**
 * Consumer type
 */


static PyObject *
Consumer_stop(RdkHandle *self)
{
    /* Call stop on all partitions, then destroy all handles */

    PyObject *retval = Py_None;
    if (self->rdk_topic_handle && self->partition_ids) {
        Py_ssize_t i, len = PyList_Size(self->partition_ids);
        for (i = 0; i != len; ++i) {
            /* Error handling here is a bit poor; we cannot bail out directly
               if we want to clean up as much as we can. */
            long part_id = PyLong_AsLong(
                    PyList_GetItem(self->partition_ids, i));
            if (part_id == -1) {
                retval = NULL;
                PyObject *log_res = PyObject_CallMethod(
                        logger, "exception", "s", "In Consumer_stop:");
                Py_XDECREF(log_res);
                continue;
            }
            int res;
            Py_BEGIN_ALLOW_THREADS  /* avoid callbacks deadlocking */
                res = rd_kafka_consume_stop(self->rdk_topic_handle, part_id);
            Py_END_ALLOW_THREADS
            if (res == -1) {
                set_pykafka_error_from_code(rd_kafka_errno2err(errno), NULL);
                retval = NULL;
                PyObject *log_res = PyObject_CallMethod(
                        logger, "exception", "sl",
                        "Error in rd_kafka_consume_stop, part_id=%s",
                        part_id);
                Py_XDECREF(log_res);
                continue;
            }
        }
    }
    PyObject *res = RdkHandle_stop(self);
    Py_XDECREF(res);  /* res should always be Py_None, the X is a formality */

    Py_XINCREF(retval);
    return retval;
}


static void
Consumer_dealloc(PyObject *self)
{
    RdkHandle_dealloc(self, Consumer_stop);
}


static PyObject *
Consumer_start_fail(RdkHandle *self)
{
    return RdkHandle_start_fail(self, Consumer_stop);
}


static PyObject *
Consumer_start(RdkHandle *self, PyObject *args, PyObject *kwds)
{
    char *keywords[] = {
        "brokers",
        "topic_name",
        "partition_ids",
        "start_offsets",  /* same order as partition_ids */
        NULL};
    PyObject *brokers = NULL;
    PyObject *topic_name = NULL;
    PyObject *partition_ids = NULL;
    PyObject *start_offsets = NULL;
    if (! PyArg_ParseTupleAndKeywords(args,
                                      kwds,
                                      "SSOO",
                                      keywords,
                                      &brokers,
                                      &topic_name,
                                      &partition_ids,
                                      &start_offsets)) {
        return NULL;
    }

    /* Basic setup */
    PyObject *res = RdkHandle_start(
            self,
            RD_KAFKA_CONSUMER,
            PyBytes_AS_STRING(brokers),
            PyBytes_AS_STRING(topic_name));
    if (! res) return NULL;
    else Py_DECREF(res);

    /* We'll keep our own copy of partition_ids, because the one handed to us
       might be mutable, and weird things could happen if the list used on init
       is different than that on dealloc */
    if (self->partition_ids) {
        set_pykafka_error("RdKafkaException", "Already started.");
        return Consumer_start_fail(self);
    }
    self->partition_ids = PySequence_List(partition_ids);
    if (! self->partition_ids) return Consumer_start_fail(self);

    /* Start a queue and add all partition_ids to it */
    Py_BEGIN_ALLOW_THREADS  /* avoid callbacks deadlocking */
        self->rdk_queue_handle = rd_kafka_queue_new(self->rdk_handle);
    Py_END_ALLOW_THREADS
    if (! self->rdk_queue_handle) {
        set_pykafka_error("RdKafkaException", "could not get queue");
        return Consumer_start_fail(self);
    }
    Py_ssize_t i, len = PyList_Size(self->partition_ids);
    for (i = 0; i != len; ++i) {
        /* We don't do much type-checking on partition_ids/start_offsets as
           this module is intended solely for use with the python class that
           wraps it */
        int32_t part_id = PyLong_AsLong(
                PyList_GetItem(self->partition_ids, i));
        if (part_id == -1 && PyErr_Occurred()) {
            return Consumer_start_fail(self);
        }
        PyObject *offset_obj = PySequence_GetItem(start_offsets, i);
        if (! offset_obj) {  /* start_offsets shorter than partition_ids? */
            return Consumer_start_fail(self);
        }
        int64_t offset = PyLong_AsLongLong(offset_obj);
        int res;
        Py_BEGIN_ALLOW_THREADS  /* avoid callbacks deadlocking */
            res = rd_kafka_consume_start_queue(self->rdk_topic_handle,
                                               part_id,
                                               offset,
                                               self->rdk_queue_handle);
        Py_END_ALLOW_THREADS
        if (res == -1) {
            set_pykafka_error_from_code(rd_kafka_errno2err(errno), NULL);
            return Consumer_start_fail(self);
        }
    }
    Py_INCREF(Py_None);
    return Py_None;
}


static PyObject *
Consumer_consume(RdkHandle *self, PyObject *args)
{
    int timeout_ms = 0;
    if (! PyArg_ParseTuple(args, "i", &timeout_ms)) return NULL;
    if (! self->rdk_queue_handle) {
        return set_pykafka_error("ConsumerStoppedException", "");
    }

    rd_kafka_message_t *rkmessage;

    Py_BEGIN_ALLOW_THREADS  /* avoid callbacks deadlocking */
        rkmessage = rd_kafka_consume_queue(self->rdk_queue_handle, timeout_ms);
    Py_END_ALLOW_THREADS

    if (!rkmessage) {
        /* Either ETIMEDOUT or ENOENT occurred, but the latter would imply we
           forgot to call rd_kafka_consume_start_queue, which is unlikely in
           this setup.  We'll assume it was ETIMEDOUT then: */
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
        /* Whenever we get to the head of a partition, we get this.  There may
           be messages available in other partitions, so if we want to match
           pykafka.SimpleConsumer behaviour, we ought to avoid breaking any
           iteration loops, and simply skip over this one altogether: */
        retval = Consumer_consume(self, args);
    } else {
        set_pykafka_error_from_code(rkmessage->err, NULL);
    }

    Py_BEGIN_ALLOW_THREADS  /* avoid callbacks deadlocking */
        rd_kafka_message_destroy(rkmessage);
    Py_END_ALLOW_THREADS
    return retval;
}


static PyMethodDef Consumer_methods[] = {
    {"consume", (PyCFunction)Consumer_consume,
        METH_VARARGS, "Consume from kafka."},
    {"stop", (PyCFunction)Consumer_stop, METH_NOARGS, "Destroy consumer."},
    {"configure", (PyCFunction)RdkHandle_configure,
        METH_VARARGS | METH_KEYWORDS, RdkHandle_configure__doc__},
    {"start", (PyCFunction)Consumer_start, METH_VARARGS | METH_KEYWORDS, NULL},
    {"poll", (PyCFunction)RdkHandle_poll, METH_VARARGS | METH_KEYWORDS, NULL},
    {NULL, NULL, 0, NULL}
};


static PyTypeObject ConsumerType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "pykafka.rd_kafka.Consumer",
    sizeof(RdkHandle),
    0,                             /* tp_itemsize */
    (destructor)Consumer_dealloc,  /* tp_dealloc */
    0,                             /* tp_print */
    0,                             /* tp_getattr */
    0,                             /* tp_setattr */
    0,                             /* tp_compare */
    0,                             /* tp_repr */
    0,                             /* tp_as_number */
    0,                             /* tp_as_sequence */
    0,                             /* tp_as_mapping */
    0,                             /* tp_hash */
    0,                             /* tp_call */
    0,                             /* tp_str */
    0,                             /* tp_getattro */
    0,                             /* tp_setattro */
    0,                             /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT,            /* tp_flags */
    0,                             /* tp_doc */
    0,                             /* tp_traverse */
    0,                             /* tp_clear */
    0,                             /* tp_richcompare */
    0,                             /* tp_weaklistoffset */
    0,                             /* tp_iter */
    0,                             /* tp_iternext */
    Consumer_methods,              /* tp_methods */
    RdkHandle_members,             /* tp_members */
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
debug_thread_cnt(PyObject *self, PyObject *args)
{
    return PyLong_FromLong(rd_kafka_thread_cnt());
}


static PyObject *
debug_wait_destroyed(PyObject *self, PyObject *arg)
{
    int timeout_ms = PyLong_AsLong(arg);
    if (timeout_ms == -1 && PyErr_Occurred()) return NULL;

    int res;
    Py_BEGIN_ALLOW_THREADS  /* avoid callbacks deadlocking */
        res = rd_kafka_wait_destroyed(timeout_ms);
    Py_END_ALLOW_THREADS

    if (res == -1) {
        return set_pykafka_error("RdKafkaException",
                                 "rd_kafka_wait_destroyed timed out");
    }
    Py_INCREF(Py_None);
    return Py_None;
}


/**
 * Module init
 */

static const char module_name[] = "pykafka.rdkafka._rd_kafka";


static PyMethodDef pyrdk_methods[] = {
    {"_thread_cnt", debug_thread_cnt, METH_NOARGS, NULL},
    {"_wait_destroyed", debug_wait_destroyed, METH_O, NULL},
    {NULL, NULL, 0, NULL}
};


#if PY_MAJOR_VERSION >= 3
    static struct PyModuleDef moduledef = {
        PyModuleDef_HEAD_INIT,
        module_name,
        NULL,  /* m_doc */
        -1,    /* m_size */
        pyrdk_methods,
        NULL,  /* m_reload */
        NULL,  /* m_traverse */
        NULL,  /* m_clear */
        NULL,  /* m_free */
    };
#endif


static PyObject *
_rd_kafkamodule_init(void)
{
#if PY_MAJOR_VERSION >= 3
    PyObject *mod = PyModule_Create(&moduledef);
#else
    PyObject *mod = Py_InitModule(module_name, pyrdk_methods);
#endif
    if (mod == NULL) return NULL;

    /* Callback logging requires the GIL */
    PyEval_InitThreads();

    PyObject *logging = PyImport_ImportModule("logging");
    if (! logging) return NULL;
    logger = PyObject_CallMethod(logging, "getLogger", "s", module_name);
    Py_DECREF(logging);
    if (! logger) return NULL;

    /* from concurrent.futures import Future */
    PyObject *futures = PyImport_ImportModule("concurrent.futures");
    if (! futures) return NULL;
    Future = PyObject_GetAttrString(futures, "Future");
    Py_DECREF(futures);
    if (! Future) return NULL;

    pykafka_exceptions = PyImport_ImportModule("pykafka.exceptions");
    if (! pykafka_exceptions) return NULL;

    if (MessageType.tp_name == NULL) {
        PyStructSequence_InitType(&MessageType, &Message_desc);
    }
    Py_INCREF(&MessageType);
    if (PyModule_AddObject(mod, "Message", (PyObject *)&MessageType)) {
        return NULL;
    }

    ProducerType.tp_new = PyType_GenericNew;
    if (PyType_Ready(&ProducerType)) return NULL;
    Py_INCREF(&ProducerType);
    if (PyModule_AddObject(mod, "Producer", (PyObject *)&ProducerType)) {
        return NULL;
    }

    ConsumerType.tp_new = PyType_GenericNew;
    if (PyType_Ready(&ConsumerType)) return NULL;
    Py_INCREF(&ConsumerType);
    if (PyModule_AddObject(mod, "Consumer", (PyObject *)&ConsumerType)) {
        return NULL;
    }

    return mod;
}


#if PY_MAJOR_VERSION >= 3
    PyMODINIT_FUNC
    PyInit__rd_kafka(void)
    {
        return _rd_kafkamodule_init();
    }
#else
    PyMODINIT_FUNC
    init_rd_kafka(void)
    {
        _rd_kafkamodule_init();
    }
#endif
