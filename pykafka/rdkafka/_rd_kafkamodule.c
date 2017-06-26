#define PY_SSIZE_T_CLEAN

#include <Python.h>
#include <structmember.h>
#include <structseq.h>

#include <errno.h>
#include <pthread.h>
#include <syslog.h>

#include <librdkafka/rdkafka.h>


static PyObject *pykafka_exceptions;  /* ~ import pykafka.exceptions */
static PyObject *Message;  /* ~ from pykafka.protocol import Message */

static PyObject *logger;  /* ~ logging.getLogger */


/**
 * Logging
 */


/* If used as librdkafka log_cb, passes log messages into python logging */
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

    /* Grab the GIL, as rdkafka callbacks may come from non-python threads */
    PyGILState_STATE gstate = PyGILState_Ensure();

    /* NB librdkafka docs say that rk may be NULL, so check that */
    /* NB2 because we hold the GIL we don't need the handle's rwlock */
    const char *rk_name = rk ? rd_kafka_name(rk) : "rk_handle null";
    const char *format = "%s [%s] %s";  /* format rk_name + fac + buf */

    PyObject *res = PyObject_CallMethod(
            logger, lvl, "ssss", format, rk_name, fac, buf);
    /* Any errors here we'll just have to swallow: we're probably on some
       background thread, and we can't log either (logging just failed!) */
    if (! res) PyErr_Clear();
    else Py_DECREF(res);

    PyGILState_Release(gstate);
}


/**
 * Exception helpers
 */


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


/* Given an error code, return most suitable class from pykafka.exceptions */
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
 * Shared bits of Producer and Consumer types
 */


/* Note that with this RdkHandle, we hold a separate rd_kafka_t handle for each
 * rd_kafka_topic_t, whereas librdkafka would allow sharing the same rd_kafka_t
 * handle between many topic handles, which would be far more efficient.  The
 * problem with that is that it would require the same rd_kafka_conf_t settings
 * across all class instances sharing a handle, which is somewhat incompatible
 * with the current pykafka API.
 *
 * We need a pthread rwlock here, because in many methods we release the GIL
 * (this for various reasons - one key reason is that it prevents us
 * deadlocking if we do blocking calls into librdkafka and then get callbacks
 * out of librdkafka, and the callbacks would try to grab the GIL).  Once we
 * release the GIL however, there may be other threads calling RdkHandle_stop
 * (which in pykafka can happen on any thread).  The rule here then is that
 * RdkHandle_stop needs to take out an exclusive lock (wrlock), whereas most
 * other calls are safe when taking out a shared lock (rdlock). */
typedef struct {
    PyObject_HEAD
    pthread_rwlock_t rwlock;
    rd_kafka_t *rdk_handle;
    rd_kafka_conf_t *rdk_conf;
    rd_kafka_topic_t *rdk_topic_handle;
    rd_kafka_topic_conf_t *rdk_topic_conf;

    /* Consumer-specific fields */
    rd_kafka_queue_t *rdk_queue_handle;
    PyObject *partition_ids;
} RdkHandle;


/* Only for inspection; if you'd manipulate these from outside the module, we
 * have no error checking in place to protect from ensuing mayhem */
static PyMemberDef RdkHandle_members[] = {
    {"_partition_ids", T_OBJECT_EX, offsetof(RdkHandle, partition_ids),
                       READONLY, "Partitions fetched from by this consumer"},
    {NULL}
};


static PyObject *
RdkHandle_new(PyTypeObject *type, PyObject *args, PyObject *kwds)
{
    PyObject *self = PyType_GenericNew(type, args, kwds);
    if (self) {
        int res = pthread_rwlock_init(&((RdkHandle *)self)->rwlock, NULL);
        if (res) {
            Py_DECREF(self);
            return set_pykafka_error("RdKafkaException", "Failed rwlock init");
        }
    }
    return self;
}


/* Release RdkHandle.rwlock (see RdkHandle docstring) */
static int
RdkHandle_unlock(RdkHandle *self)
{
    if (pthread_rwlock_unlock(&self->rwlock)) {
        set_pykafka_error("RdKafkaException", "Failed to release rwlock");
        return -1;
    }
    return 0;
}


/* Get shared lock and optionally check handle is running.  Returns non-zero
 * if error has been set.
 *
 * Should be used by any method that accesses an RdkHandle, to prevent the
 * handle being concurrently destroyed on another thread (by calling
 * RdkHandle_stop).  See also RdkHandle docstring.  */
static int
RdkHandle_safe_lock(RdkHandle *self, int check_running)
{
    int res;
    Py_BEGIN_ALLOW_THREADS
        res = pthread_rwlock_rdlock(&self->rwlock);
    Py_END_ALLOW_THREADS
    if (res) {
        set_pykafka_error("RdKafkaException", "Failed to get shared lock");
        return -1;
    }
    if (check_running && !self->rdk_handle) {
        set_pykafka_error("RdKafkaStoppedException", "");
        RdkHandle_unlock(self);
        return -1;
    }
    return 0;
}


/* Get exclusive lock on handle.  Returns non-zero if error has been set.
 *
 * Should be used by any method that might render accessing RdkHandle members
 * unsafe (in particular, RdkHandle_stop).  See also RdkHandle docstring.  */
static int
RdkHandle_excl_lock(RdkHandle *self)
{
    int res;
    Py_BEGIN_ALLOW_THREADS
        res = pthread_rwlock_wrlock(&self->rwlock);
    Py_END_ALLOW_THREADS
    if (res) {
        set_pykafka_error("RdKafkaException", "Failed to get exclusive lock");
        return -1;
    }
    return 0;
}


PyDoc_STRVAR(RdkHandle_outq_len__doc__,
    "outq_len(self) -> int\n"
    "\n"
    "Number of messages pending shipping to a broker.");
static PyObject *
RdkHandle_outq_len(RdkHandle *self)
{
    if (RdkHandle_safe_lock(self, /* check_running= */ 1)) return NULL;

    int outq_len = -1;
    Py_BEGIN_ALLOW_THREADS  /* avoid callbacks deadlocking */
        outq_len = rd_kafka_outq_len(self->rdk_handle);
    Py_END_ALLOW_THREADS

    if (RdkHandle_unlock(self)) return NULL;
    return Py_BuildValue("i", outq_len);
}


PyDoc_STRVAR(RdkHandle_poll__doc__,
    "poll(self, timeout_ms) -> int\n"
    "\n"
    "Poll the handle for events (in particular delivery callbacks) and\n"
    "return the total number of callbacks triggered.");
static PyObject *
RdkHandle_poll(RdkHandle *self, PyObject *args, PyObject *kwds)
{
    char *keywords[] = {"timeout_ms", NULL};
    int timeout_ms = 0;
    if (! PyArg_ParseTupleAndKeywords(args, kwds, "i", keywords, &timeout_ms)) {
            return NULL;
    }

    if (RdkHandle_safe_lock(self, /* check_running= */ 1)) return NULL;

    int n_events = 0;
    Py_BEGIN_ALLOW_THREADS  /* avoid callbacks deadlocking */
        n_events = rd_kafka_poll(self->rdk_handle, timeout_ms);
    Py_END_ALLOW_THREADS

    if (RdkHandle_unlock(self)) return NULL;
    return Py_BuildValue("i", n_events);
}


PyDoc_STRVAR(RdkHandle_stop__doc__,
    "stop(self)\n"
    "\n"
    "Shut down the librdkafka handle and clean up.  This may block until\n"
    "other methods accessing the same handle concurrently have finished.\n");
static PyObject *
RdkHandle_stop(RdkHandle *self)
{
    /* We'll only ever get a locking error if we programmed ourselves into a
     * deadlock.  We'd have to admit defeat, abort, and leak this RdkHandle */
    if (RdkHandle_excl_lock(self)) return NULL;

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
            PyObject *opaque = (PyObject *)rd_kafka_opaque(self->rdk_handle);
            Py_XDECREF(opaque);
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

    if (RdkHandle_unlock(self)) return NULL;
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
    pthread_rwlock_destroy(&((RdkHandle *)self)->rwlock);
    self->ob_type->tp_free(self);
}


PyDoc_STRVAR(RdkHandle_configure__doc__,
    "configure(self, conf) OR configure(self, topic_conf)\n"
    "\n"
    "Set up and populate the rd_kafka_(topic_)conf_t.  Somewhat inelegantly\n"
    "(for the benefit of code reuse, whilst avoiding some harrowing partial\n"
    "binding for C functions) this requires that you call it twice, once\n"
    "with a `conf` list only, and again with `topic_conf` only.\n"
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

    if (RdkHandle_safe_lock(self, /* check_running= */ 0)) return NULL;
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
        if (! self->rdk_conf) {
            self->rdk_conf = rd_kafka_conf_new();
            rd_kafka_conf_set_log_cb(self->rdk_conf, logging_callback);
        }
        if (! self->rdk_topic_conf) {
            self->rdk_topic_conf = rd_kafka_topic_conf_new();
        }
    Py_END_ALLOW_THREADS

    PyObject *retval = Py_None;
    PyObject *conf_or_topic_conf = topic_conf ? topic_conf : conf;
    Py_ssize_t i, len = PyList_Size(conf_or_topic_conf);
    for (i = 0; i != len; ++i) {
        PyObject *conf_pair = PyList_GetItem(conf_or_topic_conf, i);
        const char *name = NULL;
        const char *value =  NULL;
        if (! PyArg_ParseTuple(conf_pair, "ss", &name, &value)) {
            retval = NULL;
            break;
        }
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
            retval = set_pykafka_error("RdKafkaException", errstr);
            break;
        }
    }

    if (RdkHandle_unlock(self)) return NULL;
    Py_XINCREF(retval);
    return retval;
}


/* Cleanup helper for *_start(), returns NULL to allow shorthand in use.
 * NB: assumes self->rwlock is held and releases it. */
static PyObject *
RdkHandle_start_fail(RdkHandle *self, PyObject *(*stop_func) (RdkHandle *))
{
    /* Something went wrong so we expect an exception has been set */
    PyObject *err_type, *err_value, *err_traceback;
    PyErr_Fetch(&err_type, &err_value, &err_traceback);

    RdkHandle_unlock(self);
    PyObject *stop_result = stop_func(self);

    /* stop_func is likely to raise exceptions, as start was incomplete */
    if (! stop_result) PyErr_Clear();
    else Py_DECREF(stop_result);

    PyErr_Restore(err_type, err_value, err_traceback);
    return NULL;
}


/* Shared logic of Consumer_start and Producer_start */
static PyObject *
RdkHandle_start(RdkHandle *self,
                rd_kafka_type_t rdk_type,
                const char *brokers,
                const char *topic_name)
{
    if (RdkHandle_excl_lock(self)) return NULL;
    if (self->rdk_handle) {
        set_pykafka_error("RdKafkaException", "Already started!");
        return RdkHandle_start_fail(self, RdkHandle_stop);
    }

    /* Configure and start rdk_handle */
    char errstr[512];
    Py_BEGIN_ALLOW_THREADS  /* avoid callbacks deadlocking */
        self->rdk_handle = rd_kafka_new(
                rdk_type, self->rdk_conf, errstr, sizeof(errstr));
        self->rdk_conf = NULL;  /* deallocated by rd_kafka_new() */
    Py_END_ALLOW_THREADS
    if (! self->rdk_handle) {
        set_pykafka_error("RdKafkaException", errstr);
        return RdkHandle_start_fail(self, RdkHandle_stop);
    }

    /* Set brokers */
    int brokers_added;
    Py_BEGIN_ALLOW_THREADS  /* avoid callbacks deadlocking */
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

    if (RdkHandle_unlock(self)) return NULL;
    Py_INCREF(Py_None);
    return Py_None;
}


/**
 * Producer type
 */


/* NB this doesn't check if RdkHandle_outq_len is zero, and generally assumes
 * the wrapping python class will take care of ensuring any such preconditions
 * for a clean termination */
static void
Producer_dealloc(PyObject *self)
{
    RdkHandle_dealloc(self, RdkHandle_stop);
}


/* Helper function for Producer_delivery_report_callback: find an exception
 * corresponding to `err`, and send that into the report queue.  Returns -1
 * on failure, or 0 on success */
static int
Producer_delivery_report_put(PyObject *put_func,
                             PyObject *message,
                             rd_kafka_resp_err_t err)
{
    PyObject *exc = NULL;
    if (err == RD_KAFKA_RESP_ERR_NO_ERROR) {
        exc = Py_None;
        Py_INCREF(Py_None);
    } else {
        set_pykafka_error_from_code(err, &exc);
        if (! exc) return -1;
    }
    PyObject *res = PyObject_CallFunctionObjArgs(put_func, message, exc, NULL);

    Py_DECREF(exc);
    if (! res) return -1;
    else Py_DECREF(res);
    return 0;
}


/* Callback to be used as librdkafka dr_msg_cb.  Note that this must always
 * be configured to run, even if we do not care about delivery reporting,
 * because we keep the Message objects that we produce from alive until this
 * callback releases them, in order to avoid having to copy the payload  */
static void
Producer_delivery_report_callback(rd_kafka_t *rk,
                                  const rd_kafka_message_t *rkmessage,
                                  void *opaque)
{
    PyGILState_STATE gstate = PyGILState_Ensure();

    /* Producer_produce sent *Message as msg_opaque == rkmessage->_private */
    PyObject *message = (PyObject *)rkmessage->_private;
    PyObject *put_func = (PyObject *)opaque;
    if (rkmessage->offset != -1) {
        PyObject* offset = PyLong_FromUnsignedLongLong(rkmessage->offset);
        PyObject_SetAttrString(message, "offset", offset);
    }
    if (-1 == Producer_delivery_report_put(put_func, message, rkmessage->err)) {
        /* Must swallow exception as this is a non-python callback */
        PyObject *res = PyObject_CallMethod(
            logger, "exception", "s", "Failure in delivery callback");
        Py_XDECREF(res);
        PyErr_Clear();
    } else {
        // Set timestamp to the one created by librdkafka
        rd_kafka_timestamp_type_t timestamp_type;
        int64_t timestamp = rd_kafka_message_timestamp(rkmessage, &timestamp_type);
        if (timestamp_type != RD_KAFKA_TIMESTAMP_NOT_AVAILABLE) {
            PyObject* timestamp_o = PyLong_FromUnsignedLongLong(timestamp);
            PyObject_SetAttrString(message, "timestamp", timestamp_o);
        }
    }

    Py_DECREF(message);  /* We INCREF'd this in Producer_produce() */
    PyGILState_Release(gstate);
}


PyDoc_STRVAR(Producer_start__doc__,
    "start(self, brokers, topic_name, delivery_put)\n"
    "\n"
    "Starts the underlying rdkafka producer, given a broker connection\n"
    "string, a topic name, and a (bound) method that we can send delivery\n"
    "reports (as in `put(msg, exception)`).  Configuration should have been\n"
    "provided through earlier calls to configure().  Note that following\n"
    "start, you _must_ ensure that poll() is called regularly, or delivery\n"
    "reports may pile up\n");
static PyObject *
Producer_start(RdkHandle *self, PyObject *args, PyObject *kwds)
{
    if (RdkHandle_excl_lock(self)) return NULL;

    char *keywords[] = {"brokers", "topic_name", "delivery_put", NULL};
    PyObject *brokers = NULL;
    PyObject *topic_name = NULL;
    PyObject *delivery_put = NULL;
    if (! PyArg_ParseTupleAndKeywords(
        args, kwds, "SSO", keywords, &brokers, &topic_name, &delivery_put)) {
        goto failed;
    }

    /* Configure delivery-reporting */
    if (! self->rdk_conf) {
        set_pykafka_error("RdKafkaException",
                          "Please run configure() before starting.");
        goto failed;
    }
    rd_kafka_conf_set_dr_msg_cb(self->rdk_conf,
                                Producer_delivery_report_callback);
    Py_INCREF(delivery_put);
    rd_kafka_conf_set_opaque(self->rdk_conf, delivery_put);

    if (RdkHandle_unlock(self)) return NULL;
    return RdkHandle_start(
            self,
            RD_KAFKA_PRODUCER,
            PyBytes_AS_STRING(brokers),
            PyBytes_AS_STRING(topic_name));
failed:
    RdkHandle_unlock(self);
    return NULL;
}


PyDoc_STRVAR(Producer_produce__doc__,
    "produce(self, message)\n"
    "\n"
    "Produces a `pykafka.protocol.Message` through librdkafka.  Will keep\n"
    "a reference to the message internally until delivery is confirmed\n");
static PyObject *
Producer_produce(RdkHandle *self, PyObject *message)
{
    if (RdkHandle_safe_lock(self, /* check_running= */ 1)) return NULL;

    PyObject *value = NULL;
    PyObject *partition_key = NULL;
    PyObject *partition_id = NULL;

    /* Keep message alive until the delivery-callback runs.  Needed both
     * because we may want to put the message on a report queue when the
     * callback runs, and because we'll tell rd_kafka_produce() not to copy
     * the payload and it can safely use the raw Message bytes directly */
    Py_INCREF(message);

    /* Get pointers to raw Message contents */
    value = PyObject_GetAttrString(message, "value");
    if (! value) goto failed;
    partition_key = PyObject_GetAttrString(message, "partition_key");
    if (! partition_key) goto failed;
    partition_id = PyObject_GetAttrString(message, "partition_id");
    if (! partition_id) goto failed;

    char *v = NULL;
    Py_ssize_t v_len = 0;
    if (value != Py_None) {
        v = PyBytes_AsString(value);
        if (! v) goto failed;
        v_len = PyBytes_GET_SIZE(value);
    }
    char *pk = NULL;
    Py_ssize_t pk_len = 0;
    if (partition_key != Py_None) {
        pk = PyBytes_AsString(partition_key);
        if (! pk) goto failed;
        pk_len = PyBytes_GET_SIZE(partition_key);
    }
    int32_t p_id = PyLong_AsLong(partition_id);
    if (p_id == -1 && PyErr_Occurred()) goto failed;

    int res = 0;
    Py_BEGIN_ALLOW_THREADS
        res = rd_kafka_produce(self->rdk_topic_handle,
                               p_id,
                               0,  /* ie don't copy and don't dealloc v */
                               v, v_len,
                               pk, pk_len,
                               (void *)message);
    Py_END_ALLOW_THREADS
    if (res == -1) {
        rd_kafka_resp_err_t err = rd_kafka_errno2err(errno);
        if (err == RD_KAFKA_RESP_ERR__QUEUE_FULL) {
            set_pykafka_error("ProducerQueueFullError", "");
            goto failed;
        } else {
            /* Any other errors should go through the report queue,
             * because that's where pykafka.Producer would put them */
            PyObject *put_func = (PyObject *)rd_kafka_opaque(self->rdk_handle);
            if (-1 == Producer_delivery_report_put(put_func, message, err)) {
                goto failed;
            }
        }
        Py_DECREF(message);  /* There won't be a delivery-callback */
    }

    Py_DECREF(value);
    Py_DECREF(partition_key);
    Py_DECREF(partition_id);
    if (RdkHandle_unlock(self)) return NULL;

    Py_INCREF(Py_None);
    return Py_None;
failed:
    Py_XDECREF(value);
    Py_XDECREF(partition_key);
    Py_XDECREF(partition_id);
    RdkHandle_unlock(self);
    return NULL;
}


static PyMethodDef Producer_methods[] = {
    {"produce", (PyCFunction)Producer_produce,
        METH_O, Producer_produce__doc__},
    {"stop", (PyCFunction)RdkHandle_stop,
        METH_NOARGS, RdkHandle_stop__doc__},
    {"configure", (PyCFunction)RdkHandle_configure,
        METH_VARARGS | METH_KEYWORDS, RdkHandle_configure__doc__},
    {"start", (PyCFunction)Producer_start,
        METH_VARARGS | METH_KEYWORDS, Producer_start__doc__},
    {"outq_len", (PyCFunction)RdkHandle_outq_len,
        METH_NOARGS, RdkHandle_outq_len__doc__},
    {"poll", (PyCFunction)RdkHandle_poll,
        METH_VARARGS | METH_KEYWORDS, RdkHandle_poll__doc__},
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
    0,                             /* tp_alloc */
    RdkHandle_new,                 /* tp_new */
};


/**
 * Consumer type
 */


/* Destroy all internal state of the consumer */
static PyObject *
Consumer_stop(RdkHandle *self)
{
    if (RdkHandle_safe_lock(self, /* check_running= */ 0)) return NULL;

    int errored = 0;
    if (self->rdk_topic_handle && self->partition_ids) {
        Py_ssize_t i, len = PyList_Size(self->partition_ids);
        for (i = 0; i != len; ++i) {
            /* Error handling here is a bit poor; we cannot bail out directly
               if we want to clean up as much as we can. */
            long part_id = PyLong_AsLong(
                    PyList_GetItem(self->partition_ids, i));
            if (part_id == -1) {
                errored += 1;
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
                errored += 1;
                PyObject *log_res = PyObject_CallMethod(
                        logger, "exception", "sl",
                        "Error in rd_kafka_consume_stop, part_id=%s",
                        part_id);
                Py_XDECREF(log_res);
                continue;
            }
        }
    }

    RdkHandle_unlock(self);
    PyObject *res = RdkHandle_stop(self);
    if (errored) {
        Py_XDECREF(res);
        return NULL;
    }
    return res;
}


static void
Consumer_dealloc(PyObject *self)
{
    RdkHandle_dealloc(self, Consumer_stop);
}


/* Cleanup helper for Consumer_start.
 * NB: assumes self->rwlock is held and releases it. */
static PyObject *
Consumer_start_fail(RdkHandle *self)
{
    return RdkHandle_start_fail(self, Consumer_stop);
}


PyDoc_STRVAR(Consumer_start__doc__,
    "start(self, brokers, topic_name, partition_ids, start_offsets)\n"
    "\n"
    "Starts the underlying rdkafka consumer.  Configuration should have been\n"
    "provided through earlier calls to configure().  Note that following\n"
    "start, you must ensure that poll() is called regularly, as required\n"
    "by librdkafka\n");
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

    /* Set the application opaque NULL, just to make stop() simpler */
    if (RdkHandle_excl_lock(self)) return NULL;
    if (! self->rdk_conf) {
        set_pykafka_error("RdKafkaException",
                          "Please run configure() before starting.");
        return NULL;
    }
    rd_kafka_conf_set_opaque(self->rdk_conf, NULL);
    if (RdkHandle_unlock(self)) return NULL;

    /* Basic setup */
    PyObject *res = RdkHandle_start(
            self,
            RD_KAFKA_CONSUMER,
            PyBytes_AS_STRING(brokers),
            PyBytes_AS_STRING(topic_name));
    if (! res) return NULL;
    else Py_DECREF(res);

    if (RdkHandle_excl_lock(self)) return NULL;
    if (! self->rdk_handle) {
        set_pykafka_error("RdKafkaStoppedException",
                          "Stopped in the middle of starting.");
        return Consumer_start_fail(self);
    }

    /* We'll keep our own copy of partition_ids, because the one handed to us
       might be mutable, and weird things could happen if the list used on init
       is different than that on dealloc */
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
    if (RdkHandle_unlock(self)) return NULL;
    Py_INCREF(Py_None);
    return Py_None;
}


PyDoc_STRVAR(Consumer_consume__doc__,
    "consume(self, timeout_ms) -> pykafka.protocol.Message or None \n"
    "\n"
    "Block until a message can be returned, or return None if timeout_ms has\n"
    "been reached.\n");
static PyObject *
Consumer_consume(RdkHandle *self, PyObject *args)
{
    int timeout_ms = 0;
    if (! PyArg_ParseTuple(args, "i", &timeout_ms)) return NULL;

    PyObject *retval = NULL;
    PyObject *empty_args = NULL;
    PyObject *kwargs = NULL;
    rd_kafka_message_t *rkmessage;
    int protocol_version = 0;

    if (RdkHandle_safe_lock(self, /* check_running= */ 1)) return NULL;
    Py_BEGIN_ALLOW_THREADS  /* avoid callbacks deadlocking */
        rkmessage = rd_kafka_consume_queue(self->rdk_queue_handle, timeout_ms);
    Py_END_ALLOW_THREADS
    if (RdkHandle_unlock(self)) goto cleanup;

    if (!rkmessage) {
        /* Either ETIMEDOUT or ENOENT occurred, but the latter would imply we
           forgot to call rd_kafka_consume_start_queue, which is unlikely in
           this setup.  We'll assume it was ETIMEDOUT then: */
        Py_INCREF(Py_None);
        return Py_None;
    }

    if (rkmessage->err == RD_KAFKA_RESP_ERR_NO_ERROR) {
        /* Build a pykafka.protocol.Message */
        rd_kafka_timestamp_type_t timestamp_type;
        int64_t timestamp = rd_kafka_message_timestamp(rkmessage, &timestamp_type);
        if (timestamp_type == RD_KAFKA_TIMESTAMP_NOT_AVAILABLE) {
            timestamp = 0;
        } else {
            // infer protocol from presence of timestamp. Fragile, but protocol
            // not transmitted by librdkafka
            protocol_version = 1;
        }
#if PY_MAJOR_VERSION >= 3
        const char *format = "{s:y#,s:y#,s:l,s:L,s:i,s:L}";
#else
        const char *format = "{s:s#,s:s#,s:l,s:L,s:i,s:L}";
#endif
        kwargs = Py_BuildValue(
            format,
            "value", rkmessage->payload, rkmessage->len,
            "partition_key", rkmessage->key, rkmessage->key_len,
            "partition_id", (long)rkmessage->partition,
            "offset", (PY_LONG_LONG)rkmessage->offset,
            "protocol_version", protocol_version,
            "timestamp", (PY_LONG_LONG)timestamp);
        if (! kwargs) goto cleanup;
        empty_args = PyTuple_New(0);
        if (! empty_args) goto cleanup;
        retval = PyObject_Call(Message, empty_args, kwargs);
    } else if (rkmessage->err == RD_KAFKA_RESP_ERR__PARTITION_EOF) {
        /* Whenever we get to the head of a partition, we get this.  There
         * may be messages available in other partitions, so if we want to
         * match pykafka.SimpleConsumer behaviour, we ought to avoid breaking
         * any iteration loops, and simply skip over this one altogether: */
        retval = Consumer_consume(self, args);
    } else {
        set_pykafka_error_from_code(rkmessage->err, NULL);
    }
cleanup:
    Py_BEGIN_ALLOW_THREADS  /* avoid callbacks deadlocking */
        rd_kafka_message_destroy(rkmessage);
    Py_END_ALLOW_THREADS
    Py_XDECREF(empty_args);
    Py_XDECREF(kwargs);
    return retval;
}


static PyMethodDef Consumer_methods[] = {
    {"consume", (PyCFunction)Consumer_consume,
        METH_VARARGS, Consumer_consume__doc__},
    {"stop", (PyCFunction)Consumer_stop,
        METH_NOARGS, RdkHandle_stop__doc__},
    {"configure", (PyCFunction)RdkHandle_configure,
        METH_VARARGS | METH_KEYWORDS, RdkHandle_configure__doc__},
    {"start", (PyCFunction)Consumer_start,
        METH_VARARGS | METH_KEYWORDS, Consumer_start__doc__},
    {"poll", (PyCFunction)RdkHandle_poll,
        METH_VARARGS | METH_KEYWORDS, RdkHandle_poll__doc__},
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
    0,                             /* tp_alloc */
    RdkHandle_new,                 /* tp_new */
};


/**
 * Debugging helpers
 */


PyDoc_STRVAR(debug_thread_cnt__doc__,
    "_thread_cnt() -> int\n"
    "\n"
    "Debugging helper, reports number of librdkafka threads running.\n");
static PyObject *
debug_thread_cnt(PyObject *self, PyObject *args)
{
    return PyLong_FromLong(rd_kafka_thread_cnt());
}


PyDoc_STRVAR(debug_wait_destroyed__doc__,
    "_wait_destroyed(timeout_ms)\n"
    "\n"
    "Debugging helper, blocks until all rdkafka handles have been destroyed.\n"
    "Raises exception if timeout_ms is reached before then\n");
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
    {"_thread_cnt", debug_thread_cnt,
        METH_NOARGS, debug_thread_cnt__doc__},
    {"_wait_destroyed", debug_wait_destroyed,
        METH_O, debug_wait_destroyed__doc__},
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

    pykafka_exceptions = PyImport_ImportModule("pykafka.exceptions");
    if (! pykafka_exceptions) return NULL;

    PyObject *pykafka_protocol = PyImport_ImportModule("pykafka.protocol");
    if (! pykafka_protocol) return NULL;
    Message = PyObject_GetAttrString(pykafka_protocol, "Message");
    Py_DECREF(pykafka_protocol);
    if (! Message) return NULL;

    if (PyType_Ready(&ProducerType)) return NULL;
    Py_INCREF(&ProducerType);
    if (PyModule_AddObject(mod, "Producer", (PyObject *)&ProducerType)) {
        return NULL;
    }

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
