"""Some basic helpers to get benchmark output into pandas

Example usage:

```
p_data = to_dataframe("your_pickled_output.pkl")
grouped = p_data[(p_data.use_rdkafka == False) | (p_data.queued_max_messages == 10**5)].groupby(("msg_size_bytes", "use_rdkafka", "num_partitions"))
grouped.max()["msgs_per_sec"]

grouped = p_data[p_data.msg_size_bytes == 100].groupby(("use_rdkafka", "num_partitions", "queued_max_messages"))
grouped = p_data[p_data.msg_size_bytes == 1000].groupby(("use_rdkafka", "num_partitions", "queued_max_messages"))

grouped = p_data.groupby(("msg_size_bytes", "use_rdkafka", "num_partitions"))
grouped.max()["bytes_per_sec"]
```

"""
import cPickle as pickle
from operator import attrgetter

import pandas


# Create attrgetter that turns rusage struct into a plain dict for pandas
ru_names = [
    "ru_utime", "ru_stime", "ru_maxrss", "ru_ixrss", "ru_idrss", "ru_isrss",
    "ru_minflt", "ru_majflt", "ru_nswap", "ru_inblock", "ru_oublock",
    "ru_msgsnd", "ru_msgrcv", "ru_nsignals", "ru_nvcsw", "ru_nivcsw"]
get_ru_fields = attrgetter(*ru_names)


def to_dataframe(filepath):
    """Read pickled benchmark data into pandas DataFrame"""
    f = open(filepath)
    data = []
    try:
        while True:
            data.append(pickle.load(f))
    except EOFError:
        pass

    # Transform rusage struct and flatten the results dict some more
    for row in data:
        if "rusage" in row["output"]:
            row["output"].update(
                dict(zip(ru_names, get_ru_fields(row["output"]["rusage"]))))
            del row["output"]["rusage"]
        else:
            print "Error occurred in run: {}".format(row)
        row.update(row["output"])
        row.update(row["params"])
        del row["output"]
        del row["params"]

    p_data = pandas.DataFrame(data)

    # Compute useful further values from results
    p_data["t_delta"] = p_data.t_end - p_data.t_begin
    p_data["msgs_per_sec"] = p_data.num_iterations / p_data.t_delta
    p_data["bytes_per_sec"] = p_data.msgs_per_sec * p_data.msg_size_bytes

    p_data["ru_utime_per_msg"] = p_data.ru_utime / p_data.num_iterations
    p_data["ru_stime_per_msg"] = p_data.ru_stime / p_data.num_iterations
    p_data["ru_rtime_per_msg"] = p_data.ru_stime_per_msg + p_data.ru_utime_per_msg

    return p_data
