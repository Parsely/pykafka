from collections import defaultdict, namedtuple
import json
import sys

import matplotlib.pyplot as plt


def analyse(filename="consumer_bench.json"):

    with open(filename) as f:
        rawdata = map(json.loads, f.readlines())

    sorteddata = defaultdict(
        lambda: defaultdict(lambda: defaultdict(lambda: defaultdict(list))))
    # some namedtuples (just for ipython pretty-printing while exploring):
    Key0 = namedtuple("Key0", ("num_partitions", "msg_size_bytes"))
    Key2 = namedtuple("Key2", ("num_consumer_fetchers", "queued_max_messages"))

    for d in rawdata:
        key0 = Key0(d["num_partitions"], d["msg_size_bytes"])
        key1 = d["consumer_type"]
        key2 = Key2(d["num_consumer_fetchers"], d["queued_max_messages"])
        bucket = sorteddata[key0][key1][key2]
        bucket["rawdata"].append(d)

        # We're looking for the best, not the average, throughput of all runs:
        throughput_bps = (
            d["num_iterations"] * d["msg_size_bytes"] / d["runtime_secs"])
        bucket["throughput_bps"].append(throughput_bps)  # to compute stddev

        # The whole point of this separation into key0, key2 is that key2
        # contains parameters we "don't care about" - that is, we'll be happy
        # to set them to whatever makes it fly best. So we're interested in
        # the maximum across all combinations of key2:
        parent = sorteddata[key0][key1]
        if not parent["max_throughput_bps"]:  # it's a defaultdict
            parent["max_throughput_bps"] = 0.
        if throughput_bps > parent["max_throughput_bps"]:
            parent["max_throughput_bps"] = throughput_bps
            parent["winning_key"] = key2

    for v in sorteddata.values():
        v["performance_ratio"] = (v["rdkafka"]["max_throughput_bps"]
                                  / v["pure-py"]["max_throughput_bps"])
    return sorteddata


def plot(sorteddata):
    plt.figure()
    plt.suptitle("pure-pykafka vs pykafka.rdkafka consumer throughput")

    legend_handles = []
    legend_labels = []
    xlabels = sorted(sorteddata.keys())
    for key1, clr in ("rdkafka", "red"), ("pure-py", "blue"):
        plotdata = []
        for key0 in xlabels:
            key2 = sorteddata[key0][key1]["winning_key"]
            plotdata.append(sorteddata[key0][key1][key2]["throughput_bps"])
        bp = plt.boxplot(plotdata)
        for styleables in "boxes", "whiskers", "caps", "fliers", "medians":
            # Only way to set colours?!
            for st in bp[styleables]:
                st.set(color=clr)
        # proxy for legend:
        proxy, = plt.plot([], color=clr, label=key1)
        legend_handles.append(proxy)
        legend_labels.append(key1)

    plt.xlabel("topic partition number, message size (bytes)")
    plt.ylabel("throughput (bps) as boxplots")
    plt.semilogy()

    locs, _ = plt.xticks()  # get tick locations to align the plots
    plt.xticks(locs, map(tuple, xlabels), rotation='vertical')

    plt.twinx()
    ratios = [sorteddata[key0]["performance_ratio"] for key0 in xlabels]
    ratio_plot, = plt.plot(locs, ratios, '-x', color="grey", label="ratio")
    legend_handles.append(ratio_plot)
    legend_labels.append("ratio")
    plt.ylabel("rdkafka to pure-python performance ratio")
    plt.semilogy()
    for l, r in zip(locs, ratios):  # print ratios for readability
        plt.annotate("{:.3g}".format(r), xy=(l + .1, r))

    plt.legend(legend_handles, legend_labels, ncol=3, loc="upper right")
    plt.show()


if __name__ == '__main__':
    filename = sys.argv[1]
    sorteddata = analyse(filename)
    plot(sorteddata)
