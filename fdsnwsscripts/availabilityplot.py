#!/usr/bin/env python2
# -*- coding: utf-8 -*-

###############################################################################
# (C) 2016-2017 Helmholtz-Zentrum Potsdam - Deutsches GeoForschungsZentrum GFZ#
#                                                                             #
# License: LGPLv3 (https://www.gnu.org/copyleft/lesser.html)                  #
###############################################################################

import matplotlib.pyplot as plt
import urlparse
import json
import datetime
import io
from fdsnwsscripts.fdsnws_fetch import route
from fdsnwsscripts.fdsnws_fetch import RoutingURL


class Segments(list):
    def append(self, it):
        if (len(it) != 2) or (type(it) != tuple):
            raise TypeError('A tuple with 2 components was expected!')

        for ind in range(len(self) - 1, -1, -1):
            if self[ind][0] <= it[0] <= self[ind][1]:
                it = (self[ind][0], it[1])
                del self[ind]
            elif self[ind][0] <= it[1] <= self[ind][1]:
                it = (it[0], self[ind][1])
                del self[ind]
            elif (it[0] <= self[ind][0]) and (self[ind][1] <= it[1]):
                del self[ind]
            elif (self[ind][0] <= it[0]) and (it[1] <= self[ind][1]):
                continue
        super(Segments, self).append(it)

        return


def getsegments(wfc, streams=None):

    dictstreams = dict()
    if streams is not None:
        for s in streams.keys():
            dictstreams[s] = list()

    else:
        for i in wfc:
            s = "%s.%s.%s.%s" % (i["network"], i["station"], i["location"], i["channel"])
            dictstreams[s] = list()

    requested1 = list()
    segs1 = list()

    for i in wfc:
        s = "%s.%s.%s.%s" % (i["network"], i["station"], i["location"], i["channel"])
        reqseg = (datetime.datetime.strptime(i["start_time"], "%Y-%m-%dT%H:%M:%S.%fZ"),
                  datetime.datetime.strptime(i["end_time"], "%Y-%m-%dT%H:%M:%S.%fZ"))

        if "c_segments" not in i:
            dictstreams[s].append(reqseg)
            continue

        for cs in i["c_segments"]:
            dictstreams[s].append((datetime.datetime.strptime(cs["start_time"], "%Y-%m-%dT%H:%M:%S.%fZ"),
                                   datetime.datetime.strptime(cs["end_time"], "%Y-%m-%dT%H:%M:%S.%fZ")))

    for st in dictstreams:
        # Merge segments
        segs2 = Segments()
        for s in dictstreams[st]:
            segs2.append(s)

        segs2.sort()
        dictstreams[st] = segs2
    return dictstreams


def getstreams(wfc):

    streams = dict()
    requested1 = list()
    segs1 = list()

    for i in wfc:
        stream = "%s.%s.%s.%s" % (i["network"], i["station"], i["location"], i["channel"])

        reqseg = (datetime.datetime.strptime(i["start_time"], "%Y-%m-%dT%H:%M:%S.%fZ"),
                  datetime.datetime.strptime(i["end_time"], "%Y-%m-%dT%H:%M:%S.%fZ"))

        if stream not in streams.keys():
            streams[stream] = list()
        streams[stream].append(reqseg)

    for stream in streams:
        # Merge requested
        segs = Segments()
        for r in sorted(streams[stream]):
            segs.append(r)

        streams[stream] = segs

    return streams


def main():
    fig, ax = plt.subplots()
    ax.clear()
    ax.xaxis_date()
    ax.set_yticks([])

    net = 'NL'
    sta = 'G700'
    loc = '*'
    cha = 'HG*'
    sttime = datetime.datetime(2017, 1, 1)
    endtime = datetime.datetime(2017, 1, 3)
    dest = io.BytesIO()
    timeout = 30
    retry_count = 0
    retry_wait = 1
    maxthreads = 1
    verbose = 1

    url = "http://geofon.gfz-potsdam.de/eidaws/routing/1/"

    qp = {'service': 'wfcatalog', 'net': net, 'sta': sta, 'loc': loc, 'cha': cha,
          'start': sttime.isoformat(), 'end': endtime.isoformat(), 'csegments': 'true'}
    rurl = RoutingURL(urlparse.urlparse(url), qp)

    route(rurl, None, None, None, dest, None, timeout, retry_count, retry_wait, maxthreads, verbose)

    dest.seek(0)

    strs = json.loads(dest.read())

    streams = getstreams(strs)
    segments = getsegments(strs, streams)

    labels = list()
    base = 0
    for stream in streams:
        labels.append(stream)
        for i in streams[stream]:
            allse = list()
            ax.hlines(base, i[0], i[1], 'b', linewidth=10)
            for seg in segments[stream]:
                ax.hlines(base, seg[0], seg[1], 'g', linewidth=8, zorder=3)
                allse.append(seg[0])
                allse.append(seg[1])

            for i in range(1, len(allse)-1, 2):
                if allse[i] > allse[i+1]:
                    continue
                print("Gap:", allse[i], allse[i+1])
                ax.hlines(base, allse[i], allse[i+1], 'r', linewidth=8, zorder=3)

        base = base + 1

    ax.set_yticks(range(len(labels)))
    ax.set_yticklabels(labels, family="monospace", ha="right")
    ax.autoscale_view()
    fig.autofmt_xdate()
    plt.draw()

    plt.show()


if __name__ == "__main__":
    main()
