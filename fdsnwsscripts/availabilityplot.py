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


def getsegments(wfc):

    requested1 = list()
    segs1 = list()

    for i in wfc:
        reqseg = (datetime.datetime.strptime(i["start_time"], "%Y-%m-%dT%H:%M:%S.%fZ"),
                  datetime.datetime.strptime(i["end_time"], "%Y-%m-%dT%H:%M:%S.%fZ"))
        requested1.append(reqseg)

        if "c_segments" not in i:
            segs1.append(reqseg)
            continue

        for cs in i["c_segments"]:
            segs1.append((datetime.datetime.strptime(cs["start_time"], "%Y-%m-%dT%H:%M:%S.%fZ"),
                          datetime.datetime.strptime(cs["end_time"], "%Y-%m-%dT%H:%M:%S.%fZ")))

    # Merge requested
    requested2 = Segments()
    for r in requested1:
        requested2.append(r)

    # Merge segments
    segs2 = Segments()
    for s in segs1:
        segs2.append(s)

    requested2.sort()
    segs2.sort()
    return requested2, segs2


def main():
    fig, ax = plt.subplots()
    ax.clear()
    ax.xaxis_date()
    ax.set_yticks([])

    net = 'NL'
    sta = 'G700'
    loc = '*'
    cha = 'HGZ'
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

    print(strs)
    requested, segs = getsegments(strs)
    print(requested, segs)
    for i in requested:
        ax.axvspan(i[0], i[1],
                   0, 0.6, fc="b", lw=0)

    for cs in segs:
        ax.axvspan(cs[0], cs[1], 0.6, 1, facecolor="g", lw=0)

    allse = list()
    for cs in segs:
        allse.append(cs[0])
        allse.append(cs[1])

    for i in range(1, len(allse)-1, 2):
        if allse[i] > allse[i+1]:
            continue
        print("Gap:", allse[i], allse[i+1])
        ax.axvspan(allse[i], allse[i+1], 0.6, 1, facecolor="r", lw=0)

    ax.autoscale_view()
    fig.autofmt_xdate()
    plt.draw()

    plt.show()


if __name__ == "__main__":
    main()
