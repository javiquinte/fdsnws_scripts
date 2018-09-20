#!/usr/bin/env python2
# -*- coding: utf-8 -*-

###############################################################################
# (C) 2016-2017 Helmholtz-Zentrum Potsdam - Deutsches GeoForschungsZentrum GFZ#
#                                                                             #
# License: LGPLv3 (https://www.gnu.org/copyleft/lesser.html)                  #
###############################################################################

"""
A command-line FDSN Web Service client using EIDA routing and authentication.

Usage Examples
==============

Check the availability of 60 minutes of the ``"LHZ"`` channel of EIDA stations
starting with ``"A"`` for a seismic event around 2010-02-27 07:00 (UTC). Optionally
add ``"-v"`` for verbosity. A graphic showing the availability will be written to
file ``"data.png"``, while a text file to request the (meta)data will be written
to file ``"data-00.txt"``.

.. code-block:: bash

    $ %(prog)s -N '*' -S 'A*' -L '*' -C 'LHZ' \
-s "2010-02-27T07:00:00Z" -e "2010-02-27T08:00:00Z" -v -o data

"""

from __future__ import print_function
import sys
import io
import matplotlib.pyplot as plt
import urlparse
import json
import datetime
import optparse
from fdsnwsscripts.fdsnws_fetch import route
from fdsnwsscripts.fdsnws_fetch import RoutingURL


VERSION = "2018.171"


class Segments(list):
    def append(self, it):
        # Accept a starttime and endtime and merge it with existing segments if available.
        # If there are no other segments next to it, append it normally to the list.
        if (len(it) != 2) or (type(it) != tuple):
            raise TypeError('A tuple with 2 components was expected!')

        # print(self, it)
        for ind in range(len(self) - 1, -1, -1):
            if self[ind][0] <= it[0] <= self[ind][1]:
                it = (self[ind][0], it[1])
                del self[ind]
                super(Segments, self).insert(ind, it)
                return
            elif self[ind][0] <= it[1] <= self[ind][1]:
                it = (it[0], self[ind][1])
                del self[ind]
                super(Segments, self).insert(ind, it)
                return
            elif (it[0] <= self[ind][0]) and (self[ind][1] <= it[1]):
                del self[ind]
                super(Segments, self).insert(ind, it)
                return
            elif (self[ind][0] <= it[0]) and (it[1] <= self[ind][1]):
                return

        for ind in range(len(self)):
            if self[ind][0] <= it[0]:
                super(Segments, self).insert(ind, it)
                return
        else:
            super(Segments, self).insert(0, it)

        if not len(self):
            super(Segments, self).append(it)
        # print(self)

        return


def getstreams(wfc, maxsize=None):
    maxsize = maxsize * 1024 * 1024
    # streams = dict()
    result = [dict()]

    reqlines = list()
    result = list()
    reqsize = 0
    package = 0
    emptypackage = True

    # wfc.sort(key=lambda i: '%s.%s.%s.%s' % (i["network"], i["station"], i["location"], i["channel"]))

    # We expect that wfc is sorted by stream code!
    for i in wfc:
        if not(emptypackage) and (reqsize + i["num_records"] * max(i["record_length"]) > maxsize):
            reqsize = 0
            emptypackage = True
            package = package + 1

        emptypackage = False
        reqsize = reqsize + i["num_records"] * max(i["record_length"])

        if "c_segments" not in i:
            reqlines.append((i["network"], i["station"], i["location"], i["channel"],
                             datetime.datetime.strptime(i["start_time"], "%Y-%m-%dT%H:%M:%S.%fZ"),
                             datetime.datetime.strptime(i["end_time"], "%Y-%m-%dT%H:%M:%S.%fZ"),
                             package))
            continue

        for cs in i["c_segments"]:
            reqlines.append((i["network"], i["station"], i["location"], i["channel"],
                             datetime.datetime.strptime(cs["start_time"], "%Y-%m-%dT%H:%M:%S.%fZ"),
                             datetime.datetime.strptime(cs["end_time"], "%Y-%m-%dT%H:%M:%S.%fZ"),
                             package))

    stream = None
    segs = Segments()
    package = None

    for i in reqlines:
        if stream != i[:4]:
            if len(segs):
                for s in segs:
                    result.append((stream[0], stream[1], stream[2], stream[3], s[0], s[1], package))
            stream = i[:4]
            package = i[-1]
            segs = Segments()

        segs.append((i[4], i[5]))

    # Flush last stream
    if len(segs):
        for s in segs:
            result.append((stream[0], stream[1], stream[2], stream[3], s[0], s[1], package))

    return result


def main():
    qp = {'service': 'wfcatalog',
          'csegments': 'true'}

    def add_qp(option, opt_str, value, parser):
        if option.dest == 'query':
            try:
                (p, v) = value.split('=', 1)
                qp[p] = v

            except ValueError:
                raise optparse.OptionValueError("%s expects parameter=value"
                                                % opt_str)

        else:
            qp[option.dest] = value

    parser = optparse.OptionParser(
            usage="Usage: %prog [-h|--help] [OPTIONS] -o file",
            version="%prog " + VERSION,
            add_help_option=False)

    parser.set_defaults(
            url="http://geofon.gfz-potsdam.de/eidaws/routing/1/",
            timeout=30,
            retries=0,
            retry_wait=0,
            max=10000,
            output_file="output")

    parser.add_option("-h", "--help", action="store_true", default=False,
                      help="show help message and exit")

    parser.add_option("-l", "--longhelp", action="store_true", default=False,
                      help="show extended help message and exit")

    parser.add_option("-v", "--verbose", action="store_true", default=False,
                      help="verbose mode")

    parser.add_option("-u", "--url", type="string",
                      help="URL of routing service (default %default)")

    parser.add_option("-N", "--network", type="string", action="callback",
                      callback=add_qp,
                      help="network code or pattern")

    parser.add_option("-S", "--station", type="string", action="callback",
                      callback=add_qp,
                      help="station code or pattern")

    parser.add_option("-L", "--location", type="string", action="callback",
                      callback=add_qp,
                      help="location code or pattern")

    parser.add_option("-C", "--channel", type="string", action="callback",
                      callback=add_qp,
                      help="channel code or pattern")

    parser.add_option("-s", "--starttime", type="string", action="callback",
                      callback=add_qp,
                      help="start time")

    parser.add_option("-e", "--endtime", type="string", action="callback",
                      callback=add_qp,
                      help="end time")

    parser.add_option("-m", "--max", type="int",
                      help="Maximum size (in MB) for each request (default %default)")

    parser.add_option("-t", "--timeout", type="int",
                      help="request timeout in seconds (default %default)")

    # parser.add_option("-r", "--retries", type="int",
    #                   help="number of retries (default %default)")

    # parser.add_option("-w", "--retry-wait", type="int",
    #                   help="seconds to wait before each retry "
    #                        "(default %default)")

    parser.add_option("-c", "--credentials-file", type="string",
                      help="URL,user,password file (CSV format) for queryauth")

    parser.add_option("-a", "--auth-file", type="string",
                      help="file that contains the auth token")

    parser.add_option("-p", "--post-file", type="string",
                      help="request file in FDSNWS POST format")

    parser.add_option("-o", "--output-file", type="string",
                      help="filename (no extension) where streams to download and availability plot must be saved")

    (options, args) = parser.parse_args()

    if options.help:
        print(__doc__.split("Usage Examples", 1)[0], end="")
        parser.print_help()
        return 0

    if options.longhelp:
        print(__doc__)
        parser.print_help()
        return 0

    if args or not options.output_file:
        parser.print_usage(sys.stderr)
        return 1

    if qp.get('network', '*') == '*':
        print("Missing network parameter! This will be cancelled to avoid the request of a big amount of unwanted data.")
        print(__doc__.split("Usage Examples", 1)[0], end="")
        parser.print_help()
        return 2

    chans_to_check = set()
    postdata = None

    if options.post_file:
        try:
            with open(options.post_file) as fd:
                postdata = fd.read()

        except UnicodeDecodeError:
            raise Exception("invalid unicode character found in %s"
                            % options.post_file)

    if postdata is not None:
        for line in postdata.splitlines():
            nslc = line.split()[:4]
            if nslc[2] == '--': nslc[2] = ''
            chans_to_check.add('.'.join(nslc))

    else:
        net = qp.get('network', '*')
        sta = qp.get('station', '*')
        loc = qp.get('location', '*')
        cha = qp.get('channel', '*')

        for n in net.split(','):
            for s in sta.split(','):
                for l in loc.split(','):
                    for c in cha.split(','):
                        if l == '--': l = ''
                        chans_to_check.add('.'.join((n, s, l, c)))

    respwfc = io.BytesIO()

    maxthreads = 1

    # qp = {'service': 'wfcatalog', 'net': options.network, 'sta': options.station,
    #       'loc': options.location, 'cha': options.channel,
    #       'start': options.starttime, 'end': options.endtime, 'csegments': 'true'}
    rurl = RoutingURL(urlparse.urlparse(options.url), qp)

    try:
        route(rurl, None, None, postdata, respwfc, chans_to_check, options.timeout,
              options.retries, options.retry_wait, maxthreads,
              options.verbose)

    except:
        print('Exception')

    respwfc.seek(0)

    strs = json.loads(respwfc.read())
    # print(strs)

    strs.sort(key=lambda i: '%s.%s.%s.%s' % (i["network"], i["station"], i["location"], i["channel"]))

    results = getstreams(strs, options.max)

    part = -1
    for i in results:
        # print(i)
        if part != i[-1]:
            try:
                dest.close()
            except Exception:
                pass
            part = i[-1]
            dest = open('%s-%02d.txt' % (options.output_file, i[-1]), 'w')

        dest.write('%s %s %s %s %s %s\n' % (i[0], i[1], i[2], i[3], i[4].isoformat(), i[5].isoformat()))

    # Availability plot generation
    fig, ax = plt.subplots()
    ax.clear()
    ax.xaxis_date()
    ax.set_yticks([])

    labels = list()
    base = -1
    pendingsegment = None

    for i in results:
        # print(i)
        stream = '%s.%s.%s.%s' % tuple(i[0:4])

        if not len(labels) or (stream != labels[-1]):
            base = base + 1
            labels.append(stream)
            pendingsegment = None

        if pendingsegment:
            # print("Gap:", pendingsegment, i[4])
            ax.hlines(base, pendingsegment, i[4], 'r', linewidth=8, zorder=3)

        ax.hlines(base, i[4], i[5], 'g', linewidth=6, zorder=2)
        pendingsegment = i[5]

    ax.set_yticks(range(len(labels)))
    ax.set_yticklabels(labels, family="monospace", ha="right")
    ax.autoscale_view()
    fig.autofmt_xdate()
    plt.draw()
    plt.savefig(options.output_file + '.png')


if __name__ == "__main__":
    main()
