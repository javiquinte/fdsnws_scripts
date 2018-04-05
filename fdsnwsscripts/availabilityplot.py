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

Request 60 minutes of the ``"LHZ"`` channel of EIDA stations starting with
``"A"`` for a seismic event around 2010-02-27 07:00 (UTC). Optionally add
``"-v"`` for verbosity. Resulting Mini-SEED data will be written to file
``"data.mseed"``.

.. code-block:: bash

    $ %(prog)s -N '*' -S 'A*' -L '*' -C 'LHZ' \
-s "2010-02-27T07:00:00Z" -e "2010-02-27T08:00:00Z" -v -o data.mseed

The above request is anonymous and therefore restricted data will not be
included. To include restricted data, use a file containing a token obtained
from an EIDA authentication service and/or a CSV file with username and
password for each node not implementing the EIDA auth extension.

.. code-block:: bash

    $ %(prog)s -a token.asc -c credentials.csv -N '*' -S 'A*' -L '*' -C 'LHZ' \
-s "2010-02-27T07:00:00Z" -e "2010-02-27T08:00:00Z" -v -o data.mseed

StationXML metadata for the above request can be requested using the following
command:

.. code-block:: bash

    $ %(prog)s -N '*' -S 'A*' -L '*' -C 'LHZ' \
-s "2010-02-27T07:00:00Z" -e "2010-02-27T08:00:00Z" -y station \
-q level=response -v -o station.xml

Multiple query parameters can be used:

.. code-block:: bash

    $ %(prog)s -N '*' -S '*' -L '*' -C '*' \
-s "2010-02-27T07:00:00Z" -e "2010-02-27T08:00:00Z" -y station \
-q format=text -q level=channel -q latitude=20 -q longitude=-150 \
-q maxradius=15 -v -o station.txt

Bulk requests can be made in ArcLink (-f), breq_fast (-b) or native FDSNWS POST
(-p) format. Query parameters should not be included in the request file, but
specified on the command line.

.. code-block:: bash

    $ %(prog)s -p request.txt -y station -q level=channel -v -o station.xml
"""

from __future__ import print_function
import sys
import matplotlib.pyplot as plt
import urlparse
import json
import datetime
import optparse
from fdsnwsscripts.fdsnws_fetch import route
from fdsnwsscripts.fdsnws_fetch import RoutingURL


VERSION = "2018.090"


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
            retry_wait=5,
            output_file="output.txt")

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

    parser.add_option("-t", "--timeout", type="int",
                      help="request timeout in seconds (default %default)")

    parser.add_option("-r", "--retries", type="int",
                      help="number of retries (default %default)")

    parser.add_option("-w", "--retry-wait", type="int",
                      help="seconds to wait before each retry "
                           "(default %default)")

    parser.add_option("-c", "--credentials-file", type="string",
                      help="URL,user,password file (CSV format) for queryauth")

    parser.add_option("-a", "--auth-file", type="string",
                      help="file that contains the auth token")

    parser.add_option("-p", "--post-file", type="string",
                      help="request file in FDSNWS POST format")

    parser.add_option("-o", "--output-file", type="string",
                      help="file where downloaded data is written")

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

    fig, ax = plt.subplots()
    ax.clear()
    ax.xaxis_date()
    ax.set_yticks([])

    dest = open(options.output_file, 'w')

    maxthreads = 1

    # qp = {'service': 'wfcatalog', 'net': options.network, 'sta': options.station,
    #       'loc': options.location, 'cha': options.channel,
    #       'start': options.starttime, 'end': options.endtime, 'csegments': 'true'}
    rurl = RoutingURL(urlparse.urlparse(options.url), qp)

    route(rurl, None, None, postdata, dest, chans_to_check, options.timeout,
          options.retries, options.retry_wait, maxthreads,
          options.verbose)

    dest.close()
    dest = open(options.output_file)

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
