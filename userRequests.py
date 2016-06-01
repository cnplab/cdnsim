"""
    CDNSim

file: userRequests.py

    NEC Europe Ltd. PROPRIETARY INFORMATION

This software is supplied under the terms of a license agreement
or nondisclosure agreement with NEC Europe Ltd. and may not be
copied or disclosed except in accordance with the terms of that
agreement. The software and its source code contain valuable trade
secrets and confidential information which have to be maintained in
confidence.
Any unauthorized publication, transfer to third parties or duplication
of the object or source code - either totally or in part - is
prohibited.

    Copyright (c) 2016 NEC Europe Ltd. All Rights Reserved.

Author: Anton Ivanov <anton.ivanov@neclab.eu>

NEC Europe Ltd. DISCLAIMS ALL WARRANTIES, EITHER EXPRESS OR IMPLIED,
INCLUDING BUT NOT LIMITED TO IMPLIED WARRANTIES OF MERCHANTABILITY
AND FITNESS FOR A PARTICULAR PURPOSE AND THE WARRANTY AGAINST LATENT
DEFECTS, WITH RESPECT TO THE PROGRAM AND THE ACCOMPANYING
DOCUMENTATION.

No Liability For Consequential Damages IN NO EVENT SHALL NEC Europe
Ltd., NEC Corporation OR ANY OF ITS SUBSIDIARIES BE LIABLE FOR ANY
DAMAGES WHATSOEVER (INCLUDING, WITHOUT LIMITATION, DAMAGES FOR LOSS
OF BUSINESS PROFITS, BUSINESS INTERRUPTION, LOSS OF INFORMATION, OR
OTHER PECUNIARY LOSS AND INDIRECT, CONSEQUENTIAL, INCIDENTAL,
ECONOMIC OR PUNITIVE DAMAGES) ARISING OUT OF THE USE OF OR INABILITY
TO USE THIS PROGRAM, EVEN IF NEC Europe Ltd. HAS BEEN ADVISED OF THE
POSSIBILITY OF SUCH DAMAGES.

    THIS HEADER MAY NOT BE EXTRACTED OR MODIFIED IN ANY WAY.
"""


from __future__ import print_function
from decorations import printWithClock, printInfo
import networkx
import Queue
import time
import csv
import re
import os

import sim_globals as sg
import sim_event as se
import netLink as nl
import cacheNode as cn
import netDataStream as ns


class userRequests:
    def __init__(self, max_hosts, applyManualInputData):
        self.re = re.compile(
            '(\d+\.\d+\.\d+\.\d+)\s(\S+)\s(\d+)'
            '\s(\d+\.\d+)\s(\d+\.\d+)\s(\d+\.\d+)\s(\d+)',
            re.UNICODE
        )
        self.requestQueue = Queue.Queue()
        self.noiseRequestQueue = Queue.Queue()
        self.traceHostMap = dict()
        self.activeStreams = 0
        self.totalStreams = 0
        self.activeStreamsMax = sg.args.active
        self.streamGenerationRate = self.calcStreamGenRate(sg.args.reqRate)
        self.streamGenRate_next = 0
        self.streamGenActive = True
        self.activeNoiseStreams = 0
        self.totalNoiseStreams = 0
        self.activeNoiseStreamsMax = int(sg.args.backnoise)
        self.startTime = None
        self.timer = time.time()
        self.initStreamsList = []
        self.listOfChannels = None
        self.numRequestsPerTimePeriod = 0
        self.streamGenRateScenario = []  # (time, requests per min)
        self.listOfHosts = sg.gnGraph.populateGeoNetGraph(
            max_hosts, sg.args.percentCache, applyManualInputData)
        if sg.args.scenario != '':
            if os.path.isfile(sg.args.scenario):
                printInfo("Using a scenaio file: " + sg.args.scenario)
                with open(sg.args.scenario, 'rb') as csvfile:
                    reader = csv.reader(csvfile)
                    for row in reader:
                        tim, rate = row
                        self.streamGenRateScenario.append(
                            (float(tim), float(rate))
                        )
            else:
                print("specified scenaio file not found: " +
                      sg.args.scenario)
                exit(-3)
        if sg.MODEL_USER_BEHAVIOR is True:
            self.startTime = 0.0
            self.traceFile = None
            for t, r in self.streamGenRateScenario:
                sg.simRef.eventPush(
                    se.event(t, id(self), sg.EVENT_CHANGE_REQUEST_RATE, self)
                )
            sg.simRef.eventPush(
                se.event(sg.args.endtime, id(self), sg.EVENT_SIM_FINALIZE, self)
            )
        else:
            self.traceFile = open(sg.args.trace, 'r')
        sg.simRef.eventPush(
            se.event(1, id(self), sg.EVENT_PERIODIC_STATS, self)
        )
        return

    def __del__(self):
        if self.traceFile is not None:
            self.traceFile.close()

    def calcStreamGenRate(self, userRequest=0.0):
        if sg.args.endtime < sg.MEAN_PBK_TIME:
            autoCalcRate = float(self.activeStreamsMax) / sg.args.endtime
        else:
            autoCalcRate = float(self.activeStreamsMax) / sg.MEAN_PBK_TIME
        if userRequest == 0.0:
            result = autoCalcRate
            printWithClock("request rate autoset to " + str(autoCalcRate * 60))
        else:
            result = float(userRequest) / 60
            if result < autoCalcRate:
                printInfo("given reqRate (" + str(60 * result) +
                          ") is too small to guarantee " +
                          str(sg.args.active) +
                          " active connections. Try reqRate = " +
                          str(60 * autoCalcRate)
                          )
            elif result > autoCalcRate:
                printInfo("given reqRate (" + str(60 * result) +
                          ") is too high. Number active connections (" +
                          str(sg.args.active) +
                          ") will be exceeded. Try reqRate = " +
                          str(60 * autoCalcRate)
                          )
        return result

    def genChannelNumber(self):
        channel = sg.numpy.random.zipf(1.2) - 1
        while channel >= sg.NUMBER_CHANNELS:
            channel = sg.numpy.random.zipf(1.2) - 1
        return channel

    def getNextEvent(self, curTime):
        if sg.MODEL_USER_BEHAVIOR:
            self.totalStreams += 1
            randHost = sg.random.choice(self.listOfHosts).exploded
            randStartTime = curTime + sg.numpy.random.\
                standard_gamma(1.0/self.streamGenerationRate)
            randPlayTime = sg.numpy.random.\
                triangular(sg.MIN_PBK_TIME, sg.MOD_PBK_TIME, sg.MAX_PBK_TIME)
            rateN = sg.numpy.random.poisson(2)
            while rateN > len(sg.STREAM_RATES) - 1:
                rateN = sg.numpy.random.poisson(2)
            randStreamRate = sg.STREAM_RATES[rateN]
            futureRequest = (
                randHost,
                randStreamRate,
                randStreamRate * randPlayTime
            )
            ev = se.event(randStartTime, id(self), sg.EVENT_USER_REQUEST, self)
        else:
            # If we have a trace file with realistic user events...
            futureRequestLine = self.traceFile.readline()
            if futureRequestLine == '':
                return None
            match = self.re.match(futureRequestLine)
            if match is not None:
                if self.startTime is None:
                    self.startTime = float(match.group(4))
                # if the trace file is using masked
                # ip-addresses, we have to re-map them
                if match.group(1) not in self.traceHostMap:
                    randHost = sg.random.choice(self.listOfHosts).exploded
                    self.traceHostMap[match.group(1)] = randHost
                else:
                    randHost = self.traceHostMap[match.group(1)]
                futureRequest = (
                    randHost,
                    sg.STREAM_RATES[2],
                    float(match.group(7))
                )
                ev = se.event(
                    float(match.group(4)) - self.startTime,
                    id(self),
                    sg.EVENT_USER_REQUEST,
                    self
                )
            else:
                raise Exception(
                    "Unrecognized format of user behavior trace file,"
                    " line:\n\t>> " + futureRequestLine
                )
        self.requestQueue.put(futureRequest)
        return ev

    def getNoiseEvent(self, curTime):
        self.totalNoiseStreams += 1
        randHost = sg.random.choice(self.listOfHosts).exploded
        randStartTime = curTime + sg.numpy.random.\
            standard_gamma(sg.MEAN_PBK_TIME/self.activeNoiseStreamsMax)
        randPlayTime = sg.numpy.random.triangular(600, 1800, 3600)
        randStreamRate = sg.STREAM_RATES[int(
            sg.numpy.random.triangular(
                -1, len(sg.STREAM_RATES) / 2, len(sg.STREAM_RATES)
            ))]
        futureNoiseRequest = \
            (randHost, randStreamRate, randPlayTime * randStreamRate)
        self.noiseRequestQueue.put(futureNoiseRequest)
        ev = se.event(
            randStartTime,
            id(self),
            sg.EVENT_NOISE_USER_REQUEST,
            self
        )
        return ev

    def routeStreamPath(self, path, s, curTime):
        nodeA = path[0]
        as_nodeA = sg.gnGraph.netGraph.node[nodeA]
        for nodeB in path[1:]:
            as_nodeB = sg.gnGraph.netGraph.node[nodeB]
            link_AB = sg.gnGraph.netGraph[nodeA][nodeB]
            if 'p2p_link' not in link_AB:
                if sg.gnGraph.isAccessNode(
                        as_nodeA['type']
                ) or sg.gnGraph.isAccessNode(
                        as_nodeB['type']
                ):
                    link_AB['p2p_link'] = \
                        nl.netLink(
                            sg.BACKBONE_LINK_BANDWIDTH,
                            nodeA,
                            nodeB
                        )
                else:
                    link_AB['p2p_link'] = \
                        nl.netLink(
                            sg.FAST_BACKBONE_LINK_BANDWIDTH,
                            nodeA,
                            nodeB
                        )
            s.links.append(link_AB['p2p_link'])
            nodeA = nodeB
            as_nodeA = sg.gnGraph.netGraph.node[nodeA]
        if s.streamType == sg.STREAM_NOISE and not sg.simRef.simulatorReady:
            for l in s.links:
                l.netDataStreams.append(s)
            self.initStreamsList.append(s)
        else:
            sg.simRef.eventPush(
                se.event(
                    curTime + sg.PROPAGATION_DELAY*len(path),
                    id(s),
                    sg.EVENT_STREAM_START,
                    s
                )
            )
        return

    def addCacheToAS(self, ASn, curTime, channelNum, static=False):
        thisAS = sg.gnGraph.netGraph.node[ASn]
        if 'caches' not in thisAS:
            thisAS['caches'] = [None] * sg.NUMBER_CHANNELS
            thisAS['stats_maxConnections'] = 0
            thisAS['stats_maxThroughput'] = 0.0
            thisAS['stats_max_NumVMs'] = 0
            thisAS['cur_Connections'] = 0
            thisAS['cur_Throughput'] = 0.0
            thisAS['cur_NumVMs'] = 0
            # 1 vm per channel (all str.Rates)
        if thisAS['caches'][channelNum] is None:
            cache = cn.cacheNode(ASn)
            thisAS['cur_NumVMs'] += 1
            if thisAS['cur_NumVMs'] > thisAS['stats_max_NumVMs']:
                thisAS['stats_max_NumVMs'] = thisAS['cur_NumVMs']
            assert cache.id not in sg.gnGraph.netGraph
            sg.gnGraph.netGraph.add_edge(ASn, cache.id)
            thisAS['caches'][channelNum] = cache
            if static:
                cache.process(
                    se.event(
                        curTime,
                        id(cache),
                        sg.EVENT_CACHE_READY,
                        cache
                    )
                )
            else:
                sg.simRef.eventPush(
                    se.event(
                        curTime + sg.args.cacheinit,
                        id(cache),
                        sg.EVENT_CACHE_READY,
                        cache
                    )
                )
        else:
            cache = thisAS['caches'][channelNum]
        return cache

    def routeStreamPath_inclCache(self, path, s, curTime, first=True):
        cacheOnDemand = sg.args.ondemandCache
        nodeA = path[0]
        as_nodeA = sg.gnGraph.netGraph.node[nodeA]
        for nodeB in path[1:]:
            link_AB = sg.gnGraph.netGraph[nodeA][nodeB]
            # Creating a link between node A and B, if it does not exist yet
            if 'p2p_link' not in link_AB:
                # if one of the nodes is an 'access' AS node then the link
                # speed is set to BACKBONE_LINK_BANDWIDTH
                if sg.gnGraph.isAccessNode(
                        as_nodeA['type']
                ) or sg.gnGraph.isAccessNode(
                    sg.gnGraph.netGraph.node[nodeB]['type']
                ):
                    link_AB['p2p_link'] = \
                        nl.netLink(
                            sg.BACKBONE_LINK_BANDWIDTH,
                            nodeA,
                            nodeB
                        )
                else:
                    link_AB['p2p_link'] = \
                        nl.netLink(
                            sg.FAST_BACKBONE_LINK_BANDWIDTH,
                            nodeA,
                            nodeB
                        )
            if nodeA == path[0] or not sg.LOCAL_CACHE_ONLY:
                # increase the cache-init counter and check the threshold
                if 'nCacheRequests' not in as_nodeA:
                    as_nodeA['nCacheRequests'] = [0] * sg.NUMBER_CHANNELS
                as_nodeA['nCacheRequests'][s.channel] += 1
                if as_nodeA['nCacheRequests'][s.channel] >= \
                        sg.args.cachethreshold:
                    # threshold passed, add a cache
                    # (all checks are inside the 'addCacheToAS')
                    cache = None
                    if 'static_cache' in as_nodeA:
                        cache = self.addCacheToAS(
                            nodeA,
                            curTime,
                            s.channel,
                            static=True
                        )
                    elif cacheOnDemand and first:
                        cache = self.addCacheToAS(nodeA, curTime, s.channel)
                    if cache is not None\
                            and cache != s.downCacheRef \
                            and cache.attachNetDataStream(s, curTime):
                        # 'attachNetDataStream' returns False if cache is not
                        # ready yet, if connected -> stop routing
                        break
            # if the stream is not connected to a
            # cache @ node A (or there is no cache @ node A)
            if not s.connectedToCache:
                # add the link from node A to node B to the stream path
                # (! this does not mean adding stream to all links along
                # the path, this is done later)
                s.links.append(link_AB['p2p_link'])
            nodeA = nodeB
            as_nodeA = sg.gnGraph.netGraph.node[nodeA]
        # background noise streams: adding stream to all links along
        # the path at init time
        if not sg.simRef.simulatorReady and s.streamType == sg.STREAM_NOISE:
            for l in s.links:
                l.netDataStreams.append(s)
            self.initStreamsList.append(s)
        else:
            # schedule 'start streaming' events
            if not s.connectedToCache:
                sg.simRef.eventPush(
                    se.event(
                        curTime + sg.PROPAGATION_DELAY*len(path),
                        id(s),
                        sg.EVENT_STREAM_START,
                        s
                    )
                )
        return

    def process(self, ev):
        if ev.type == sg.EVENT_USER_REQUEST:
            dest_ip, stream_rate, data_size = self.requestQueue.get()
            hostAs = sg.gnGraph.ip2as[dest_ip]
            path = networkx.shortest_path(
                sg.gnGraph.netGraph,
                hostAs,
                sg.gnGraph.contentProvider
            )
            serv_ip = sg.gnGraph.netGraph.node[sg.gnGraph.contentProvider]['ip'].exploded
            ds = ns.netDataStream(
                stream_rate,
                serv_ip,
                dest_ip,
                data_size,
                self.genChannelNumber()
            )
            ds.bufferingBegin = ev.time
            if sg.args.streaming:
                self.routeStreamPath_inclCache(path, ds, ev.time)
            else:
                self.routeStreamPath(path, ds, ev.time)
            # statistics for user request
            ds.stats_events.append((ev.time, ev.type))
            self.activeStreams += 1
            self.numRequestsPerTimePeriod += 1
            if self.streamGenActive:
                sg.simRef.eventPush(self.getNextEvent(ev.time))
        elif ev.type == sg.EVENT_NOISE_USER_REQUEST:
            dest_ip, stream_rate, data_size = self.noiseRequestQueue.get()
            hostAs = sg.gnGraph.ip2as[dest_ip]
            servAs = sg.random.choice(sg.gnGraph.contentNodes)
            serv_ip = sg.gnGraph.as2ip[servAs][0][1].exploded
            path = networkx.shortest_path(sg.gnGraph.netGraph, hostAs, servAs)
            ds = ns.netDataStream(
                stream_rate,
                serv_ip,
                dest_ip,
                data_size,
                strType=sg.STREAM_NOISE
            )
            self.routeStreamPath(path, ds, ev.time)
            if sg.simRef.simulatorReady:
                sg.simRef.eventPush(
                    se.event(
                        ev.time + sg.PROPAGATION_DELAY*len(path),
                        id(ds),
                        sg.EVENT_STREAM_START,
                        ds
                    )
                )
            else:
                self.initStreamsList.append(ds)
            self.activeNoiseStreams += 1
            if not sg.simRef.simulationDone:
                if not sg.simRef.simulatorReady:
                    sg.simRef.eventPush(self.getNoiseEvent(ev.time))
                    if self.activeNoiseStreams >= self.activeNoiseStreamsMax:
                        for tmpStream in self.initStreamsList:
                            tmpStream.startStreaming(ev.time)
                            tmpStream.bufferingBegin = ev.time
                        self.initStreamsList = []
                        sg.simRef.simulatorReady = True
                        self.streamGenActive = True
                        # start normal stream
                        sg.simRef.eventPush(self.getNextEvent(ev.time))
        elif ev.type == sg.EVENT_CHANGE_REQUEST_RATE:
            self.streamGenerationRate = self.calcStreamGenRate(
                self.streamGenRateScenario[self.streamGenRate_next][1]
            )
            self.streamGenRate_next += 1
        elif ev.type == sg.EVENT_SIM_FINALIZE:
            printWithClock("Simulated: {:.1f}s.".format(float(ev.time)) +
                           " -- SIM_FINALIZE: no new streams", pre='\n')
            self.streamGenActive = False
        elif ev.type == sg.EVENT_PERIODIC_STATS:
            sg.simRef.urStatistics_nActCons.append(
                (ev.time, self.activeStreams)
            )
            reqPerSec = float(self.numRequestsPerTimePeriod) / 10 * 60
            sg.simRef.urStatistics_nReqPSec.append((ev.time, reqPerSec))
            self.numRequestsPerTimePeriod = 0
            if not sg.simRef.simulationDone:
                sg.simRef.eventPush(
                    se.event(
                        ev.time + 1,
                        id(self),
                        sg.EVENT_PERIODIC_STATS,
                        self
                    )
                )
            curTime = time.time()
            printWithClock(
                "Simulated: {:.1f}s.".format(float(ev.time)) +
                " active streams = " + str(self.activeStreams) +
                ", 1 sim-second = {:.1f}s.".format(curTime - self.timer),
                pre='\r', end='\n' if ev.time % 10 == 0 else ''
            )
            self.timer = curTime
        else:
            raise Exception("Unknown event type:" + str(ev.type))
        return
