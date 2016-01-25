"""
        CDNSim

    file: netStreamingPrimitives.py

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
import networkx as nx
import matplotlib.pyplot as plt
import numpy.random
import random
import Queue
import math
import csv
import sys
import os
import re

numpy.random.seed(42)
random.seed(42)

EVENT_RESERVED = 0
EVENT_USER_REQUEST = 1
EVENT_STREAM_START = 2
EVENT_STREAM_COMPLETED = 3
EVENT_CONSUME_BEGIN = 4
EVENT_CONSUME_COMPLETE = 5
EVENT_CONSUME_BUFFER_EMPTY = 6

EVENT_STREAM_EXPAND = 7
EVENT_NOISE_USER_REQUEST = 8
EVENT_SWITCH_TO_LIVERATE = 9
EVENT_CACHE_READY = 10

EVENT_CHANGE_REQUEST_RATE = 11
EVENT_SIM_FINALIZE = 12
EVENT_PERIODIC_STATS = 13


NAMES_EVENTS = ['---',
                'Connect reqst',
                'Dwnl. started',
                'Dwnl. stopped',
                'Playing start',
                'Playing stop',
                'Buffer empty',
                'Stream expand',
                'Noise request',
                'to live-TRate']

COLORS_EVENTS = []
m = plt.cm.get_cmap('Paired')
for i in range(1, len(NAMES_EVENTS) + 1):
    COLORS_EVENTS.append(m(float(i) / (len(NAMES_EVENTS) + 1)))

PROPAGATION_DELAY = 0.01
# Max rates for video streaming quality: 360p, 480p, 720p, 1080p, 2K, 4K
STREAM_RATES = [1000000, 2500000, 5000000, 8000000, 10000000, 20000000]
FAST_BACKBONE_LINK_BANDWIDTH = 40000000000.0  # 40 Gbps
BACKBONE_LINK_BANDWIDTH = 10000000000.0  # 10 Gbps
BACKBONE_LINK_DELAY = 0.005  # 5ms in sec
LAN_LINK_RATE = 25000000.0  # 25 Mbps
NUMBER_CHANNELS = 200
EXPAND_INTERVAL = 1  # seconds

MIN_PBK_TIME = 60.0     # sec
MOD_PBK_TIME = 1800.0   # sec
MAX_PBK_TIME = 2700.0   # sec
MEAN_PBK_TIME = (MIN_PBK_TIME + MOD_PBK_TIME + MAX_PBK_TIME) / 3  # sec
# MEAN_PBK_TIME is valid for triangular distribution

STREAM_NORMAL = 0
STREAM_NOISE = 1
STREAM_CACHE = 2

MODEL_USER_BEHAVIOR = True
LOCAL_CACHE_ONLY = True

globalStreamID = 0
globalNoiseStreamID = 0
globalCacheStreamID = 0
globalEventID = 0
globalCacheID = 1000000


class event:
    __slots__ = ['time', 'objRef', 'type', 'id']

    def __init__(self, time, objRef, typ):
        self.time = time
        self.objRef = objRef
        self.type = typ
        global globalEventID
        self.id = globalEventID
        globalEventID += 1
        return

    def __lt__(self, other):
        return (self.time, self.id) < (other.time, other.id)

    def __ge__(self, other):
        return (self.time, self.id) > (other.time, other.id)


class netLink:

    def __init__(self, sim, ca, as_nodeA, as_nodeB):
        self.capacity = float(ca)
        self.netDataStreams = []
        self.as_nodeA = as_nodeA
        self.as_nodeB = as_nodeB
        self.simRef = sim
        return

    def __str__(self):
        s = 'netLink: ' + str(self.as_nodeA) + '-' + str(self.as_nodeB) +\
            ', capacity=' + str(self.capacity) +\
            ', capacityLeft=' + str(self.getCapacityLeft()) +\
            ', occupied by ' + str(len(self.netDataStreams)) + ' streams'
        return s

    def getCapacityLeft(self):
        capacityLeft = self.capacity
        for s in self.netDataStreams:
            capacityLeft -= s.transmitRate
        return capacityLeft

    def getHopsTo(self, link):
        assert link != self
        path = nx.shortest_path(
            self.simRef.urRef.gnGraph.netGraph,
            self.as_nodeA,
            link.as_nodeA
        )
        path.remove(self.as_nodeA)
        path.remove(link.as_nodeA)
        if self.as_nodeB in path:
            path.remove(self.as_nodeB)
        if link.as_nodeB in path:
            path.remove(link.as_nodeB)
        return len(path) + 1

    def getFairThroughput(self, nNewStreams):
        res = self.capacity
        nStreams = len(self.netDataStreams) + nNewStreams
        if len(self.netDataStreams) > 0:
            share = self.capacity / nStreams
            nExcludeStreams = 0
            for s in self.netDataStreams:
                if s.bottleneckLink != self and s.transmitRate < share:
                    nExcludeStreams += 1
                    res -= s.transmitRate
            if nExcludeStreams != nStreams:
                res /= (nStreams - nExcludeStreams)
        return res

    def allocateBandwidthForNewStream(self, curTime, newTR):
        for s in self.netDataStreams:
            if newTR < s.transmitRate:
                s.setTransmitRate(newTR, curTime)
        return

    def process(self, ev):
        # nothing
        return


class cacheNode:

    def __init__(self, sim, gnGraph, ASNum):
        global globalCacheID
        self.id = globalCacheID
        globalCacheID += 1
        self.ASnum = ASNum
        self.simRef = sim
        self.gnGraph = gnGraph
        self.ready = False
        self.waitingStreams = []
        # listStreamsPerChannelPerRate[STREAM_RATE][CHANNEL_NUMBER][STREAM]
        self.cacheStrs = [None] * len(STREAM_RATES) * NUMBER_CHANNELS
        self.lstStrs_Cnl_Rate = [None] * len(STREAM_RATES)
        for j in range(len(STREAM_RATES)):
            self.lstStrs_Cnl_Rate[j] = dict()
        return

    def attachNetDataStream(self, stream, curTime):
        if self.ready:
            # attach a stream to the cache instance:
            # a cache stream is created, 'stream' is added as dependent stream
            sRateID = STREAM_RATES.index(stream.consumeRate)
            if stream.channel in self.lstStrs_Cnl_Rate[sRateID]:
                # we have channel with this rate in cache
                self.lstStrs_Cnl_Rate[sRateID][stream.channel].append(
                    stream
                )
            else:
                self.lstStrs_Cnl_Rate[sRateID][stream.channel] = [stream]
            if self.cacheStrs[sRateID * NUMBER_CHANNELS + stream.channel] is \
                    None:
                # FIXME: use ip-address as the dest ip instead...
                cSt = netDataStream(
                    self.simRef,
                    stream.consumeRate,
                    stream.srcIP,
                    'cache@'+str(self.ASnum),
                    0,
                    stream.channel,
                    STREAM_CACHE
                )
                cSt.downCacheRef = self
                path = nx.shortest_path(
                    self.gnGraph.netGraph,
                    self.ASnum,
                    self.gnGraph.ip2as[cSt.srcIP]
                )
                if self.simRef.topArgs.hierarchical:
                    # in case of hierarchical caches,
                    # on-demand instantiations are not allowed -> 'first=False'
                    self.simRef.urRef.routeStreamPath_inclCache(
                        path,
                        cSt,
                        curTime,
                        first=False
                    )
                else:
                    self.simRef.urRef.routeStreamPath(path, cSt, curTime)
                self.cacheStrs[sRateID * NUMBER_CHANNELS + stream.channel] = cSt
            else:
                cSt = self.cacheStrs[sRateID * NUMBER_CHANNELS + stream.channel]
                if cSt.beingConsumed:
                    self.simRef.eventPush(
                        event(
                            curTime + PROPAGATION_DELAY,
                            stream,
                            EVENT_STREAM_START
                        )
                    )
        else:
            if self.simRef.topArgs.waitCacheBoot:
                self.waitingStreams.append(stream)
            else:
                return False
        if not stream.connectedToCache:
            stream.upCacheRef = self
            stream.connectedToCache = True
        return True

    def detachNetDataStream(self, stream, curTime):
        sRateID = STREAM_RATES.index(stream.consumeRate)
        self.lstStrs_Cnl_Rate[sRateID][stream.channel].remove(stream)
        stream.upCacheRef = None
        if len(self.lstStrs_Cnl_Rate[sRateID][stream.channel]) == 0:
            cSt = self.cacheStrs[sRateID * NUMBER_CHANNELS + stream.channel]
            self.cacheStrs[sRateID * NUMBER_CHANNELS + stream.channel] = None
            cEv = event(curTime, cSt, EVENT_STREAM_COMPLETED)
            cSt.process(cEv)
            if 'static_cache' not in self.gnGraph.netGraph.node[self.ASnum]:
                deleteCache = True
                for sr in range(len(STREAM_RATES)):
                    if self.cacheStrs[sr * NUMBER_CHANNELS + stream.channel] \
                            is not None:
                        deleteCache = False
                        break
                if deleteCache:
                    self.gnGraph.netGraph.remove_node(self.id)
                    # delete old cache node not to crowd up the topology
                    self.gnGraph.netGraph.\
                        node[self.ASnum]['caches'][stream.channel] = None
                    self.gnGraph.netGraph.\
                        node[self.ASnum]['nCacheRequests'][stream.channel] = 0
        return

    def startDependentStraems(self, cacheStream, curTime):
        cacheStream.updateCounters(curTime)
        cacheStream.beingConsumed = True
        cacheStream.consumePoint = curTime
        channel = cacheStream.channel
        sRateID = STREAM_RATES.index(cacheStream.consumeRate)
        for stream in self.lstStrs_Cnl_Rate[sRateID][channel]:
            if not stream.beingTransmitted:
                self.simRef.eventPush(
                    event(
                        curTime + PROPAGATION_DELAY,
                        stream,
                        EVENT_STREAM_START
                    )
                )
        return

    def getParentCacheStreamTransmitRate(self, stream):
        channel = stream.channel
        sRateID = STREAM_RATES.index(stream.consumeRate)
        cSt = self.cacheStrs[sRateID * NUMBER_CHANNELS + channel]
        return cSt.transmitRate

    def getParentCacheStreamBufferSize(self, stream, curTime):
        channel = stream.channel
        sRateID = STREAM_RATES.index(stream.consumeRate)
        cSt = self.cacheStrs[sRateID * NUMBER_CHANNELS + channel]
        cSt.updateCounters(curTime)
        inBuffer = float(cSt.downloadedBit - cSt.consumedBit)
        return inBuffer

    def updateDependentStreams(self, cacheStream, curTime):
        channel = cacheStream.channel
        sRateID = STREAM_RATES.index(cacheStream.consumeRate)
        for stream in self.lstStrs_Cnl_Rate[sRateID][channel]:
            if stream.transmitingLive:
                stream.tryUseMaxTRate(curTime)
        return

    def process(self, ev):
        if ev.type == EVENT_CACHE_READY:
            self.ready = True
            for s in self.waitingStreams:
                self.attachNetDataStream(s, ev.time)
            self.waitingStreams = []
        else:
            raise Exception("Unknown event type:" + str(ev.type))
        return


class netDataStream:
    def __init__(self, sim, cr, sip, dip, s, cnl=None, strType=STREAM_NORMAL):
        self.downloadedBit = 0
        self.sizeBit = s
        self.transmitRate = 0
        self.transmitPoint = None
        self.consumeRate = float(cr)
        self.srcIP = sip
        self.dstIP = dip
        self.links = []
        self.bottleneckLink = None
        self.simRef = sim
        self.id = 0
        self.beingConsumed = False
        self.beingTransmitted = False
        self.consumePoint = 0
        self.consumedBit = 0
        self.bufferingBegin = 0.0
        self.eventRef_trComplete = None
        self.eventRef_consBegin = None
        self.eventRef_consComplete = None
        self.eventRef_bufferEmpty = None
        self.eventRef_expand = None
        self.eventRef_toLiveTRate = None
        self.stats_startTime = None
        self.stats_bufferingTime = 0.0
        self.stats_bufferingEvents = 0
        self.stats_bitRates = []
        self.collectBitrateStats = False
        self.stats_lastTransmitRate_time = 0
        self.stats_transmitRate_sumRates = 0
        self.stats_transmitRate_sumTime = 0
        self.interestingResult = False
        self.stats_events = []
        self.streamType = strType
        if strType == STREAM_NORMAL or strType == STREAM_NOISE:
            self.links.append(netLink(sim, LAN_LINK_RATE, None, None))
        self.channel = cnl
        self.connectedToCache = False   # true when a stream is getting the data
        # from a cache node
        self.upCacheRef = None          # link to the upped level cache, the one
        # from which the stream gets its data
        self.downCacheRef = None        # link to the lower level cache, used to
        # enable cache hierarchy
        self.transmitingLive = False
        return

    def __del__(self):
        self.printStats()
        return

    def __str__(self):
        if self.streamType == STREAM_NORMAL:
            s = 'netDataStream-'
        elif self.streamType == STREAM_CACHE:
            s = 'netCacheStream-'
        elif self.streamType == STREAM_NOISE:
            s = 'netNoiseStream-'
        else:
            s = 'unknownStream-'
        s += str(self.id) + ' from: ' + self.srcIP +\
            (
                '(c' + str(len(self.links)) + ')'
                if self.connectedToCache
                else '(d' + str(len(self.links)) + ')'
            ) +\
            ', to: ' + self.dstIP + ', transmitRate: ' +\
            str(self.transmitRate) + 'b/s'
        return s

    def printStats(self):
        if self.streamType == STREAM_NORMAL:
            self.simRef.simulationStatistics.append(
                (self.streamType,
                 self.id,
                 self.channel,
                 self.stats_startTime,
                 self.stats_bufferingTime,
                 self.stats_bufferingEvents,
                 self.sizeBit / self.consumeRate,
                 self.getAvgTRate(),
                 self.consumeRate,
                 self.connectedToCache,
                 self.srcIP,
                 self.dstIP)
            )
        if self.streamType != STREAM_NORMAL:
            return
        s = 'stream-' + str(self.id) +\
            ' from: ' + self.srcIP + ', to: ' + self.dstIP + '\n' +\
            'start time: {:.2f}'.format(self.stats_startTime) +\
            ', buffering time: {:.2f}'.format(self.stats_bufferingTime) +\
            ', buffering events:' + str(self.stats_bufferingEvents) +\
            ', playback time: {:.2f}'.format(self.sizeBit / self.consumeRate) +\
            ', avg dwl-rate: {:.2f}'.format(self.getAvgTRate())
        # and draw a plot
        if (self.interestingResult and self.simRef.topArgs.figures)\
                or self.simRef.topArgs.allfigures:
            self.drawStreamingPlot(s)
        return

    def getAvgTRate(self):
        r = float(self.stats_transmitRate_sumRates) / \
            self.stats_transmitRate_sumTime
        return r

    def drawStreamingPlot(self, s):
        downStartX = downStopX = consStartX = consStopX = None
        buffStartX = buffStopX = None
        legendLines = set()
        for time, typ in self.stats_events:
            plt.plot(
                (time, time),
                (0, LAN_LINK_RATE),
                marker='.',
                mec='k',
                mew=0.25,
                ms=5,
                ls='-',
                lw=0.5,
                color=COLORS_EVENTS[typ],
                label=NAMES_EVENTS[typ] if typ not in legendLines else ''
            )
            if typ == EVENT_CONSUME_BEGIN and consStartX is None:
                consStartX = time
            elif typ == EVENT_CONSUME_BEGIN:
                buffStopX = time
            elif typ == EVENT_CONSUME_BUFFER_EMPTY:
                buffStartX = time
            elif typ == EVENT_CONSUME_COMPLETE:
                consStopX = time
            elif typ == EVENT_STREAM_START:
                downStartX = time
            elif typ == EVENT_STREAM_COMPLETED:
                downStopX = time
            if buffStartX is not None and buffStopX is not None:
                plt.plot(
                    (buffStartX, buffStopX),
                    (self.consumeRate, self.consumeRate),
                    color='r',
                    ls='-',
                    lw=5,
                    alpha=0.8,
                    solid_capstyle='butt',
                    label='Bufferring' if 'Bufferring' not in legendLines
                    else ''
                )
                buffStartX = None
                buffStopX = None
                legendLines.add('Bufferring')
            legendLines.add(typ)
        avgTRate = self.getAvgTRate()
        plt.plot(
            (downStartX, downStopX),
            (avgTRate, avgTRate),
            color='c',
            ls=':',
            lw=2,
            alpha=0.7,
            solid_capstyle='butt',
            label='Avg. TRate'
        )
        plt.plot(
            (consStartX, consStopX),
            (self.consumeRate, self.consumeRate),
            color='c',
            ls='-',
            lw=2,
            alpha=0.7,
            solid_capstyle='butt',
            label='Cons. rate'
        )
        x, y = zip(*self.stats_bitRates)
        plt.plot(x, y, lw=1, color='b')
        plt.legend(
            fontsize=7,
            bbox_to_anchor=(1, 1),
            numpoints=1,
            framealpha=0.7
        )
        plt.suptitle(s, fontsize=7)
        plt.ylabel('Bandwidth (b/s)', fontsize=7)
        plt.xlabel('Time (s)', fontsize=7)
        plt.yticks(
            range(
                0,
                int(LAN_LINK_RATE) + (int(LAN_LINK_RATE)/10),
                int(LAN_LINK_RATE) / 10)
        )
        plt.tick_params(axis='both', which='both', labelsize=5)
        plt.ticklabel_format(style='plain', useOffset=False)
        plt.minorticks_on()
        plt.grid(True)
        plt.savefig(self.simRef.simResDirName + '/fig_' + str(self.id) + '.pdf')
        plt.clf()
        return

    def updateCounters(self, curTime):
        if self.beingTransmitted:
            self.downloadedBit +=\
                (curTime - self.transmitPoint) * self.transmitRate
            self.transmitPoint = curTime
        if self.beingConsumed:
            if self.streamType == STREAM_CACHE \
                    and self.transmitingLive \
                    and self.consumeRate > self.transmitRate:
                self.consumedBit +=\
                    (curTime - self.consumePoint) * self.transmitRate
            else:
                self.consumedBit +=\
                    (curTime - self.consumePoint) * self.consumeRate
            self.consumePoint = curTime
        return

    def updateEvent_trComplete(self, curTime):
        if self.beingTransmitted \
                and self.streamType != STREAM_CACHE:
            expStreamingComplete = \
                curTime +\
                float(self.sizeBit - self.downloadedBit) / self.transmitRate
            if self.eventRef_trComplete is None:
                self.eventRef_trComplete = event(
                    expStreamingComplete,
                    self,
                    EVENT_STREAM_COMPLETED
                )
                self.simRef.eventPush(self.eventRef_trComplete)
            else:
                self.simRef.eventUpdateTime(
                    self.eventRef_trComplete,
                    expStreamingComplete
                )
        else:
            if self.eventRef_trComplete is not None:
                self.simRef.deleteEvent(self.eventRef_trComplete)
                self.eventRef_trComplete = None
        return

    def updateEvent_bufferEmpty(self, curTime):
        inBuffer = float(self.downloadedBit - self.consumedBit)
        if -1 < inBuffer < 1:
            inBuffer = 0.0
        if self.transmitRate < self.consumeRate and self.beingConsumed:
            # buffer will become empty
            timeLeft = self.calcBefferEmptyTime(
                inBuffer,
                self.transmitRate,
                self.consumeRate
            )
            if self.eventRef_bufferEmpty is None:
                self.eventRef_bufferEmpty = event(
                    curTime + timeLeft,
                    self,
                    EVENT_CONSUME_BUFFER_EMPTY
                )
                self.simRef.eventPush(self.eventRef_bufferEmpty)
            else:
                self.simRef.eventUpdateTime(
                    self.eventRef_bufferEmpty,
                    curTime +
                    timeLeft
                )
        elif self.eventRef_bufferEmpty is not None:
                # buffer will not become empty
                self.simRef.deleteEvent(self.eventRef_bufferEmpty)
                self.eventRef_bufferEmpty = None
        return

    def updateEvent_toLiveTRate(self, curTime):
        if not self.transmitingLive and self.beingTransmitted:
            if self.connectedToCache and self.upCacheRef is not None:
                cacheStreamBufferSize = \
                    self.upCacheRef.getParentCacheStreamBufferSize(
                        self, curTime
                    )
                timeTillSwitch = \
                    cacheStreamBufferSize / self.transmitRate + curTime
            else:
                bufferSize = self.consumeRate * self.simRef.topArgs.cachesec
                inBuffer = float(self.downloadedBit - self.consumedBit)
                if -1 < inBuffer < 1:
                    inBuffer = 0.0
                if self.streamType != STREAM_CACHE:
                    if bufferSize > inBuffer + \
                            (self.sizeBit - self.downloadedBit):
                        bufferSize = \
                            inBuffer + (self.sizeBit - self.downloadedBit)
                if bufferSize < inBuffer:
                    bufferSize = inBuffer
                timeTillSwitch = \
                    (bufferSize - inBuffer) / self.transmitRate + curTime
            if self.eventRef_toLiveTRate is None:
                self.eventRef_toLiveTRate = event(
                    timeTillSwitch,
                    self,
                    EVENT_SWITCH_TO_LIVERATE
                )
                self.simRef.eventPush(self.eventRef_toLiveTRate)
            else:
                self.simRef.eventUpdateTime(
                    self.eventRef_toLiveTRate,
                    timeTillSwitch
                )
        return

    def updateEvent_consumeBegin(self, curTime):
        # when data in buffer must be >= befferSize
        if self.beingConsumed:
            return
        bufferSize = self.consumeRate * self.simRef.topArgs.cachesec
        inBuffer = float(self.downloadedBit - self.consumedBit)
        if -1 < inBuffer < 1:
            inBuffer = 0.0
        if self.streamType != STREAM_CACHE:
            if bufferSize > inBuffer + (self.sizeBit - self.downloadedBit):
                bufferSize = inBuffer + (self.sizeBit - self.downloadedBit)
        if bufferSize < inBuffer:
            bufferSize = inBuffer
        if self.beingTransmitted:
            readyToPlayTime = \
                (bufferSize - inBuffer) / self.transmitRate + curTime
            if self.eventRef_consBegin is None:  # new event
                self.eventRef_consBegin = event(
                    readyToPlayTime,
                    self,
                    EVENT_CONSUME_BEGIN
                )
                self.simRef.eventPush(self.eventRef_consBegin)
            else:  # update old
                self.simRef.eventUpdateTime(
                    self.eventRef_consBegin,
                    readyToPlayTime
                )
        elif bufferSize == inBuffer and inBuffer > 0:
            if self.eventRef_consBegin is not None:
                self.simRef.eventUpdateTime(self.eventRef_consBegin, curTime)
        else:
            if self.eventRef_consBegin is not None:
                self.simRef.deleteEvent(self.eventRef_consBegin)
                self.eventRef_consBegin = None
        return

    def updateEvent_consumeComplete(self, curTime):
        # when we finish consuming the file (if no buff.empty occurs)
        if self.streamType == STREAM_CACHE:
            return
        if self.beingConsumed:  # need to update event consume complete
            duration = float(self.sizeBit - self.consumedBit) / self.consumeRate
            if self.eventRef_consComplete is None:
                self.eventRef_consComplete = event(
                    curTime + duration,
                    self,
                    EVENT_CONSUME_COMPLETE
                )
                self.simRef.eventPush(self.eventRef_consComplete)
            else:
                self.simRef.eventUpdateTime(
                    self.eventRef_consComplete,
                    curTime + duration
                )
        else:
            if self.eventRef_consComplete is not None:
                self.simRef.deleteEvent(self.eventRef_consComplete)
                self.eventRef_consComplete = None
        return

    def updateEvents(self, curTime):
        self.updateEvent_trComplete(curTime)
        if self.simRef.topArgs.streaming == 'live':
            self.updateEvent_toLiveTRate(curTime)
        if self.streamType != STREAM_NOISE:
            self.updateEvent_bufferEmpty(curTime)
            self.updateEvent_consumeBegin(curTime)
        if not self.beingTransmitted:
            if self.eventRef_expand is not None:
                self.simRef.deleteEvent(self.eventRef_expand)
                self.eventRef_expand = None
        return

    def calcBefferEmptyTime(self, buffSize, Vi, Vo):
        #   Vi -- download speed
        #   Vo -- playback speed
        #   Calculate the sum of the first N terms of a geometric series
        if Vi >= Vo:
            raise Exception("Series has no sum (diverges) -> "
                            "Buffer will not become empty")
        Vi = float(Vi)
        t0 = float(buffSize) / Vo
        if Vi > 0:
            accuracy = 0.001  # seconds
            b = Vi/Vo
            n = math.ceil(math.log(accuracy, b))
            sumN = t0 * (1.0 - math.pow((Vi/Vo), n)) / (1.0 - Vi/Vo)
        else:
            sumN = t0
        return sumN

    def startStreaming(self, curTime):
        if self.streamType == STREAM_NOISE:
            global globalNoiseStreamID
            self.id = globalNoiseStreamID
            globalNoiseStreamID += 1
        elif self.streamType == STREAM_CACHE:
            global globalCacheStreamID
            self.id = globalCacheStreamID
            globalCacheStreamID += 1
        elif self.streamType == STREAM_NORMAL:
            global globalStreamID
            self.id = globalStreamID
            globalStreamID += 1
        self.updateBottleneckLink(newStream=1)
        if self.simRef.simulatorReady:
            newTR = self.bottleneckLink.getFairThroughput(1)
            self.stats_lastTransmitRate_time = curTime
            self.setTransmitRate(newTR, curTime)
            for link in self.links:
                link.allocateBandwidthForNewStream(curTime, newTR)
                link.netDataStreams.append(self)
        else:
            # implementing simultaneous start of background noise streams
            # they are all placed onto the links, but have tRate = 0
            newTR = self.bottleneckLink.getFairThroughput(0)
            self.stats_lastTransmitRate_time = curTime
            self.setTransmitRate(newTR, curTime)
        self.eventRef_expand = event(
            curTime + EXPAND_INTERVAL,
            self,
            EVENT_STREAM_EXPAND
        )
        self.simRef.eventPush(self.eventRef_expand)
        return

    def setTransmitRate(self, newRate, curTime):
        if newRate != self.transmitRate:
            self.updateCounters(curTime)
            if self.collectBitrateStats:
                self.stats_bitRates.append((curTime, self.transmitRate))
                self.stats_bitRates.append((curTime, newRate))
            self.stats_transmitRate_sumRates += \
                (curTime - self.stats_lastTransmitRate_time) * self.transmitRate
            self.stats_transmitRate_sumTime += \
                (curTime - self.stats_lastTransmitRate_time)
            self.stats_lastTransmitRate_time = curTime
            self.transmitRate = newRate
            self.updateEvents(curTime)
            if self.streamType == STREAM_CACHE:
                self.downCacheRef.updateDependentStreams(self, curTime)
        return

    def tryUseMaxTRate(self, curTime):
        tr = self.updateBottleneckLink()
        if self.transmitingLive:
            if self.connectedToCache and self.upCacheRef is not None:
                cacheStreamTRate = \
                    self.upCacheRef.getParentCacheStreamTransmitRate(self)
            else:
                cacheStreamTRate = self.consumeRate
            if cacheStreamTRate < tr:
                tr = cacheStreamTRate
        if self.transmitRate != tr:
            self.setTransmitRate(tr, curTime)
        return

    def updateBottleneckLink(self, newStream=0):
        self.bottleneckLink = self.links[0]
        minThroughput = self.bottleneckLink.getFairThroughput(newStream)
        for l in self.links:
            tempRateVal = l.getFairThroughput(newStream)
            if tempRateVal < minThroughput:
                minThroughput = tempRateVal
                self.bottleneckLink = l
        return minThroughput

    def process(self, ev):
        if ev.type == EVENT_STREAM_START:
            self.beingTransmitted = True
            self.transmitPoint = ev.time
            self.startStreaming(ev.time)

        elif ev.type == EVENT_STREAM_COMPLETED:
            self.updateCounters(ev.time)
            self.beingTransmitted = False
            self.eventRef_trComplete = None
            self.setTransmitRate(0, ev.time)
            for link in self.links:
                link.netDataStreams.remove(self)
            if self.connectedToCache:
                self.upCacheRef.detachNetDataStream(self, ev.time)
            if self.streamType == STREAM_NOISE:
                self.simRef.urRef.activeNoiseStreams -= 1
                if self.simRef.simulatorReady \
                        and not self.simRef.simulationDone:
                    newEv = self.simRef.urRef.getNoiseEvent(ev.time)
                    self.simRef.eventPush(newEv)
            elif self.streamType == STREAM_NORMAL:
                self.simRef.urRef.activeStreams -= 1
            if not self.simRef.urRef.streamGenActive \
                    and (self.simRef.urRef.activeStreams == 0
                         and self.simRef.simulatorReady):
                self.simRef.simulationDone = True

        elif ev.type == EVENT_STREAM_EXPAND:
            if self.beingTransmitted:
                self.tryUseMaxTRate(ev.time)
                # try to expand every second
                self.eventRef_expand = event(
                    ev.time + EXPAND_INTERVAL,
                    self,
                    EVENT_STREAM_EXPAND
                )
                self.simRef.eventPush(self.eventRef_expand)
            else:
                self.eventRef_expand = None
            return
            # don't let Expand event register in the stream stats

        elif ev.type == EVENT_CONSUME_BEGIN:
            self.eventRef_consBegin = None
            self.updateCounters(ev.time)
            self.beingConsumed = True
            self.consumePoint = ev.time
            self.updateEvent_consumeComplete(ev.time)
            self.updateEvent_bufferEmpty(ev.time)
            if self.streamType == STREAM_CACHE:
                self.downCacheRef.startDependentStraems(self, ev.time)
            # statistics
            if self.stats_startTime is None:
                self.stats_startTime = ev.time - self.bufferingBegin
            if self.bufferingBegin != 0:
                self.stats_bufferingTime += ev.time - self.bufferingBegin
                self.bufferingBegin = 0

        elif ev.type == EVENT_SWITCH_TO_LIVERATE:
            self.transmitingLive = True
            self.eventRef_toLiveTRate = None
            self.tryUseMaxTRate(ev.time)
            return
            # don't let 'Switch to liveRate' event register in the stream stats

        elif ev.type == EVENT_CONSUME_COMPLETE:
            self.updateCounters(ev.time)
            self.beingConsumed = False
            self.eventRef_consComplete = None
            self.updateEvents(ev.time)

        elif ev.type == EVENT_CONSUME_BUFFER_EMPTY:
            self.eventRef_bufferEmpty = None
            self.updateCounters(ev.time)
            if self.beingConsumed:
                if self.streamType != STREAM_CACHE:
                    # the cache stream continues sending
                    self.beingConsumed = False
                self.bufferingBegin = ev.time
                self.updateEvent_consumeBegin(ev.time)
                if self.beingTransmitted:
                    self.updateEvent_consumeComplete(ev.time)
                    # statistics
                    self.stats_bufferingEvents += 1
                    if self.collectBitrateStats:
                        self.interestingResult = True
        else:
            raise Exception("Unknown event type: " + str(ev.type))
        if self.streamType != STREAM_NOISE:
            self.stats_events.append((ev.time, ev.type))
        return


class userRequests:
    def __init__(self, sim, fName, gnGraph, listHosts, maxHosts,
                 maxActiveStreams):
        self.re = re.compile(
            '(\d+\.\d+\.\d+\.\d+)\s(\S+)\s(\d+)'
            '\s(\d+\.\d+)\s(\d+\.\d+)\s(\d+\.\d+)\s(\d+)',
            re.UNICODE
        )
        self.requestQueue = Queue.Queue()
        self.noiseRequestQueue = Queue.Queue()
        self.gnGraph = gnGraph
        self.listOfHosts = listHosts
        self.traceHostMap = dict()
        self.maxHosts = maxHosts  # total number of hosts
        self.simRef = sim
        self.activeStreams = 0
        self.totalStreams = 0
        self.activeStreamsMax = maxActiveStreams
        self.streamGenerationRate = self.calcStreamGenRate(sim.topArgs.reqRate)
        self.streamGenRate_next = 0
        self.streamGenActive = True
        self.activeNoiseStreams = 0
        self.totalNoiseStreams = 0
        self.activeNoiseStreamsMax = int(sim.topArgs.backnoise)
        self.startTime = None
        self.initStreamsList = []
        self.listOfChannels = None
        self.numRequestsPerTimePeriod = 0
        self.streamGenRateScenario = []  # (time, requests per min)
        if sim.topArgs.scenario != '':
            if os.path.isfile(sim.topArgs.scenario):
                print("\tUsing a scenaio file: " + sim.topArgs.scenario)
                with open(sim.topArgs.scenario, 'rb') as csvfile:
                    reader = csv.reader(csvfile)
                    for row in reader:
                        time, rate = row
                        self.streamGenRateScenario.append(
                            (float(time), float(rate))
                        )
            else:
                print("\tspecified scenaio file not found: " +
                      sim.topArgs.scenario)
                exit(-3)
        if MODEL_USER_BEHAVIOR is True:
            self.startTime = 0.0
            self.traceFile = None
            for t, r in self.streamGenRateScenario:
                self.simRef.eventPush(
                    event(t, self, EVENT_CHANGE_REQUEST_RATE)
                )
            self.simRef.eventPush(
                event(sim.topArgs.endtime, self, EVENT_SIM_FINALIZE)
            )
        else:
            self.traceFile = open(fName, 'r')
        self.simRef.eventPush(event(1, self, EVENT_PERIODIC_STATS))
        return

    def __del__(self):
        self.traceFile.close()

    def calcStreamGenRate(self, userRequest=0.0):
        autoCalcRate = float(self.activeStreamsMax) / MEAN_PBK_TIME
        if userRequest == 0.0:
            result = autoCalcRate
        else:
            result = float(userRequest) / 60
            if result < autoCalcRate:
                print(
                    "\n\tinfo: given reqRate (" + str(60 * result) +
                    ") is too small to guarantee " +
                    str(self.simRef.topArgs.active) +
                    " active connections. Try reqRate = " +
                    str(60 * autoCalcRate)
                )
            elif result > autoCalcRate:
                print(
                    "\n\tinfo: given reqRate (" + str(60 * result) +
                    ") is too high. Number active connections (" +
                    str(self.simRef.topArgs.active) +
                    ") will be exceeded. Try reqRate = " +
                    str(60 * autoCalcRate)
                )
        return result

    def genChannelNumber(self):
        channel = numpy.random.zipf(1.2) - 1
        while channel >= NUMBER_CHANNELS:
            channel = numpy.random.zipf(1.2) - 1
        return channel

    def getNextEvent(self, curTime):
        if MODEL_USER_BEHAVIOR:
            self.totalStreams += 1
            randHost = random.choice(self.listOfHosts).exploded
            randStartTime = curTime + numpy.random.\
                standard_gamma(1.0/self.streamGenerationRate)
            randPlayTime = numpy.random.\
                triangular(MIN_PBK_TIME, MOD_PBK_TIME, MAX_PBK_TIME)
            rateN = numpy.random.poisson(2)
            while rateN > len(STREAM_RATES) - 1:
                rateN = numpy.random.poisson(2)
            randStreamRate = STREAM_RATES[rateN]
            futureRequest = (
                randHost,
                randStreamRate,
                randStreamRate * randPlayTime
            )
            ev = event(randStartTime, self, EVENT_USER_REQUEST)
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
                    randHost = random.choice(self.listOfHosts).exploded
                    self.traceHostMap[match.group(1)] = randHost
                else:
                    randHost = self.traceHostMap[match.group(1)]
                futureRequest = (
                    randHost,
                    STREAM_RATES[2],
                    float(match.group(7))
                )
                ev = event(
                    float(match.group(4)) - self.startTime,
                    self,
                    EVENT_USER_REQUEST
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
        randHost = random.choice(self.listOfHosts).exploded
        randStartTime = curTime + numpy.random.\
            standard_gamma(MEAN_PBK_TIME/self.activeNoiseStreamsMax)
        randPlayTime = numpy.random.triangular(600, 1800, 3600)
        randStreamRate = STREAM_RATES[int(
            numpy.random.triangular(
                -1, len(STREAM_RATES) / 2, len(STREAM_RATES)
            ))]
        futureNoiseRequest = \
            (randHost, randStreamRate, randPlayTime * randStreamRate)
        self.noiseRequestQueue.put(futureNoiseRequest)
        ev = event(randStartTime, self, EVENT_NOISE_USER_REQUEST)
        return ev

    def routeStreamPath(self, path, s, curTime):
        nodeA = path[0]
        for nodeB in path[1:]:
            if 'p2p_link' not in self.gnGraph.netGraph[nodeA][nodeB]:
                if self.gnGraph.isAccessNode(
                        self.gnGraph.netGraph.node[nodeA]['type']
                ) or self.gnGraph.isAccessNode(
                    self.gnGraph.netGraph.node[nodeB]['type']
                ):
                    self.gnGraph.netGraph[nodeA][nodeB]['p2p_link'] =\
                        netLink(
                            self.simRef,
                            BACKBONE_LINK_BANDWIDTH,
                            nodeA,
                            nodeB
                        )
                else:
                    self.gnGraph.netGraph[nodeA][nodeB]['p2p_link'] =\
                        netLink(
                            self.simRef,
                            FAST_BACKBONE_LINK_BANDWIDTH,
                            nodeA,
                            nodeB
                        )
            s.links.append(self.gnGraph.netGraph[nodeA][nodeB]['p2p_link'])
            nodeA = nodeB
        if s.streamType == STREAM_NOISE and not self.simRef.simulatorReady:
            for l in s.links:
                l.netDataStreams.append(s)
            self.initStreamsList.append(s)
        else:
            self.simRef.eventPush(
                event(
                    curTime + PROPAGATION_DELAY*len(path),
                    s,
                    EVENT_STREAM_START
                )
            )
        return

    def addCacheToAS(self, ASn, curTime, channelNum, static=False):
        if 'caches' not in self.gnGraph.netGraph.node[ASn]:
            self.gnGraph.netGraph.\
                node[ASn]['caches'] = [None] * NUMBER_CHANNELS
            # 1 vm per channel (all str.Rates)
        if self.gnGraph.netGraph.node[ASn]['caches'][channelNum] is None:
            cache = cacheNode(self.simRef, self.gnGraph, ASn)
            assert cache.id not in self.gnGraph.netGraph
            self.gnGraph.netGraph.add_edge(ASn, cache.id)
            self.gnGraph.netGraph.node[ASn]['caches'][channelNum] = cache
            if static:
                cache.process(
                    event(
                        curTime,
                        cache,
                        EVENT_CACHE_READY
                    )
                )
            else:
                self.simRef.eventPush(
                    event(
                        curTime + self.simRef.topArgs.cacheinit,
                        cache,
                        EVENT_CACHE_READY
                    )
                )
        else:
            cache = self.gnGraph.netGraph.node[ASn]['caches'][channelNum]
        return cache

    def routeStreamPath_inclCache(self, path, s, curTime, first=True):
        cacheOnDemand = self.simRef.topArgs.ondemandCache
        nodeA = path[0]
        for nodeB in path[1:]:
            # Creating a link between node A and B, if it does not exist yet
            if 'p2p_link' not in self.gnGraph.netGraph[nodeA][nodeB]:
                # if one of the nodes is an 'access' AS node then the link
                # speed is set to BACKBONE_LINK_BANDWIDTH
                if self.gnGraph.isAccessNode(
                        self.gnGraph.netGraph.node[nodeA]['type']
                ) or self.gnGraph.isAccessNode(
                    self.gnGraph.netGraph.node[nodeB]['type']
                ):
                    self.gnGraph.netGraph[nodeA][nodeB]['p2p_link'] = \
                        netLink(
                            self.simRef,
                            BACKBONE_LINK_BANDWIDTH,
                            nodeA,
                            nodeB
                        )
                else:
                    self.gnGraph.netGraph[nodeA][nodeB]['p2p_link'] =\
                        netLink(
                            self.simRef,
                            FAST_BACKBONE_LINK_BANDWIDTH,
                            nodeA,
                            nodeB
                        )
            if nodeA == path[0] or not LOCAL_CACHE_ONLY:
                # increase the cache-init counter and check the threshold
                if 'nCacheRequests' not in self.gnGraph.netGraph.node[nodeA]:
                    self.gnGraph.netGraph.\
                        node[nodeA]['nCacheRequests'] = [0] * NUMBER_CHANNELS
                self.gnGraph.netGraph.\
                    node[nodeA]['nCacheRequests'][s.channel] += 1
                if self.gnGraph.netGraph.\
                        node[nodeA]['nCacheRequests'][s.channel] >= \
                        self.simRef.topArgs.cachethreshold:
                    # threshold passed, add a cache
                    # (all checks are inside the 'addCacheToAS')
                    cache = None
                    if 'static_cache' in self.gnGraph.netGraph.node[nodeA]:
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
                s.links.append(self.gnGraph.netGraph[nodeA][nodeB]['p2p_link'])
            nodeA = nodeB
        # background noise streams: adding stream to all links along
        # the path at init time
        if not self.simRef.simulatorReady and s.streamType == STREAM_NOISE:
            for l in s.links:
                l.netDataStreams.append(s)
            self.initStreamsList.append(s)
        else:
            # schedule 'start streaming' events
            if not s.connectedToCache:
                self.simRef.eventPush(
                    event(
                        curTime + PROPAGATION_DELAY*len(path),
                        s,
                        EVENT_STREAM_START
                    )
                )
        return

    def process(self, ev):
        if ev.type == EVENT_USER_REQUEST:
            dest_ip, stream_rate, data_size = self.requestQueue.get()
            hostAs = self.gnGraph.ip2as[dest_ip]
            path = nx.shortest_path(
                self.gnGraph.netGraph,
                hostAs,
                self.gnGraph.contentProvider
            )
            serv_ip = self.gnGraph.netGraph.\
                node[self.gnGraph.contentProvider]['ip'].exploded
            ds = netDataStream(
                self.simRef,
                stream_rate,
                serv_ip,
                dest_ip,
                data_size,
                self.genChannelNumber()
            )
            ds.bufferingBegin = ev.time
            if self.simRef.topArgs.streaming == 'live':
                self.routeStreamPath_inclCache(path, ds, ev.time)
            else:
                self.routeStreamPath(path, ds, ev.time)
            # statistics for user request
            ds.stats_events.append((ev.time, ev.type))
            self.activeStreams += 1
            self.numRequestsPerTimePeriod += 1
            if self.streamGenActive:
                self.simRef.eventPush(self.getNextEvent(ev.time))
        elif ev.type == EVENT_NOISE_USER_REQUEST:
            dest_ip, stream_rate, data_size = self.noiseRequestQueue.get()
            hostAs = self.gnGraph.ip2as[dest_ip]
            servAs = random.choice(self.gnGraph.contentNodes)
            serv_ip = self.gnGraph.as2ip[servAs][0][1].exploded
            path = nx.shortest_path(self.gnGraph.netGraph, hostAs, servAs)
            ds = netDataStream(
                self.simRef,
                stream_rate,
                serv_ip,
                dest_ip,
                data_size,
                strType=STREAM_NOISE
            )
            self.routeStreamPath(path, ds, ev.time)
            if self.simRef.simulatorReady:
                self.simRef.eventPush(
                    event(
                        ev.time + PROPAGATION_DELAY*len(path),
                        ds,
                        EVENT_STREAM_START
                    )
                )
            else:
                self.initStreamsList.append(ds)
            self.activeNoiseStreams += 1
            if not self.simRef.simulationDone:
                if not self.simRef.simulatorReady:
                    self.simRef.eventPush(self.getNoiseEvent(ev.time))
                    if self.activeNoiseStreams >= self.activeNoiseStreamsMax:
                        for tmpStream in self.initStreamsList:
                            tmpStream.startStreaming(ev.time)
                            tmpStream.bufferingBegin = ev.time
                        self.initStreamsList = []
                        self.simRef.simulatorReady = True
                        self.streamGenActive = True
                        # start normal stream
                        self.simRef.eventPush(self.getNextEvent(ev.time))
        elif ev.type == EVENT_CHANGE_REQUEST_RATE:
            self.streamGenerationRate = self.calcStreamGenRate(
                self.streamGenRateScenario[self.streamGenRate_next][1]
            )
            self.streamGenRate_next += 1
        elif ev.type == EVENT_SIM_FINALIZE:
            print("\n{:.2f}".format(float(ev.time)) +
                  " sec. -- SIM_FINALIZE: no new streams")
            self.streamGenActive = False
        elif ev.type == EVENT_PERIODIC_STATS:
            self.simRef.urStatistics_nActCons.append(
                (ev.time, self.activeStreams)
            )
            reqPerSec = float(self.numRequestsPerTimePeriod) / 10 * 60
            self.simRef.urStatistics_nReqPSec.append((ev.time, reqPerSec))
            self.numRequestsPerTimePeriod = 0
            if not self.simRef.simulationDone:
                self.simRef.eventPush(
                    event(
                        ev.time + 10,
                        self,
                        EVENT_PERIODIC_STATS
                    )
                )
            print(
                '\r{:.2f}'.format(float(ev.time)) +
                " sec. simulated. Active Streams = " +
                str(self.activeStreams), end=""
            )
            sys.stdout.flush()
        else:
            raise Exception("Unknown event type:" + str(ev.type))
        return
