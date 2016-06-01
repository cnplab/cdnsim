"""
    CDNSim

file: netDataStream.py

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
import matplotlib.pyplot as plt
import time
import math

import sim_globals as sg
import sim_event as se
import netLink as nl


class netDataStream:
    def __init__(self, cr, sip, dip, s, cnl=None, strType=sg.STREAM_NORMAL):
        self.transmitRate = 0
        self.bottleneckLinkID = None
        if cr is None:
            return
        #   fields up to this point are necessary for calcFairThroughput
        self.bottleneckLink = None
        self.useParallel = True
        self.tSeq = 0.0
        self.tParal = 0.0
        self.downloadedBit = 0
        self.sizeBit = s
        self.transmitPoint = None
        self.consumeRate = float(cr)
        self.srcIP = sip
        self.dstIP = dip
        self.links = []
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
        if strType == sg.STREAM_NORMAL or strType == sg.STREAM_NOISE:
            self.links.append(nl.netLink(sg.LAN_LINK_RATE, None, None))
        self.channel = cnl
        self.connectedToCache = False   # true when a stream is getting the data
        # from a cache node
        self.upCacheRef = None          # link to the upped level cache, the one
        # from which the stream gets its data
        self.downCacheRef = None        # link to the lower level cache, used to
        # enable cache hierarchy
        self.transmitingLive = False
        return

    def __str__(self):
        if self.streamType == sg.STREAM_NORMAL:
            s = 'netDataStream-'
        elif self.streamType == sg.STREAM_CACHE:
            s = 'netCacheStream-'
        elif self.streamType == sg.STREAM_NOISE:
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

    def __getstate__(self):
        return self.bottleneckLink.id, self.transmitRate

    def __setstate__(self, (bn_id, tr)):
        self.__init__(None, None, None, None)
        self.bottleneckLinkID = bn_id
        self.transmitRate = tr

    def printStats(self):
        if self.streamType == sg.STREAM_NORMAL:
            sg.simRef.simulationStatistics.append(
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
        if self.streamType != sg.STREAM_NORMAL:
            return
        s = 'stream-' + str(self.id) +\
            ' from: ' + self.srcIP + ', to: ' + self.dstIP + '\n' +\
            'start time: {:.2f}'.format(self.stats_startTime) +\
            ', buffering time: {:.2f}'.format(self.stats_bufferingTime) +\
            ', buffering events:' + str(self.stats_bufferingEvents) +\
            ', playback time: {:.2f}'.format(self.sizeBit / self.consumeRate) +\
            ', avg dwl-rate: {:.2f}'.format(self.getAvgTRate())
        # and draw a plot
        if (self.interestingResult and sg.args.figures)\
                or sg.args.allfigures:
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
                (0, sg.LAN_LINK_RATE),
                marker='.',
                mec='k',
                mew=0.25,
                ms=5,
                ls='-',
                lw=0.5,
                color=sg.COLORS_EVENTS[typ],
                label=sg.NAMES_EVENTS[typ] if typ not in legendLines else ''
            )
            if typ == sg.EVENT_CONSUME_BEGIN and consStartX is None:
                consStartX = time
            elif typ == sg.EVENT_CONSUME_BEGIN:
                buffStopX = time
            elif typ == sg.EVENT_CONSUME_BUFFER_EMPTY:
                buffStartX = time
            elif typ == sg.EVENT_CONSUME_COMPLETE:
                consStopX = time
            elif typ == sg.EVENT_STREAM_START:
                downStartX = time
            elif typ == sg.EVENT_STREAM_COMPLETED:
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
                int(sg.LAN_LINK_RATE) + (int(sg.LAN_LINK_RATE)/10),
                int(sg.LAN_LINK_RATE) / 10)
        )
        plt.tick_params(axis='both', which='both', labelsize=5)
        plt.ticklabel_format(style='plain', useOffset=False)
        plt.minorticks_on()
        plt.grid(True)
        plt.savefig(sg.simRef.simResDirName + '/fig_' + str(self.id) + '.pdf')
        plt.clf()
        return

    def updateCounters(self, curTime):
        if self.beingTransmitted:
            self.downloadedBit +=\
                (curTime - self.transmitPoint) * self.transmitRate
            self.transmitPoint = curTime
        if self.beingConsumed:
            if self.streamType == sg.STREAM_CACHE \
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
                and self.streamType != sg.STREAM_CACHE:
            expStreamingComplete = \
                curTime +\
                float(self.sizeBit - self.downloadedBit) / self.transmitRate
            if self.eventRef_trComplete is None:
                self.eventRef_trComplete = se.event(
                    expStreamingComplete,
                    id(self),
                    sg.EVENT_STREAM_COMPLETED,
                    self
                )
                sg.simRef.eventPush(self.eventRef_trComplete)
            else:
                sg.simRef.eventUpdateTime(
                    self.eventRef_trComplete,
                    expStreamingComplete
                )
        else:
            if self.eventRef_trComplete is not None:
                sg.simRef.deleteEvent(self.eventRef_trComplete)
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
                self.eventRef_bufferEmpty = se.event(
                    curTime + timeLeft,
                    id(self),
                    sg.EVENT_CONSUME_BUFFER_EMPTY,
                    self
                )
                sg.simRef.eventPush(self.eventRef_bufferEmpty)
            else:
                sg.simRef.eventUpdateTime(
                    self.eventRef_bufferEmpty,
                    curTime +
                    timeLeft
                )
        elif self.eventRef_bufferEmpty is not None:
                # buffer will not become empty
                sg.simRef.deleteEvent(self.eventRef_bufferEmpty)
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
                bufferSize = self.consumeRate * sg.args.cachesec
                inBuffer = float(self.downloadedBit - self.consumedBit)
                if -1 < inBuffer < 1:
                    inBuffer = 0.0
                if self.streamType != sg.STREAM_CACHE:
                    if bufferSize > inBuffer + \
                            (self.sizeBit - self.downloadedBit):
                        bufferSize = \
                            inBuffer + (self.sizeBit - self.downloadedBit)
                if bufferSize < inBuffer:
                    bufferSize = inBuffer
                timeTillSwitch = \
                    (bufferSize - inBuffer) / self.transmitRate + curTime
            if self.eventRef_toLiveTRate is None:
                self.eventRef_toLiveTRate = se.event(
                    timeTillSwitch,
                    id(self),
                    sg.EVENT_SWITCH_TO_LIVERATE,
                    self
                )
                sg.simRef.eventPush(self.eventRef_toLiveTRate)
            else:
                sg.simRef.eventUpdateTime(
                    self.eventRef_toLiveTRate,
                    timeTillSwitch
                )
        return

    def updateEvent_consumeBegin(self, curTime):
        # when data in buffer must be >= befferSize
        if self.beingConsumed:
            return
        bufferSize = self.consumeRate * sg.args.cachesec
        inBuffer = float(self.downloadedBit - self.consumedBit)
        if -1 < inBuffer < 1:
            inBuffer = 0.0
        if self.streamType != sg.STREAM_CACHE:
            if bufferSize > inBuffer + (self.sizeBit - self.downloadedBit):
                bufferSize = inBuffer + (self.sizeBit - self.downloadedBit)
        if bufferSize < inBuffer:
            bufferSize = inBuffer
        if self.beingTransmitted:
            readyToPlayTime = \
                (bufferSize - inBuffer) / self.transmitRate + curTime
            if self.eventRef_consBegin is None:  # new event
                self.eventRef_consBegin = se.event(
                    readyToPlayTime,
                    id(self),
                    sg.EVENT_CONSUME_BEGIN,
                    self
                )
                sg.simRef.eventPush(self.eventRef_consBegin)
            else:  # update old
                sg.simRef.eventUpdateTime(
                    self.eventRef_consBegin,
                    readyToPlayTime
                )
        elif bufferSize == inBuffer and inBuffer > 0:
            if self.eventRef_consBegin is not None:
                sg.simRef.eventUpdateTime(self.eventRef_consBegin, curTime)
        else:
            if self.eventRef_consBegin is not None:
                sg.simRef.deleteEvent(self.eventRef_consBegin)
                self.eventRef_consBegin = None
        return

    def updateEvent_consumeComplete(self, curTime):
        # when we finish consuming the file (if no buff.empty occurs)
        if self.streamType == sg.STREAM_CACHE:
            return
        if self.beingConsumed:  # need to update event consume complete
            duration = float(self.sizeBit - self.consumedBit) / self.consumeRate
            if self.eventRef_consComplete is None:
                self.eventRef_consComplete = se.event(
                    curTime + duration,
                    id(self),
                    sg.EVENT_CONSUME_COMPLETE,
                    self
                )
                sg.simRef.eventPush(self.eventRef_consComplete)
            else:
                sg.simRef.eventUpdateTime(
                    self.eventRef_consComplete,
                    curTime + duration
                )
        else:
            if self.eventRef_consComplete is not None:
                sg.simRef.deleteEvent(self.eventRef_consComplete)
                self.eventRef_consComplete = None
        return

    def updateEvents(self, curTime):
        self.updateEvent_trComplete(curTime)
        if sg.args.streaming:
            self.updateEvent_toLiveTRate(curTime)
        if self.streamType != sg.STREAM_NOISE:
            self.updateEvent_bufferEmpty(curTime)
            self.updateEvent_consumeBegin(curTime)
        if not self.beingTransmitted:
            if self.eventRef_expand is not None:
                sg.simRef.deleteEvent(self.eventRef_expand)
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
        if self.streamType == sg.STREAM_NOISE:
            self.id = sg.globalNoiseStreamID
            sg.globalNoiseStreamID += 1
        elif self.streamType == sg.STREAM_CACHE:
            self.id = sg.globalCacheStreamID
            sg.globalCacheStreamID += 1
        elif self.streamType == sg.STREAM_NORMAL:
            self.id = sg.globalStreamID
            sg.globalStreamID += 1
        self.updateBottleneckLink(newStream=1)
        if sg.simRef.simulatorReady:
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
        self.eventRef_expand = se.event(
            curTime + sg.EXPAND_INTERVAL,
            id(self),
            sg.EVENT_STREAM_EXPAND,
            self
        )
        sg.simRef.eventPush(self.eventRef_expand)
        return

    def setTransmitRate(self, newRate, curTime):
        if newRate != self.transmitRate:
            old_rate = self.transmitRate
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
            if self.streamType == sg.STREAM_CACHE:
                self.downCacheRef.updateDependentStreams(self, curTime)
            if self.connectedToCache and self.upCacheRef is not None:
                self.upCacheRef.updateThroughputStats(old_rate, newRate)
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
        minThroughput = self.links[0].getFairThroughput(newStream)
        self.bottleneckLink = self.links[0]
        if sg.args.parallel and self.useParallel:
            t1 = time.time()
            data = ((link, self.links.index(link), newStream) for link in self.links)
            for thr, link_index in sg.simRef.calcFT_pool.imap_unordered(
                    sg.calcFairThroughput, data):
                if thr < minThroughput:
                    minThroughput = thr
                    self.bottleneckLink = self.links[link_index]
            tdur = time.time() - t1
            if tdur > self.tSeq:
                self.useParallel = False
                self.tParal = tdur
            else:
                self.tSeq *= 0.9
        elif sg.args.parallel:
            t1 = time.time()
            for l in self.links:
                tempRateVal = l.getFairThroughput(newStream)
                if tempRateVal < minThroughput:
                    minThroughput = tempRateVal
                    self.bottleneckLink = l
            tdur = time.time() - t1
            if tdur > self.tParal:
                self.useParallel = True
                self.tSeq = tdur
            else:
                self.tParal *= 0.9
        else:
            for l in self.links:
                tempRateVal = l.getFairThroughput(newStream)
                if tempRateVal < minThroughput:
                    minThroughput = tempRateVal
                    self.bottleneckLink = l
        return minThroughput

    def process(self, ev):
        if ev.type == sg.EVENT_STREAM_START:
            self.beingTransmitted = True
            self.transmitPoint = ev.time
            self.startStreaming(ev.time)

        elif ev.type == sg.EVENT_STREAM_COMPLETED:
            self.updateCounters(ev.time)
            self.beingTransmitted = False
            self.eventRef_trComplete = None
            self.setTransmitRate(0, ev.time)
            for link in self.links:
                link.netDataStreams.remove(self)
            if self.connectedToCache:
                self.upCacheRef.detachNetDataStream(self, ev.time)
            if self.streamType == sg.STREAM_NOISE:
                sg.urRef.activeNoiseStreams -= 1
                if sg.simRef.simulatorReady \
                        and not sg.simRef.simulationDone:
                    newEv = sg.urRef.getNoiseEvent(ev.time)
                    sg.simRef.eventPush(newEv)
            elif self.streamType == sg.STREAM_NORMAL:
                sg.urRef.activeStreams -= 1
            if not sg.urRef.streamGenActive and (
                    sg.urRef.activeStreams == 0 and sg.simRef.simulatorReady):
                sg.simRef.simulationDone = True

        elif ev.type == sg.EVENT_STREAM_EXPAND:
            if self.beingTransmitted:
                self.tryUseMaxTRate(ev.time)
                # try to expand every second
                self.eventRef_expand = se.event(
                    ev.time + sg.EXPAND_INTERVAL,
                    id(self),
                    sg.EVENT_STREAM_EXPAND,
                    self
                )
                sg.simRef.eventPush(self.eventRef_expand)
            else:
                self.eventRef_expand = None
            return
            # don't let Expand event register in the stream stats

        elif ev.type == sg.EVENT_CONSUME_BEGIN:
            self.eventRef_consBegin = None
            self.updateCounters(ev.time)
            self.beingConsumed = True
            self.consumePoint = ev.time
            self.updateEvent_consumeComplete(ev.time)
            self.updateEvent_bufferEmpty(ev.time)
            if self.streamType == sg.STREAM_CACHE:
                self.downCacheRef.startDependentStraems(self, ev.time)
            # statistics
            if self.stats_startTime is None:
                self.stats_startTime = ev.time - self.bufferingBegin
            if self.bufferingBegin != 0:
                self.stats_bufferingTime += ev.time - self.bufferingBegin
                self.bufferingBegin = 0

        elif ev.type == sg.EVENT_SWITCH_TO_LIVERATE:
            self.transmitingLive = True
            self.eventRef_toLiveTRate = None
            self.tryUseMaxTRate(ev.time)
            return
            # don't let 'Switch to liveRate' event register in the stream stats

        elif ev.type == sg.EVENT_CONSUME_COMPLETE:
            self.updateCounters(ev.time)
            self.beingConsumed = False
            self.eventRef_consComplete = None
            self.updateEvents(ev.time)
            self.printStats()
            del sg.event_obj_dict[id(self)]

        elif ev.type == sg.EVENT_CONSUME_BUFFER_EMPTY:
            self.eventRef_bufferEmpty = None
            self.updateCounters(ev.time)
            if self.beingConsumed:
                if self.streamType != sg.STREAM_CACHE:
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
        if self.streamType != sg.STREAM_NOISE:
            self.stats_events.append((ev.time, ev.type))
        return