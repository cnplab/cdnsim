"""
    CDNSim

file: cacheNode.py

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

import networkx

import sim_globals as sg
import sim_event as se
import netDataStream as ns


class cacheNode:

    #   originally cacheNode was intended to mimic a server (hardware entity),
    #   however with time it became in some sense a virtual machine as we
    #   create more than one cacheNode entity for every AS, however
    #   for the reason of backward compatibility we still keep the old structure
    #   of the cacheNode to be able to mimic a server (one entity per AS)
    #   this means that strs_cnl_rate[STREAM_RATE][CHANNEL_NUMBER][STREAM] list
    #   is only partially used; e.g. one CHANNEL_NUMBER per VM

    def __init__(self, ASNum):
        self.id = sg.globalCacheID
        sg.globalCacheID += 1
        self.ASnum = ASNum
        self.ready = False
        self.waitingStreams = []
        # how to access the list of Streams Per Channel Per Rate
        # strs_cnl_rate[STREAM_RATE][CHANNEL_NUMBER][STREAM]
        self.cacheStreams = [None] * len(sg.STREAM_RATES) * sg.NUMBER_CHANNELS
        self.strs_cnl_rate = [None] * len(sg.STREAM_RATES)
        self.numStreamsConnected = 0
        self.currentThroughput = 0.0
        self.stats_maxThroughput_vm = 0
        self.stats_maxConnections_vm = 0
        for j in range(len(sg.STREAM_RATES)):
            self.strs_cnl_rate[j] = dict()
        return

    def attachNetDataStream(self, stream, curTime):
        if self.ready:
            # attach a stream to the cache instance:
            # a cache stream is created, 'stream' is added as dependent stream
            sRateID = sg.STREAM_RATES.index(stream.consumeRate)
            cacheStreamID = sRateID * sg.NUMBER_CHANNELS + stream.channel
            if stream.channel in self.strs_cnl_rate[sRateID]:
                # we have channel with this rate in cache
                self.strs_cnl_rate[sRateID][stream.channel].append(
                    stream
                )
            else:
                self.strs_cnl_rate[sRateID][stream.channel] = [stream]
            if self.cacheStreams[cacheStreamID] is None:
                # FIXME: use ip-address as the dest ip instead...
                cSt = ns.netDataStream(
                    stream.consumeRate,
                    stream.srcIP,
                    'cache@'+str(self.ASnum),
                    0,
                    stream.channel,
                    sg.STREAM_CACHE
                )
                cSt.downCacheRef = self
                path = networkx.shortest_path(
                    sg.gnGraph.netGraph,
                    self.ASnum,
                    sg.gnGraph.ip2as[cSt.srcIP]
                )
                if sg.args.hierarchical:
                    # in case of hierarchical caches,
                    # on-demand instantiations are not allowed -> 'first=False'
                    sg.urRef.routeStreamPath_inclCache(
                        path,
                        cSt,
                        curTime,
                        first=False
                    )
                else:
                    sg.urRef.routeStreamPath(path, cSt, curTime)
                self.cacheStreams[cacheStreamID] = cSt
            else:
                cSt = self.cacheStreams[cacheStreamID]
                if cSt.beingConsumed:
                    sg.simRef.eventPush(
                        se.event(
                            curTime + sg.PROPAGATION_DELAY,
                            id(stream),
                            sg.EVENT_STREAM_START,
                            stream
                        )
                    )
        else:
            if sg.args.waitCacheBoot:
                self.waitingStreams.append(stream)
            else:
                return False
        if not stream.connectedToCache:
            stream.upCacheRef = self
            stream.connectedToCache = True
            #   Update stats
            thisAS = sg.gnGraph.netGraph.node[self.ASnum]
            thisAS['cur_Connections'] += 1
            self.numStreamsConnected += 1
            if self.numStreamsConnected > self.stats_maxConnections_vm:
                self.stats_maxConnections_vm = self.numStreamsConnected
            if thisAS['cur_Connections'] > thisAS['stats_maxConnections']:
                thisAS['stats_maxConnections'] = thisAS['cur_Connections']

        return True

    def detachNetDataStream(self, stream, curTime):
        sRateID = sg.STREAM_RATES.index(stream.consumeRate)
        self.strs_cnl_rate[sRateID][stream.channel].remove(stream)
        self.numStreamsConnected -= 1
        thisAS = sg.gnGraph.netGraph.node[self.ASnum]
        thisAS['cur_Connections'] -= 1
        stream.upCacheRef = None
        cacheStreamID = sRateID * sg.NUMBER_CHANNELS + stream.channel
        if len(self.strs_cnl_rate[sRateID][stream.channel]) == 0:
            # stop downloading, if there are no consumers
            cSt = self.cacheStreams[cacheStreamID]
            self.cacheStreams[cacheStreamID] = None
            cEv = se.event(curTime, id(cSt), sg.EVENT_STREAM_COMPLETED)
            cSt.process(cEv)
            if 'static_cache' not in thisAS:
                if self.numStreamsConnected == 0:
                    sg.simRef.cacheStatistics_vm.append((
                        self.ASnum,
                        self.id,
                        self.stats_maxThroughput_vm,
                        self.stats_maxConnections_vm
                    ))
                    sg.gnGraph.netGraph.remove_node(self.id)
                    # delete old cache node not to crowd up the topology
                    thisAS['caches'][stream.channel] = None
                    thisAS['nCacheRequests'][stream.channel] = 0
                    del sg.event_obj_dict[id(self)]
        return

    def startDependentStraems(self, cacheStream, curTime):
        cacheStream.updateCounters(curTime)
        cacheStream.beingConsumed = True
        cacheStream.consumePoint = curTime
        channel = cacheStream.channel
        sRateID = sg.STREAM_RATES.index(cacheStream.consumeRate)
        for stream in self.strs_cnl_rate[sRateID][channel]:
            if not stream.beingTransmitted:
                sg.simRef.eventPush(
                    se.event(
                        curTime + sg.PROPAGATION_DELAY,
                        id(stream),
                        sg.EVENT_STREAM_START,
                        stream
                    )
                )
        return

    def getParentCacheStreamTransmitRate(self, stream):
        channel = stream.channel
        sRateID = sg.STREAM_RATES.index(stream.consumeRate)
        cSt = self.cacheStreams[sRateID * sg.NUMBER_CHANNELS + channel]
        return cSt.transmitRate

    def getParentCacheStreamBufferSize(self, stream, curTime):
        channel = stream.channel
        sRateID = sg.STREAM_RATES.index(stream.consumeRate)
        cSt = self.cacheStreams[sRateID * sg.NUMBER_CHANNELS + channel]
        cSt.updateCounters(curTime)
        inBuffer = float(cSt.downloadedBit - cSt.consumedBit)
        return inBuffer

    def updateDependentStreams(self, cacheStream, curTime):
        channel = cacheStream.channel
        sRateID = sg.STREAM_RATES.index(cacheStream.consumeRate)
        for stream in self.strs_cnl_rate[sRateID][channel]:
            if stream.transmitingLive:
                stream.tryUseMaxTRate(curTime)
        return

    def updateThroughputStats(self, old_tr, new_tr):
        #   Update stats
        self.currentThroughput += new_tr - old_tr
        thisAS = sg.gnGraph.netGraph.node[self.ASnum]
        thisAS['cur_Throughput'] += new_tr - old_tr
        if self.currentThroughput > self.stats_maxThroughput_vm:
            self.stats_maxThroughput_vm = self.currentThroughput
        if thisAS['cur_Throughput'] > thisAS['stats_maxThroughput']:
            thisAS['stats_maxThroughput'] = thisAS['cur_Throughput']
        return

    def process(self, ev):
        if ev.type == sg.EVENT_CACHE_READY:
            self.ready = True
            for s in self.waitingStreams:
                self.attachNetDataStream(s, ev.time)
            self.waitingStreams = []
        else:
            raise Exception("Unknown event type:" + str(ev.type))
        return