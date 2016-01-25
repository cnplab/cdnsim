"""
        CDNSim

    file: hl_sim.py

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
from decorations import printWithClock
import treap


class highLevelSimulation:
    def __init__(self, args, resDirName):
        self.eventQueue = treap.treap()
        self.lastEventTime = 0
        self.urRef = None
        self.topArgs = args
        self.simulatorReady = False if args.backnoise else True
        self.simulationDone = False
        self.simulationStatistics = []
        self.urStatistics_nActCons = []
        self.urStatistics_nReqPSec = []
        self.simResDirName = resDirName
        return

    def step(self):
        minKey = self.eventQueue.find_min()
        e = self.eventQueue[minKey]
        self.eventQueue.remove(minKey)
        self.lastEventTime = e.time
        e.objRef.process(e)
        return

    def eventPush(self, ev):
        self.eventQueue[ev] = ev
        return

    def eventUpdateTime(self, e, newTime):
        self.eventQueue.remove(e)
        e.time = newTime
        self.eventQueue[e] = e
        return

    def deleteEvent(self, e):
        self.eventQueue.remove(e)
        return

    def plotSimStats(self):
        import netStreamingPrimitives
        printWithClock("Plotting simulation results..")
        sTypes, ids, chnls, startTs, buffTs, buffEvs, playTs, avgTRs, consRs,\
            toCaches, srcIPs, dstIPs = zip(*self.simulationStatistics)
        setRates = set()
        for i in consRs:
            setRates.add(i)
        avgTRperCR = dict.fromkeys(setRates, [])
        for k in avgTRperCR.keys():
            avgTRperCR[k] = [float(i[7])/float(i[8])
                             for i in self.simulationStatistics if i[8] == k]
        buffPlayRatio = [float(i[4])/float(i[4] + i[6])
                         for i in self.simulationStatistics]

        plt.clf()
        plt.suptitle('Histogram: Distribution of channel popularity')
        plt.ylabel('Fraction of users')
        plt.xlabel('Channel #')
        plt.hist(
            chnls,
            netStreamingPrimitives.NUMBER_CHANNELS,
            histtype='stepfilled',
            normed=True
        )
        plt.savefig(self.simResDirName + '/fig_channelPopularity.pdf')

        plt.clf()
        plt.suptitle('Histogram: Start times')
        plt.ylabel('Number of viewers')
        plt.xlabel('Start time (s)')
        plt.hist(
            startTs,
            max(startTs),
            histtype='stepfilled',
            cumulative=True,
            normed=True
        )
        plt.savefig(self.simResDirName + '/fig_startTimes.pdf')

        plt.clf()
        plt.suptitle('Histogram: Buffering times to playbacktime ratio')
        plt.ylabel('Fraction of viewers')
        plt.xlabel('Buffering time')
        plt.hist(
            buffPlayRatio,
            100,
            histtype='stepfilled',
            cumulative=True,
            normed=True
        )
        plt.savefig(self.simResDirName + '/fig_buffTimes.pdf')

        plt.clf()
        plt.suptitle('Histogram: Buffering events')
        plt.ylabel('Number of viewers')
        plt.xlabel('Buffering events')
        maxBufEvntVal = max(buffEvs)
        plt.hist(
            buffEvs,
            maxBufEvntVal if maxBufEvntVal > 0 else 10,
            histtype='stepfilled',
            cumulative=True,
            normed=True
        )
        plt.savefig(self.simResDirName + '/fig_buffEvents.pdf')

        plt.clf()
        plt.suptitle('Histogram: Distribution of play times')
        plt.ylabel('Fraction of viewers')
        plt.xlabel('Play time (s)')
        plt.hist(
            playTs,
            100,
            histtype='stepfilled',
            cumulative=False,
            normed=False
        )
        plt.savefig(self.simResDirName + '/fig_playTimes.pdf')

        for k in avgTRperCR.keys():
            plt.clf()
            plt.suptitle(
                'Histogram: Average download rate, playback = ' +
                str(k) + ' bps.'
            )
            plt.ylabel('Number of viewers')
            plt.xlabel('Download / consume rate')
            plt.hist(
                avgTRperCR[k],
                histtype='stepfilled',
                cumulative=False,
                normed=False
            )
            plt.savefig(self.simResDirName + '/fig_avgTRates_' +
                        str(k) + '.pdf')

        plt.clf()
        plt.suptitle('Server side statistics')
        ax1 = plt.gca()
        ax1.set_xlabel('Time (s)')
        ax1.set_ylabel('# active connections', color='b')
        x, y = zip(*self.urStatistics_nActCons)
        ax1.plot(x, y)
        for tl in ax1.get_yticklabels():
            tl.set_color('b')
        ax2 = ax1.twinx()
        ax2.set_ylabel('# requests per minute', color='r')
        x, y = zip(*self.urStatistics_nReqPSec)
        ax2.plot(x, y, color='r')
        for tl in ax2.get_yticklabels():
            tl.set_color('r')
        plt.savefig(self.simResDirName + '/fig_serverStats.pdf')

        return

    def saveSimStatsToFile(self):
        import csv
        resFileName = 'results' +\
                      '_nh' + str(len(self.urRef.listOfHosts)) +\
                      '_ac' + str(self.urRef.activeStreamsMax) +\
                      '_ba' + str(self.urRef.activeNoiseStreamsMax) +\
                      '_to' + str(self.urRef.totalStreams) +\
                      '_cp' + str(self.topArgs.percentCache) +\
                      '_cb' + str(self.topArgs.cachesec) +\
                      '_ci' + str(self.topArgs.cacheinit) +\
                      '_ct' + str(self.topArgs.cachethreshold) +\
                      '_on' + str(self.topArgs.ondemandCache) +\
                      '_st' + str(self.topArgs.streaming)
        fOutName = self.simResDirName + '/' + resFileName + '.csv'
        printWithClock("Saving simulation results to: " + fOutName)
        with open(fOutName, 'wb') as csvfile:
            writer = csv.writer(csvfile)
            for entry in self.simulationStatistics:
                writer.writerow(entry)
        return
