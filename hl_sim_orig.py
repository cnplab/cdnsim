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
import multiprocessing as mp
import Queue
import treap

import sim_globals as sg

def eventQueueKeeper(inPipe, outQueue, commLock):
    eventQueue = treap.treap()
    updated = False
    keepRunning = True
    print(">>\teventQueueKeeper is started")

    while keepRunning:
        if inPipe.poll():
            #   withdraw prev event if not yet
            if not outQueue.empty():
                prevNextEv = outQueue.get()
                eventQueue[prevNextEv] = prevNextEv
            #   insert new event
            inc_ev, extra = inPipe.recv()
            if extra is None:
                eventQueue[inc_ev] = inc_ev
            elif type(extra) == tuple:
                action, val = extra
                if action == sg.ACTION_UPDATE:
                    eventQueue.remove(inc_ev)
                    inc_ev.time = val
                    eventQueue[inc_ev] = inc_ev
                elif action == sg.ACTION_DELETE:
                    eventQueue.remove(inc_ev)
                elif action == sg.ACTION_STOP:
                    if not eventQueue:
                        print(">>\tstop eventQueueKeeper")
                        keepRunning = False
                        break
                else:
                    print(">>\tReceived:\t" + str(inc_ev))
            updated = True
        if eventQueue and (updated or outQueue.empty()):
            newest_ev = eventQueue.find_min()
            eventQueue.remove(newest_ev)
            outQueue.put(newest_ev)
        if updated:
            commLock.release()
            updated = False
    print(">>\teventQueueKeeper finished successfully")


class highLevelSimulation:
    def __init__(self):
        self.mpManager = mp.Manager()
        self.commQueue_in = self.mpManager.Queue(maxsize=1)
        commPipe_chld, self.commPipe_out = mp.Pipe(False)
        self.communicationLock = mp.Lock()
        self.eventQueueProcess = mp.Process(
            target=eventQueueKeeper,
            args=(commPipe_chld, self.commQueue_in, self.communicationLock)
        )
        self.eventQueueProcess.start()
        self.lastEventTime = 0.0
        self.simulatorReady = False if sg.args.backnoise else True
        self.simulationDone = False
        self.simulationStatistics = []
        self.cacheStatistics_vm = []
        self.cacheStatistics_hw = []
        self.urStatistics_nActCons = []
        self.urStatistics_nReqPSec = []
        return

    def __del__(self):
        self.eventQueueProcess.terminate()
        self.eventQueueProcess.join()

    def step(self):
        self.communicationLock.acquire()
        try:
            e = self.commQueue_in.get(timeout=1)
        except Queue.Empty:
            self.commPipe_out.send((None, (sg.ACTION_STOP, None)))
            return False
        self.communicationLock.release()
        if self.lastEventTime > e.time:
            print("Last event time: " + str(self.lastEventTime) +
                  "current event: " + str(e))
            exit(-1)
        self.lastEventTime = e.time
        objRef = sg.event_obj_dict[e.objRef_id]
        objRef.process(e)
        return True

    def eventPush(self, e):
        self.communicationLock.acquire()
        self.commPipe_out.send((e, None))
        return

    def eventUpdateTime(self, e, newTime):
        self.communicationLock.acquire()
        self.commPipe_out.send((e, (sg.ACTION_UPDATE, newTime)))
        #   keep the local version synced
        e.time = newTime
        return

    def deleteEvent(self, e):
        self.communicationLock.acquire()
        self.commPipe_out.send((e, (sg.ACTION_DELETE, None)))
        return

    def plotSimStats(self, simResDirName):
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
            sg.NUMBER_CHANNELS,
            histtype='stepfilled',
            normed=True
        )
        plt.savefig(simResDirName + '/fig_channelPopularity.pdf')

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
        plt.savefig(simResDirName + '/fig_startTimes.pdf')

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
        plt.savefig(simResDirName + '/fig_buffTimes.pdf')

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
        plt.savefig(simResDirName + '/fig_buffEvents.pdf')

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
        plt.savefig(simResDirName + '/fig_playTimes.pdf')

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
            plt.savefig(simResDirName + '/fig_avgTRates_' +
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
        plt.savefig(simResDirName + '/fig_serverStats.pdf')

        return

    def saveSimStatsToFile(self, simResDirName):
        import csv
        fOutName = simResDirName + '/results.csv'
        printWithClock("Saving simulation results to: " + fOutName)
        with open(fOutName, 'wb') as csvfile:
            writer = csv.writer(csvfile)
            for entry in self.simulationStatistics:
                writer.writerow(entry)
        with open(simResDirName + '/cache_vm_Stats.csv', 'wb') as csvfile:
            writer = csv.writer(csvfile)
            for entry in self.cacheStatistics_vm:
                writer.writerow(entry)
        with open(simResDirName + '/cache_hw_Stats.csv', 'wb') as csvfile:
            writer = csv.writer(csvfile)
            for entry in self.cacheStatistics_hw:
                writer.writerow(entry)
        return

    def saveSimulationSetupToFile(self, simResDirName):
        fOutName = simResDirName + '/params.txt'
        with open(fOutName, 'w') as fOut:
            for d, v in sorted(sg.args.__dict__.items(), key=lambda tup: tup[0]):
                fOut.write(d + ': ' + str(v) + '\n')
        return

