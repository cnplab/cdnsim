#!/usr/bin/env python2

"""
        CDNSim

    file: cdnsim.py

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
from decorations import printWithClock
import argparse
import cProfile
import time
import sys
import os


def main(argv=None):

    if argv is None:
        argv = sys.argv[1:]
    parser = argparse.ArgumentParser(
        description='CDN-Sim in Python',
        formatter_class=lambda prog: argparse.ArgumentDefaultsHelpFormatter(
            prog, max_help_position=32
        )
    )

    inFilesGr = parser.add_argument_group('Input files')
    inFilesGr.add_argument('-trace', metavar='file',
                           default='usr_trace.dat',
                           help='User behavior trace')
    inFilesGr.add_argument('-links', metavar='file',
                           default='as_links.dat',
                           help='IRL AS-to-AS links')
    inFilesGr.add_argument('-origin', metavar='file',
                           default='origin.dat',
                           help='IRL origin prefixes')
    inFilesGr.add_argument('-rank', metavar='file',
                           default='caida.org.dat',
                           help='CAIDA AS rank data')

    simSetupGr = parser.add_argument_group('Simulation setup')
    simSetupGr.add_argument('-geo', metavar='string',
                            default='de',
                            help='Comma-separated list of countries')
    simSetupGr.add_argument('-nhosts', metavar='number',
                            default=1000000,
                            help='Maximal number of hosts')
    simSetupGr.add_argument('-active', metavar='number',
                            default=100000, type=int,
                            help='Simultaneously active streams')
    simSetupGr.add_argument('-backnoise', metavar='number',
                            default=0,
                            help='Simultaneous active background streams')
    simSetupGr.add_argument('-streaming', action='store_true',
                            default=True, help='Live streaming (not VoD)')
    simSetupGr.add_argument('-ondemandCache', action='store_true',
                            default=False,
                            help='Create caches on demand')
    simSetupGr.add_argument('-percentCache', metavar='number', type=int,
                            choices=xrange(1, 101), default=0,
                            help='%% of ASes with static cache')
    simSetupGr.add_argument('-hierarchical', action='store_true',
                            default=False,
                            help='Use hierarchical cache placement')
    simSetupGr.add_argument('-cachesec', metavar='number', type=int,
                            default=10,
                            help='# seconds of video to keep in cache')
    simSetupGr.add_argument('-cacheinit', metavar='number', type=float,
                            default=0.1,
                            help='ondemand cache init time')
    simSetupGr.add_argument('-cachethreshold', metavar='number', type=int,
                            default=1,
                            help='# streams to start a cache')
    simSetupGr.add_argument('-interactive', action='store_true',
                            default=False,
                            help='Interactively populate ASes')
    simSetupGr.add_argument('-reqRate', metavar='number', type=float,
                            default=0,
                            help='Request rate per min (0-auto)')
    simSetupGr.add_argument('-scenario', metavar='file',
                            default='',
                            help='Scenario file (format: time, rate/min)')
    simSetupGr.add_argument('-endtime', metavar='number', type=float,
                            default=30,
                            help='Finalize simulation, no new requests')
    simSetupGr.add_argument('-waitCacheBoot', action='store_true',
                            default=True,
                            help='Wait cache to boot or bypass it')
    simSetupGr.add_argument('-unlimCoreLinkBandwidth', action='store_true',
                            default=False,
                            help='Set no limit to the core link bandwidth')
    resultsGr = parser.add_argument_group('Results')
    resultsGr.add_argument('-siminfo', metavar='text',
                           default='',
                           help='Name of the simulation')
    resultsGr.add_argument('-figures', action='store_true',
                           default=False,
                           help='Figures with results')
    resultsGr.add_argument('-allfigures', action='store_true',
                           default=False,
                           help='Figures for all user streams')
    resultsGr.add_argument('-parallel', action='store_true',
                           default=False,
                           help='Enable parallelism in simulation')

    args = parser.parse_args(argv)

    import matplotlib
    if args.interactive and "DISPLAY" in os.environ:
        matplotlib.use('TkAgg')
    else:
        matplotlib.use('pdf')

    import sim_globals as sg
    sg.init(args)

    printWithClock("CDN-Sim started on " + str(time.ctime()))

    max_hosts = sys.maxint
    if args.nhosts != 'all':
        max_hosts = int(args.nhosts)
    printWithClock("Maximal number of hosts is " + str(max_hosts))

    countries = ['de']
    if args.geo != "":
        countries = str(args.geo).replace(' ', '').split(',')
    else:
        printWithClock("Default geographic area: de")

    printWithClock("Building the geoNetGraph")
    import geoNetGraph
    sg.gnGraph = geoNetGraph.geoNetGraph(
        args.links,
        args.origin,
        args.rank,
        countries
    )

    applyManualInputData = False
    if args.interactive:
        sg.gnGraph.iSetGeoNetGraph(selectHosts=True,
                          selectCaches=True,
                          selectProvider=True)
        applyManualInputData = True

    sg.gnGraph.initContentProviders()

    import hl_sim
    simulator = hl_sim.highLevelSimulation()
    sg.simRef = simulator

    printWithClock("Populate the geoNetGraph")
    import userRequests
    sg.urRef = userRequests.userRequests(max_hosts, applyManualInputData)

    nASes = nCaches = 0
    for tmpASNum, tmpAS in sg.gnGraph.netGraph.nodes_iter(data=True):
        if 'ns_nets' in tmpAS and sg.gnGraph.isAccessNode(tmpAS['type']):
            nASes += 1
        if 'static_cache' in tmpAS:
            nCaches += 1
    printWithClock("Number of populated ASes: " + str(nASes))
    printWithClock("Number of ASes with static caches: " + str(nCaches))

    simTimeStamp = time.strftime('%Y.%m.%d-%H.%M.%S')
    printWithClock("Starting simulation on: " + simTimeStamp)
    start = time.time()
    if int(args.backnoise) > 0:
        e = sg.urRef.getNoiseEvent(simulator.lastEventTime)
    else:
        e = sg.urRef.getNextEvent(simulator.lastEventTime)
    simulator.eventPush(e)

    # main simulation loop
    while simulator.step():
        pass

    stop = time.time()
    print("")
    printWithClock("Simulation completed on: " +
                   time.strftime('%Y.%m.%d-%H.%M.%S'))
    printWithClock("Time spent (s): " + str(stop-start))

    for ASnum, ASnode in sg.gnGraph.netGraph.nodes_iter(data=True):
        if 'caches' in ASnode:
            simulator.cacheStatistics_hw.append((
                ASnum,
                ASnode['stats_maxThroughput'],
                ASnode['stats_maxConnections'],
                ASnode['stats_max_NumVMs']
            ))

    simResDirName = 'sim_res' + args.siminfo + '-' + simTimeStamp
    if os.path.exists('debug_out'):
        simResDirName = 'debug_out'
    else:
        while os.path.exists(simResDirName):
            import string
            simResDirName += '_' + sg.random.choice(string.letters)
            print("Result directory exists! Changing name to " + simResDirName)
        os.makedirs(simResDirName)

    simulator.saveSimStatsToFile(simResDirName)
    simulator.saveSimulationSetupToFile(simResDirName)
    if args.figures:
        simulator.plotSimStats(simResDirName)
        sg.gnGraph.drawGeoNetGraph(simResDirName + '/fig_topology.pdf')

    return 0


if __name__ == '__main__':
    #cProfile.run('main()', sort='tottime')
    main()
