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
import random
random.seed(42)
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
                            default=100000,
                            help='Simultaneously active streams')
    simSetupGr.add_argument('-backnoise', metavar='number',
                            default=0,
                            help='Simultaneous active background streams')
    simSetupGr.add_argument('-streaming', choices=['live', 'vod'],
                            default='live', help='Streaming Live/Vod')
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
                            default=3000,
                            help='Finalize simulation, no new requests')
    simSetupGr.add_argument('-waitCacheBoot', action='store_true',
                            default=False,
                            help='Wait cache to boot or bypass it')

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

    args = parser.parse_args(argv)

    import matplotlib
    if args.interactive and "DISPLAY" in os.environ:
        matplotlib.use('TkAgg')
    else:
        matplotlib.use('pdf')
    import geoNetGraph
    from netStreamingPrimitives import userRequests
    import hl_sim

    printWithClock("CDN Started on " + str(time.ctime()))

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
    g = geoNetGraph.geoNetGraph(args.links, args.origin, args.rank, countries)

    applyManualInputData = False
    if args.interactive:
        g.iSetGeoNetGraph(selectHosts=True,
                          selectCaches=True,
                          selectProvider=True)
        applyManualInputData = True

    initContentProviders(g)
    printWithClock("Populate the geoNetGraph")
    listOfHosts = populateGeoNetGraph(g, max_hosts, args.percentCache,
                                      applyManualInputData)

    nASes = nCaches = 0
    for tmpASNum, tmpAS in g.netGraph.nodes_iter(data=True):
        if 'ns_nets' in tmpAS and g.isAccessNode(tmpAS['type']):
            nASes += 1
        if 'static_cache' in tmpAS:
            nCaches += 1
    printWithClock("Number of populated ASes: " + str(nASes))
    printWithClock("Number of ASes with static caches: " + str(nCaches))

    simTimeStamp = '-'.join([str(k) for k in time.localtime()[0:6]])
    simResDirName = 'sim_res' + args.siminfo + '(' + simTimeStamp + ')'

    if os.path.exists('debug_out'):
        simResDirName = 'debug_out'
    else:
        if not os.path.exists(simResDirName):
            os.makedirs(simResDirName)
        else:
            print("Result directory exists! Cancel simulation")
            exit(-1)

    printWithClock("Starting simulation on: " + str(time.ctime()))
    start = time.time()
    simulator = hl_sim.highLevelSimulation(args, simResDirName)
    ur = userRequests(simulator, args.trace, g,
                      listOfHosts, max_hosts, int(args.active))
    simulator.urRef = ur
    if int(args.backnoise) > 0:
        simulator.eventPush(ur.getNoiseEvent(simulator.lastEventTime))
    else:
        simulator.eventPush(ur.getNextEvent(simulator.lastEventTime))

    # main simulation loop
    while simulator.eventQueue:
        simulator.step()

    stop = time.time()
    printWithClock("\nSimulation completed on: " + str(time.ctime()))
    printWithClock("Time spent (s): " + str(stop-start))

    simulator.saveSimStatsToFile()
    if args.figures:
        simulator.plotSimStats()
        g.drawGeoNetGraph(simResDirName + '/fig_topology.pdf')

    return 0


def initContentProviders(gnGraph):
    asRouter = p2p_subnet = cp_Nodes = cp_NetDevs = cp_Interfaces = None

    for subNet in gnGraph.as2ip[gnGraph.contentProvider]:
        hostAS = gnGraph.ip2as[subNet[1].exploded]
        if hostAS == gnGraph.contentProvider:
            p2p_subnet = subNet.subnets(new_prefix=30).next()
            printWithClock("Content provider subnet: " + p2p_subnet.exploded)
            break
    assert p2p_subnet is not None

    host_ip = p2p_subnet[1]

    gnGraph.netGraph.node[gnGraph.contentProvider]['as_router'] = asRouter
    gnGraph.netGraph.node[gnGraph.contentProvider]['ns_nets'] = [(
        p2p_subnet, {
            'nodes': cp_Nodes,
            'devices': cp_NetDevs,
            'interfaces': cp_Interfaces
        }
    )]
    gnGraph.netGraph.node[gnGraph.contentProvider]['ip'] = host_ip
    printWithClock("Content provider ip-address: " + host_ip.exploded)
    return


def populateGeoNetGraph(gnGraph, maxHosts, percentCache, onlyPreselected=False):
    listHosts = []
    listASesWithHosts = []
    if onlyPreselected:
        hostsAvailable = sum(
            [sum(gnGraph.netGraph.node[n]['subnetSizes'])
             for n in gnGraph.accessNodes
             if 'ns_nets' in gnGraph.netGraph.node[n]]
        )
    else:
        hostsAvailable = sum(
            [sum(gnGraph.netGraph.node[n]['subnetSizes'])
             for n in gnGraph.accessNodes]
        )
    for tmpASn in gnGraph.accessNodes:
        possibleHostsInAS = sum(gnGraph.netGraph.node[tmpASn]['subnetSizes'])
        nHostsToPopulate = (float(maxHosts) / hostsAvailable) *\
            possibleHostsInAS
        hostsPopulated = 0
        tmpAS = gnGraph.netGraph.node[tmpASn]
        as_subnet_nsNodes = None
        as_subnet_nsNetDevs = None
        as_subnet_nsIfs = None
        channel = None
        if 'ns_nets' in tmpAS or not onlyPreselected:
            for net in gnGraph.as2ip[tmpASn]:
                subNetInfo = (
                    net, {
                        'nodes': as_subnet_nsNodes,
                        'devices': as_subnet_nsNetDevs,
                        'interfaces': as_subnet_nsIfs,
                        'channel': channel
                    }
                )
                if 'ns_nets' in tmpAS:
                    tmpAS['ns_nets'].append(subNetInfo)
                else:
                    tmpAS['ns_nets'] = [subNetInfo]
                for h in net.hosts():
                    if hostsPopulated < nHostsToPopulate:
                        listHosts.append(h)
                        hostsPopulated += 1
                    else:
                        break
                if hostsPopulated >= nHostsToPopulate:
                    break
        if 'ns_nets' in tmpAS and len(tmpAS['ns_nets']) > 0:
            listASesWithHosts.append(tmpASn)
    staticCaches = round(float(len(listASesWithHosts) * percentCache) / 100)
    printWithClock("Percent of ASes with static caches: " + str(percentCache))
    random.shuffle(listASesWithHosts)
    for i in range(int(staticCaches)):
        gnGraph.netGraph.node[listASesWithHosts[i]]['static_cache'] = True
    return listHosts


if __name__ == '__main__':
    main()
