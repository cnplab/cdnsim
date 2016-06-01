"""
        CDNSim

    file: geoNetGraph.py

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
import matplotlib.pyplot as plt
import ipaddress as ip
import networkx as nx
import SubnetTree
import pickle
import sys
import os
import re


# derive a class enabling pickle dump/load
class IPv4Network(ip.IPv4Network):

    def __getstate__(self):
        subnet = self.exploded
        return subnet

    def __setstate__(self, content):
        self.__init__(content)
        return None


class geoNetGraph:
    def parseIRLorigin(self, fileName, ip2as=True, as2ip=False):
        re_origin = re.compile('(\d+\.\d+\.\d+\.\d+/\d+)\t(\d+)', re.UNICODE)
        if as2ip:
            self.as2ip = dict()
        if ip2as:
            self.ip2as = SubnetTree.SubnetTree()
        F_origin = open(fileName, 'r')
        for line in iter(F_origin):
            match = re_origin.match(line)
            if match is not None:
                asNum = int(match.group(2))
                if ip2as:
                    self.ip2as[match.group(1)] = asNum
                if as2ip:
                    newNet = IPv4Network(match.group(1))
                    if newNet.prefixlen > self.smallSubnetPrefix:
                        continue
                    if asNum in self.netGraph:
                        if asNum not in self.as2ip:
                            self.as2ip[asNum] = [newNet]
                        else:
                            self.as2ip[asNum].append(newNet)
        return None

    def initContentProviders(self):
        asRouter = p2p_subnet = cp_Nodes = cp_NetDevs = cp_Interfaces = None

        for subNet in self.as2ip[self.contentProvider]:
            hostAS = self.ip2as[subNet[1].exploded]
            if hostAS == self.contentProvider:
                p2p_subnet = subNet.subnets(new_prefix=30).next()
                printWithClock(
                    "Content provider subnet: " + p2p_subnet.exploded)
                break
        assert p2p_subnet is not None

        host_ip = p2p_subnet[1]

        self.netGraph.node[self.contentProvider]['as_router'] = asRouter
        self.netGraph.node[self.contentProvider]['ns_nets'] = [(
            p2p_subnet, {
                'nodes': cp_Nodes,
                'devices': cp_NetDevs,
                'interfaces': cp_Interfaces
            }
        )]
        self.netGraph.node[self.contentProvider]['ip'] = host_ip
        printWithClock("Content provider ip-address: " + host_ip.exploded)
        return

    def populateGeoNetGraph(self, maxHosts, percentCache,
                            onlyPreselected=False):
        listHosts = []
        listASesWithHosts = []
        if onlyPreselected:
            hostsAvailable = sum(
                [sum(self.netGraph.node[n]['subnetSizes'])
                 for n in self.accessNodes
                 if 'ns_nets' in self.netGraph.node[n]]
            )
        else:
            hostsAvailable = sum(
                [sum(self.netGraph.node[n]['subnetSizes'])
                 for n in self.accessNodes]
            )
        for tmpASn in self.accessNodes:
            possibleHostsInAS = sum(
                self.netGraph.node[tmpASn]['subnetSizes'])
            nHostsToPopulate = (float(maxHosts) / hostsAvailable) * \
                               possibleHostsInAS
            hostsPopulated = 0
            tmpAS = self.netGraph.node[tmpASn]
            as_subnet_nsNodes = None
            as_subnet_nsNetDevs = None
            as_subnet_nsIfs = None
            channel = None
            if 'ns_nets' in tmpAS or not onlyPreselected:
                for net in self.as2ip[tmpASn]:
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
        printWithClock(
            "Percent of ASes with static caches: " + str(percentCache))
        import sim_globals as sg
        sg.random.shuffle(listASesWithHosts)
        for i in range(int(staticCaches)):
            self.netGraph.node[listASesWithHosts[i]]['static_cache'] = True
        return listHosts


    def cache_write(self, cache_folder):
        os.makedirs(cache_folder)
        pickle.dump(self.contentProvider,
                    open(cache_folder + '/contentProvider.cache', 'wb'),
                    protocol=2)
        pickle.dump(self.contentNodes,
                    open(cache_folder + '/contentNodes.cache', 'wb'),
                    protocol=2)
        pickle.dump(self.accessNodes,
                    open(cache_folder + '/accessNodes.cache', 'wb'),
                    protocol=2)
        nx.write_gpickle(self.netGraph, cache_folder + '/asGraph.cache')
        pickle.dump(self.as2ip,
                    open(cache_folder + '/as2ip.cache', 'wb'),
                    protocol=2)
        return None

    def cache_read(self, cache_folder):
        self.contentProvider = pickle.load(
            open(cache_folder + '/contentProvider.cache', 'rb')
        )
        self.contentNodes = pickle.load(
            open(cache_folder + '/contentNodes.cache', 'rb')
        )
        self.accessNodes = pickle.load(
            open(cache_folder + '/accessNodes.cache', 'rb')
        )
        self.netGraph = nx.read_gpickle(cache_folder + '/asGraph.cache')
        self.as2ip = pickle.load(
            open(cache_folder + '/as2ip.cache', 'rb')
        )
        return None

    def __init__(self, irlLinks_f, irlOrigin_f, caida_f, listOfCountries):
        self.countries = listOfCountries
        self.overlayObjects = dict()
        self.contentProvider = None
        self.smallSubnetPrefix = 24
        self.contentNodes = None
        self.accessNodes = None
        self.netGraph = None
        self.geo_as_dir = "geoAS"
        self.cache_folder = self.geo_as_dir + '/' + '_'.join(sorted(listOfCountries))
        self.pickedNodes = []
        self.as2ip = None
        self.ip2as = None
        self.hosts = None
        self.pos = None

        re_AS_link = re.compile('(\d+)\t(\d+)\t(\d+)', re.UNICODE)
        re_caida = re.compile(
            '"(\d+)"\t"(\d+)"\t"(.*)"\t"(.*)"\t"(.*)"\t"(.*)"\t"(.*)'
            '"\t"(.*)"\t"(.*)"\t"(.*)"\t"(.*)"\t"(.*)"', re.UNICODE
        )
        re_geoAS = re.compile('(\d+)\t(.+)\t(\d+-\d+-\d+)\.*', re.UNICODE)


        if os.path.exists(self.cache_folder):
            printWithClock("geoNetGraph cache for " + str(listOfCountries) +
                           " found in " + self.cache_folder +
                           ", restoring geoNetGraph")
            self.cache_read(self.cache_folder)
            printWithClock("Restore complete. Reading IRL origin, "
                           "building the 2nd part of the AS_num <-> "
                           "IP_subnet map..")
            self.parseIRLorigin(irlOrigin_f)
        else:
            printWithClock("geoNetGraph cache for " + str(listOfCountries) +
                           " not found, building geoNetGraph..")
            print("\t>>> This will take a while, "
                  "you may go take a cup of coffee.. <<<")
            printWithClock("Reading IRL topology graph..")
            self.netGraph = nx.Graph()
            irlLinkLife = 31
            F_AS_links = open(irlLinks_f, 'r')
            for line in iter(F_AS_links):
                match = re_AS_link.match(line)
                if match is not None:
                    if int(match.group(3)) >= irlLinkLife:
                        self.netGraph.add_node(
                            int(match.group(1)),
                            type='',
                            name='',
                            size=0,
                            subnetSizes=[],
                            degree='',
                            country=''
                        )
                        if int(match.group(2)) not in self.netGraph:
                            self.netGraph.add_node(
                                int(match.group(2)),
                                type='',
                                name='',
                                size=0,
                                subnetSizes=[],
                                degree='',
                                country=''
                            )
                        self.netGraph.add_edge(
                            int(match.group(1)),
                            int(match.group(2))
                        )
            F_AS_links.close()
            printWithClock("Total ASes in the topology: " +
                           str(self.netGraph.number_of_nodes()) +
                           ", Edges:" + str(self.netGraph.number_of_edges()))

            printWithClock("Reading geoAS data..", end=" ")
            geoASes = dict()
            geoASes_flat = []
            for countryPrefix in self.countries:
                F_country = open(self.geo_as_dir+'/'+countryPrefix+'.dat', 'r')
                print(countryPrefix, end=" ")
                for line in iter(F_country):
                    match = re_geoAS.match(line)
                    if match is not None:
                        asNum = int(match.group(1))
                        if asNum in self.netGraph:
                            self.netGraph.node[asNum]['country'] = countryPrefix
                            geoASes_flat.append(asNum)
                            if countryPrefix not in geoASes:
                                geoASes[countryPrefix] = [asNum]
                            else:
                                geoASes[countryPrefix].append(asNum)
                F_country.close()
            print("\n\t" + str(sum(len(li) for li in geoASes.values())) +
                  " ASes satisfied " + str(self.countries))

            printWithClock("Applying the geoAS data..")
            printWithClock("Removing", end=" ")
            toDel = [n for n in self.netGraph.nodes_iter()
                     if n not in geoASes_flat]
            print(str(len(toDel)) +
                  " ASes located outside of the provided region")
            self.netGraph.remove_nodes_from(toDel)

            printWithClock("Reading IRL origin, "
                           "building the AS_num<->IP_subnet map..")
            self.parseIRLorigin(irlOrigin_f, ip2as=True, as2ip=True)

            printWithClock("Removing", end=" ")
            toDel = [n for n in self.netGraph.nodes_iter()
                     if n not in self.as2ip]
            print(str(len(toDel)) + " ASes with missing origin data")
            self.netGraph.remove_nodes_from(toDel)

            printWithClock("Reading the CAIDA AS data..")
            self.accessNodes = []
            self.contentNodes = []
            F_CAIDA_RANKS = open(caida_f, 'r')
            for line in iter(F_CAIDA_RANKS):
                match = re_caida.match(line)
                if match is not None:
                    asNum = int(match.group(2))
                    if asNum in self.netGraph:
                        node = self.netGraph.node[asNum]
                        node['type'] = match.group(5)
                        node['name'] = match.group(3)
                        if match.group(8) is not '':
                            node['size'] = int(match.group(8).replace(',', ''))
                        if match.group(12) is not '':
                            node['degree'] = int(
                                match.group(12).replace(',', '')
                            )
                        if self.isAccessNode(match.group(5)):
                            self.accessNodes.append(asNum)
                        if self.isContentNode(match.group(5)):
                            self.contentNodes.append(asNum)
            F_CAIDA_RANKS.close()

            printWithClock("Selecting a content provider within "
                           "the region of interest:", end=" ")
            self.contentNodes = sorted(
                self.contentNodes, key=lambda tmpAS:
                self.netGraph.node[tmpAS]['degree'], reverse=True
            )
            self.contentProvider = self.contentNodes[0]
            print(str(self.contentProvider) +
                  ", transit degree = " +
                  str(self.netGraph.node[self.contentProvider]['degree']))

            printWithClock("Removing", end=" ")
            toDel = [n for n in self.netGraph.nodes_iter()
                     if not nx.has_path(self.netGraph, self.contentProvider, n)]
            print(str(len(toDel)) +
                  " ASes with no connection to the content provider..")
            self.netGraph.remove_nodes_from(toDel)
            self.accessNodes = [n for n in self.accessNodes
                                if self.netGraph.has_node(n)]
            self.contentNodes = [n for n in self.contentNodes
                                 if self.netGraph.has_node(n)]

            printWithClock("Allocating ip-addresses for every AS..")
            print("\tI appreciate you staying here with me, "
                  "but really, go get some coffee c[_] :)")
            self.allocHostAddresses()

            printWithClock("Saving geoNetGraph cache for " +
                           str(listOfCountries) + " in " + self.cache_folder)
            self.cache_write(self.cache_folder)

        printWithClock("Final number of ASes in the sub-graph: " +
                       str(self.netGraph.number_of_nodes()) + ", Edges:" +
                       str(self.netGraph.number_of_edges()))
        printWithClock("\tContent-provider ASes: " +
                       str(len(self.contentNodes)))
        printWithClock("\tAccess-provider ASes: " + str(len(self.accessNodes)))
        printWithClock("\tContent provider AS: " + str(self.contentProvider))
        self.hosts = 0
        return

    def allocHostAddresses(self):
        for curASn, curAS in self.netGraph.nodes_iter(data=True):
            curASnets = self.as2ip[curASn]
            updatedASnets = []
            for curASnet in curASnets:
                skipThisNet = False
                for smallSubnet in curASnet.subnets(
                        new_prefix=self.smallSubnetPrefix
                ):
                    if self.ip2as[smallSubnet[1].exploded] != curASn:
                        skipThisNet = True
                        break
                if not skipThisNet:
                    curAS['subnetSizes'].append(curASnet.num_addresses)
                    updatedASnets.append(curASnet)
            self.as2ip[curASn] = updatedASnets
        return

    def isAccessNode(self, type):
        # Choose wisely :)
        """
        if 'Ac' == type:
        if not 'Co' == type:
        if not ('Co' == type or 'Tr' in type):
        """
        if 'Ac' == type:
            return True
        else:
            return False

    def isContentNode(self, type):
        if 'Co' == type:
            return True
        else:
            return False

    def drawGeoNetGraph(self, filename, large=False):
        drawLabels = True
        scaleFont = 1
        if large:
            scaleFont = 2
        printWithClock("Drawing " + str(self.netGraph.number_of_nodes()) +
                       " ASes in " + str(self.countries))
        populatedASes = []
        populatedASes_cache = []
        contentASes = []
        contentASes_cache = []
        emptyAccessASes = []
        cacheOnlyASes = []
        restOfASes = []
        for n in self.netGraph.nodes_iter():
            if 'type' in self.netGraph.node[n]:
                if self.isAccessNode(self.netGraph.node[n]['type']):
                    if 'ns_nets' in self.netGraph.node[n]:
                        if 'cache' in self.netGraph.node[n] \
                                and self.netGraph.node[n]['cache'] is not None:
                            populatedASes_cache.append(n)
                        else:
                            populatedASes.append(n)
                    else:
                        if 'cache' in self.netGraph.node[n] \
                                and self.netGraph.node[n]['cache'] is not None:
                            cacheOnlyASes.append(n)
                        else:
                            emptyAccessASes.append(n)
                elif self.isContentNode(self.netGraph.node[n]['type']):
                    if 'cache' in self.netGraph.node[n] \
                            and self.netGraph.node[n]['cache'] is not None:
                        contentASes_cache.append(n)
                    else:
                        contentASes.append(n)
                else:
                    if 'cache' in self.netGraph.node[n] \
                            and self.netGraph.node[n]['cache'] is not None:
                        cacheOnlyASes.append(n)
                    else:
                        restOfASes.append(n)
            else:
                if 'cache' in self.netGraph.node[n] \
                        and self.netGraph.node[n]['cache'] is not None:
                    cacheOnlyASes.append(n)
                else:
                    restOfASes.append(n)
        contentASes.remove(self.contentProvider)
        plt.figure(figsize=(10, 6))
        plt.axis('off')
        self.pos = nx.spring_layout(self.netGraph)
        nx.draw_networkx_edges(
            self.netGraph,
            pos=self.pos,
            width=0.1,
            alpha=0.3
        )
        nx.draw_networkx_nodes(
            self.netGraph,
            pos=self.pos,
            nodelist=restOfASes,
            # grey
            node_color='0.5',
            node_shape='s',
            edge_color='k',
            width=0.1,
            linewidths=0.1,
            node_size=5,
            label='Tr-AS',
            alpha=0.4
        )
        nx.draw_networkx_nodes(
            self.netGraph,
            pos=self.pos,
            nodelist=contentASes,
            # grey
            node_color='0.5',
            node_shape='*',
            edge_color='k',
            linewidths=0.1,
            node_size=7,
            label='empty Co-AS',
            alpha=0.4
        )
        nx.draw_networkx_nodes(
            self.netGraph,
            pos=self.pos,
            nodelist=contentASes_cache,
            # grey
            node_color='m',
            node_shape='*',
            edge_color='k',
            linewidths=0.1,
            node_size=7,
            label='empty Co-AS + Cache',
            alpha=0.4
        )
        nx.draw_networkx_nodes(
            self.netGraph,
            pos=self.pos,
            nodelist=emptyAccessASes,
            # white
            node_color='w',
            edge_color='k',
            linewidths=0.1,
            node_size=7,
            label='empty Ac-AS',
            alpha=0.4
        )
        nx.draw_networkx_nodes(
            self.netGraph,
            pos=self.pos,
            nodelist=populatedASes,
            # yellow
            node_color='y',
            edge_color='k',
            linewidths=0.2,
            node_size=7,
            label='Populated Ac-AS',
            alpha=0.5
        )
        nx.draw_networkx_nodes(
            self.netGraph,
            pos=self.pos,
            nodelist=populatedASes_cache,
            # orange
            node_color='orange',
            edge_color='k',
            linewidths=0.2,
            node_size=7,
            label='Populated Ac-AS + Cache',
            alpha=0.5
        )
        nx.draw_networkx_nodes(
            self.netGraph,
            pos=self.pos,
            nodelist=cacheOnlyASes,
            # magenta
            node_color='m',
            edge_color='k',
            linewidths=0.2,
            node_size=7,
            label='AS + Cache',
            alpha=0.5
        )
        nx.draw_networkx_nodes(
            self.netGraph,
            pos=self.pos,
            nodelist=[self.contentProvider],
            # red
            node_color='r',
            node_shape='*',
            edge_color='k',
            linewidths=0.1,
            node_size=8,
            label='Content provider',
            alpha=0.7
        )
        if drawLabels:
            nx.draw_networkx_labels(
                self.netGraph,
                pos=self.pos,
                font_size=1*scaleFont,
                font_color='g',
                alpha=0.4
            )
        plt.legend(
            fontsize=5,
            frameon=False,
            bbox_to_anchor=(1, 1),
            numpoints=1,
            framealpha=0.7
        )
        plt.savefig(filename, bbox_inches='tight')
        plt.figure()

    def on_pick(self, event, nodeID=None, mButton=None):
        if event is not None:
            mouseButton = event.mouseevent.button
            x, y = event.artist._offsets[event.ind[0]]
            closestNode = None
            for key, val in self.pos.iteritems():
                xp, yp = val
                if xp == x and yp == y:
                    closestNode = key
                    break
            if closestNode is not None:
                nodeID = closestNode
            else:
                return
        else:
            mouseButton = mButton

        if nodeID not in self.overlayObjects:
            self.overlayObjects[nodeID] = dict()
        if mouseButton not in self.overlayObjects[nodeID]:
            newObj = None
            if mouseButton == 1:  # populate with hosts
                if self.isAccessNode(self.netGraph.node[nodeID]['type']):
                    self.netGraph.node[nodeID]['ns_nets'] = []
                    newObj = nx.draw_networkx_nodes(
                        self.netGraph,
                        self.pos, [nodeID],
                        node_size=70,
                        node_shape='o',
                        node_color='y',
                        alpha=0.5
                    )
            elif mouseButton == 2:  # provider
                if self.isContentNode(self.netGraph.node[nodeID]['type']):
                    if self.contentProvider is not None:
                        overlayObj = \
                            self.overlayObjects[self.contentProvider].pop(
                                mouseButton, None
                            )
                        if overlayObj is not None:
                            overlayObj.remove()
                    newObj = nx.draw_networkx_nodes(
                        self.netGraph,
                        self.pos,
                        [nodeID],
                        node_size=70,
                        node_shape='*',
                        node_color='r',
                        alpha=0.5
                    )
                    self.contentProvider = nodeID
            elif mouseButton == 3:  # cache
                newObj = nx.draw_networkx_nodes(
                    self.netGraph,
                    self.pos,
                    [nodeID],
                    node_size=70,
                    node_shape='o',
                    node_color='m',
                    alpha=0.5
                )
                self.netGraph.node[nodeID]['cache'] = None
            if newObj is not None:
                self.overlayObjects[nodeID][mouseButton] = newObj
        else:
            if mouseButton == 1:  # don't populate with hosts
                self.netGraph.node[nodeID].pop('net', None)
            elif mouseButton == 2:  # remove provider
                self.contentProvider = None
            elif mouseButton == 3:  # remove cache
                self.netGraph.node[nodeID].pop('cache', None)
            overlayObj = self.overlayObjects[nodeID].pop(mouseButton, None)
            if overlayObj is not None:
                overlayObj.remove()
        if event is not None:
            plt.draw()
        if event is not None:
            self.pickedNodes.append((nodeID, mouseButton))
        print('on pick:' + str(nodeID), str(mouseButton))

    def iSetGeoNetGraph(self, selectHosts, selectCaches, selectProvider,
                        large=False):
        drawLabels = True
        scaleFont = 1
        if large:
            scaleFont = 2
        printWithClock("Drawing " + str(self.netGraph.number_of_nodes()) +
                       " ASes in " + str(self.countries))
        noCacheNodes = []
        cacheNodes = []
        contentNodes = []
        emptyAcNodes = []
        restOfNodes = []
        for n in self.netGraph.nodes_iter():
            if 'type' in self.netGraph.node[n]:
                if self.isAccessNode(self.netGraph.node[n]['type']):
                    if 'ns_nets' in self.netGraph.node[n]:
                        if 'cache' in self.netGraph.node[n]:
                            cacheNodes.append(n)
                        else:
                            noCacheNodes.append(n)
                    else:
                        emptyAcNodes.append(n)
                elif self.isContentNode(self.netGraph.node[n]['type']):
                    contentNodes.append(n)
                else:
                    restOfNodes.append(n)
            else:
                restOfNodes.append(n)
        contentNodes.remove(self.contentProvider)
        printWithClock("Drawing the graph ...")

        ax = plt.gca()
        ax.axis('off')
        ax.autoscale(False)
        fig = plt.gcf()

        if self.pos is None:
            self.pos = nx.spring_layout(self.netGraph)
        nx.draw_networkx_edges(
            self.netGraph,
            pos=self.pos,
            ax=ax,
            width=0.5,
            alpha=0.5
        )
        plottedNodes = nx.draw_networkx_nodes(
            self.netGraph,
            pos=self.pos,
            ax=ax,
            nodelist=restOfNodes,
            # grey
            node_color='0.5',
            node_shape='s',
            edge_color='k',
            width=0.1,
            linewidths=1,
            node_size=50,
            label='Tr-AS',
            alpha=0.4
        )
        if plottedNodes is not None:
            if selectCaches:
                plottedNodes.set_picker(0.001)
        plottedNodes = nx.draw_networkx_nodes(
            self.netGraph,
            pos=self.pos,
            ax=ax,
            nodelist=contentNodes,
            # dark grey
            node_color='0.5',
            node_shape='*',
            edge_color='k',
            linewidths=1,
            node_size=70,
            label='empty Co-AS',
            alpha=0.4
        )
        if plottedNodes is not None:
            if selectProvider or selectCaches:
                plottedNodes.set_picker(0.001)
        plottedNodes = nx.draw_networkx_nodes(
            self.netGraph,
            pos=self.pos,
            ax=ax,
            nodelist=emptyAcNodes,
            # white
            node_color='w',
            edge_color='k',
            linewidths=1,
            node_size=70,
            label='empty Ac-AS',
            alpha=0.4
        )
        if plottedNodes is not None:
            if selectHosts or selectCaches:
                plottedNodes.set_picker(0.001)
        plottedNodes = nx.draw_networkx_nodes(
            self.netGraph,
            pos=self.pos,
            ax=ax,
            nodelist=noCacheNodes,
            # yellow
            node_color='y',
            edge_color='k',
            linewidths=1,
            node_size=70,
            label='Ac+hosts',
            alpha=0.5
        )
        if plottedNodes is not None:
            if selectHosts or selectCaches:
                plottedNodes.set_picker(0.001)
        plottedNodes = nx.draw_networkx_nodes(
            self.netGraph,
            pos=self.pos,
            ax=ax,
            nodelist=cacheNodes,
            # magenta
            node_color='m',
            edge_color='k',
            linewidths=1,
            node_size=70,
            label='Ac+hosts+cache',
            alpha=0.5
        )
        if plottedNodes is not None:
            plottedNodes.set_picker(False)
        if self.contentProvider is not None:
            plottedNodes = nx.draw_networkx_nodes(
                self.netGraph,
                pos=self.pos,
                ax=ax,
                nodelist=[self.contentProvider],
                # red
                node_color='r',
                node_shape='*',
                edge_color='k',
                linewidths=1,
                node_size=70,
                label='Content provider',
                alpha=0.7
            )
            if plottedNodes is not None:
                if selectProvider:
                    plottedNodes.set_picker(0.001)
                    self.overlayObjects[self.contentProvider] = dict()
                    self.overlayObjects[self.contentProvider][2] = plottedNodes
        if drawLabels:
            nx.draw_networkx_labels(
                self.netGraph,
                pos=self.pos,
                ax=ax,
                font_size=3*scaleFont,
                font_color='g',
                alpha=0.4
            )
        plt.legend(
            fontsize=12,
            frameon=False,
            bbox_to_anchor=(1, 1),
            numpoints=1,
            framealpha=0.7
        )
        cid = fig.canvas.mpl_connect('pick_event', self.on_pick)
        if os.path.isfile(self.cache_folder + '/userPickedSetup.cache'):
            print("userPickedSetup.cache is found, "
                  "do you want to use it? (y)es / no")
            reply = sys.stdin.readline()
            if 'yes' in reply or 'y' in reply:
                self.pickedNodes = pickle.load(
                    open(self.cache_folder + '/userPickedSetup.cache', 'rb')
                )
                printWithClock("user-picked nodes found, total: " +
                               str(len(self.pickedNodes)))
                for nodeID, mouseButton in iter(self.pickedNodes):
                    self.on_pick(None, nodeID, mouseButton)
            else:
                self.pickedNodes = []
        plt.show()
        fig.canvas.mpl_disconnect(cid)
        self.overlayObjects = dict()
        if len(self.pickedNodes) > 0:
            pickle.dump(
                self.pickedNodes,
                open(self.cache_folder + '/userPickedSetup.cache', 'wb'),
                protocol=2
            )
        assert self.contentProvider is not None
