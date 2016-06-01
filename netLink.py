"""
    CDNSim

file: netLink.py

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


class netLink:

    def __init__(self, ca, as_nodeA, as_nodeB, l_id=None):
        self.capacity = float(ca)
        self.netDataStreams = []
        self.id = l_id
        if l_id is not None:
            return
        # the rest is only executted in main thread
        self.id = sg.globalLinkID
        sg.globalLinkID += 1
        self.as_nodeA = as_nodeA
        self.as_nodeB = as_nodeB
        return

    def __str__(self):
        s = 'netLink: ' + str(self.as_nodeA) + '-' + str(self.as_nodeB) +\
            ', capacity=' + str(self.capacity) +\
            ', capacityLeft=' + str(self.getCapacityLeft()) +\
            ', occupied by ' + str(len(self.netDataStreams)) + ' streams'
        return s

    def __getstate__(self):
        return self.capacity, self.netDataStreams, self.id

    def __setstate__(self, (cap, listStr, l_id)):
        self.__init__(cap, None, None, l_id)
        self.netDataStreams = listStr

    def getCapacityLeft(self):
        capacityLeft = self.capacity
        for s in self.netDataStreams:
            capacityLeft -= s.transmitRate
        return capacityLeft

    def getHopsTo(self, link):
        assert link != self
        path = networkx.shortest_path(
            sg.gnGraph.netGraph,
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

    def getFairThroughput(self, nNew):
        res = self.capacity
        if sg.BACKBONE_LINKS_INF_CAPACITY:
            return self.capacity
        nStreams = len(self.netDataStreams) + nNew
        if len(self.netDataStreams) > 0:
            share = self.capacity / nStreams
            nExcludeStreams = 0
            for s in self.netDataStreams:
                if s.bottleneckLink.id != self.id and s.transmitRate < share:
                    nExcludeStreams += 1
                    res -= s.transmitRate
            if nExcludeStreams != nStreams:
                res /= (nStreams - nExcludeStreams)
        return res

    def allocateBandwidthForNewStream(self, curTime, newTR):
        if sg.BACKBONE_LINKS_INF_CAPACITY:
            return
        for s in self.netDataStreams:
            if newTR < s.transmitRate:
                s.setTransmitRate(newTR, curTime)
        return

    def process(self, ev):
        # nothing
        return
