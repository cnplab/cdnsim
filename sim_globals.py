"""
        CDNSim

    file: sim_globals.py

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
import numpy.random
import random
import time

EVENT_RESERVED = 0

#   netDataStream events
EVENT_STREAM_START = 1
EVENT_STREAM_COMPLETED = 2
EVENT_STREAM_EXPAND = 3
EVENT_CONSUME_BEGIN = 4
EVENT_SWITCH_TO_LIVERATE = 5
EVENT_CONSUME_COMPLETE = 6
EVENT_CONSUME_BUFFER_EMPTY = 7

#   cacheNode events
EVENT_CACHE_READY = 8

#   userRequests events
EVENT_USER_REQUEST = 9
EVENT_NOISE_USER_REQUEST = 10
EVENT_CHANGE_REQUEST_RATE = 11
EVENT_SIM_FINALIZE = 12
EVENT_PERIODIC_STATS = 13

#   event queue keeper actions
ACTION_DELETE = -1
ACTION_UPDATE = -2
ACTION_STOP = -3

PROPAGATION_DELAY = 0.01
# Max rates for video streaming quality: 360p, 480p, 720p, 1080p, 2K, 4K
STREAM_RATES = [1000000, 2500000, 5000000, 8000000, 10000000, 20000000]
FAST_BACKBONE_LINK_BANDWIDTH = 40000000000.0  # 40 Gbps
BACKBONE_LINK_BANDWIDTH = 10000000000.0  # 10 Gbps
BACKBONE_LINKS_INF_CAPACITY = False
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
globalLinkID = 0

ts_sim_begin = time.time()

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

args = None
urRef = None
simRef = None
gnGraph = None

event_obj_dict = dict()


def init(sim_args):
    import matplotlib.pyplot as plt
    numpy.random.seed(42)
    random.seed(42)
    global args
    args = sim_args
    m = plt.cm.get_cmap('Paired')
    global BACKBONE_LINKS_INF_CAPACITY
    if sim_args.unlimCoreLinkBandwidth:
        BACKBONE_LINKS_INF_CAPACITY = True
    for i in range(1, len(NAMES_EVENTS) + 1):
        COLORS_EVENTS.append(m(float(i) / (len(NAMES_EVENTS) + 1)))
    return None


def calcFairThroughput((link, index, nNewStreams)):
    res = link.capacity
    if len(link.netDataStreams) > 0 and not BACKBONE_LINKS_INF_CAPACITY:
        nStreams = len(link.netDataStreams) + nNewStreams
        share = link.capacity / nStreams
        for s in link.netDataStreams:
            if s.bottleneckLinkID != link.id and s.transmitRate < share:
                res -= s.transmitRate
                nStreams -= 1
        if nStreams != 0:
            res /= nStreams
    return res, index
