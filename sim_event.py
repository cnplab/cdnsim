"""
    CDNSim

file: sim_event.py

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

import sim_globals as sg


class event:
    __slots__ = ['time', 'objRef_id', 'type', 'eid']

    def __init__(self, tim, objRef_id, typ, objRef=None, eid=None):
        self.time = tim
        self.objRef_id = objRef_id
        self.type = typ
        self.eid = eid
        if objRef is not None:
            sg.event_obj_dict[objRef_id] = objRef
            self.eid = sg.globalEventID
            sg.globalEventID += 1
        return

    def __lt__(self, other):
        return (self.time, self.eid) < (other.time, other.eid)

    def __ge__(self, other):
        return (self.time, self.eid) >= (other.time, other.eid)

    def __eq__(self, other):
        return (self.time, self.eid) == (other.time, other.eid)

    def __str__(self):
        return 'Event t={0:.4f}, id='.format(self.time) + str(self.eid)

    def __getstate__(self):
        return self.time, self.objRef_id, self.type, self.eid

    def __setstate__(self, (tim, objRef_id, typ, eid)):
        self.__init__(tim, objRef_id, typ, None, eid)
        return None
