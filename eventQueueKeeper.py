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

from decorations import printInfo
import treap

# этот класс следит будет общим
# между фунцией, которая станет процессом и мэйн
# я буду вызывать enqueue() и dequeue(), которые по-сути будут
# записывать в локальную копию нужное значение, затем в фоне
# обновлять в ремоут потоке и по запросу (dequeue)
# будут возвращать мне нужные данные

# in the remote thread:
#   while true()
#       check


class eventQueueKeeper(object):
    def __init__(self, commLock):
        self.eventQueue = treap.treap()
        self.keepRunning = True
        self.commLock = commLock
        printInfo("eventQueueKeeper is started")

    def run(self):
        while self.keepRunning:
            if inPipe.poll():
                inc_ev, extra = inPipe.recv()
                if outQueue.empty():
                    if eventQueue:
                        prevNextEv = eventQueue.find_min()
                        outQueue.put(prevNextEv)
                        eventQueue.remove(prevNextEv)
                    else:
                        prevNextEv = None


                if eventQueue:
                    e = eventQueue.find_min()
                    nextEv = e
                    eventQueue.remove(e)
                else:
                    nextEv = None
                # add new event
                if extra is None:
                    if nextEv is None:
                        nextEv = inc_ev
                        commLock.release()
                        continue
                    elif nextEv < inc_ev:
                        commLock.release()
                        eventQueue[inc_ev] = inc_ev
                        continue
                    else:
                        tmpEv = nextEv
                        nextEv = inc_ev
                        commLock.release()
                        eventQueue[tmpEv] = tmpEv
                        continue
                else: # perform some action on event
                    action, val = extra
                    if action == sg.ACTION_UPDATE:
                        if nextEv is None:
                            commLock.release()
                            eventQueue.remove(inc_ev)
                            inc_ev.time = val
                            eventQueue[inc_ev] = inc_ev
                            continue
                        else:
                            oldEv = inc_ev
                            inc_ev.time = val
                            if nextEv < inc_ev:
                                commLock.release()
                                eventQueue.remove(oldEv)
                                eventQueue[inc_ev] = inc_ev
                                continue
                            else:
                                tmpEv = nextEv
                                nextEv = inc_ev
                                commLock.release()
                                eventQueue[tmpEv] = tmpEv
                                continue
                    elif action == sg.ACTION_DELETE:
                        if nextEv == inc_ev:
                            nextEv = None
                            commLock.release()
                            continue
                        else:
                            commLock.release()
                            eventQueue.remove(inc_ev)
                            continue
                    elif action == sg.ACTION_STOP:
                        if not eventQueue:
                            commLock.release()
                            printInfo("stop eventQueueKeeper")
                            keepRunning = False
                            break
            if eventQueue and nextEv is None:
                nextEv = eventQueue.find_min()
                eventQueue.remove(nextEv)

        printInfo("eventQueueKeeper finished successfully")