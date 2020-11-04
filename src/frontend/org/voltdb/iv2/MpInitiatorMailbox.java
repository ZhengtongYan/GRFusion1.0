/* This file is part of VoltDB.
 * Copyright (C) 2008-2020 VoltDB Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.iv2;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;

import org.voltcore.messaging.HostMessenger;
import org.voltcore.messaging.VoltMessage;
import org.voltdb.dtxn.TransactionState;
import org.voltdb.messaging.CompleteTransactionMessage;
import org.voltdb.messaging.Iv2InitiateTaskMessage;

import com.google_voltpatches.common.base.Supplier;
import com.google_voltpatches.common.base.Throwables;

/**
 * InitiatorMailbox accepts initiator work and proxies it to the
 * configured InitiationRole.
 */
public class MpInitiatorMailbox extends InitiatorMailbox
{
    private final LinkedBlockingQueue<Runnable> m_taskQueue = new LinkedBlockingQueue<Runnable>();
    @SuppressWarnings("serial")
    private static class TerminateThreadException extends RuntimeException {};
    private long m_taskThreadId = 0;
    private final MpRestartSequenceGenerator m_restartSeqGenerator;
    // LX: this task thread is used by MpInitiator to deliver msg
    private final Thread m_taskThread = new Thread(null,
                    new Runnable() {
                        @Override
                        public void run() {
                            m_taskThreadId = Thread.currentThread().getId();
                            while (true) {
                                try {
                                    // LX: take() is a blocking operation
                                    org.voltdb.VLog.GLog("MpInitiatorMailbox", "run", 55, "current size = " + m_taskQueue.size() + ", " + Thread.currentThread().getName() + " sleep or run");
                                    m_taskQueue.take().run();
                                    org.voltdb.VLog.GLog("MpInitiatorMailbox", "run", 57, "task thread done running??");
                                } catch (TerminateThreadException e) {
                                    break;
                                } catch (Exception e) {
                                    tmLog.error("Unexpected exception in MpInitiator deliver thread", e);
                                }
                            }
                        }
                    },
                    "MpInitiator deliver", 1024 * 128);

    private final LinkedBlockingQueue<Runnable> m_sendQueue = new LinkedBlockingQueue<Runnable>();
    private final Thread m_sendThread = new Thread(null,
                    new Runnable() {
                        @Override
                        public void run() {
                            while (true) {
                                try {
                                    org.voltdb.VLog.GLog("MpInitiatorMailbox", "run", 74, "current size = " + m_sendQueue.size() + ", " + Thread.currentThread().getName() + " sleep or run");
                                    m_sendQueue.take().run();
                                    org.voltdb.VLog.GLog("MpInitiatorMailbox", "run", 78, "send thread done running??");
                                } catch (TerminateThreadException e) {
                                    break;
                                } catch (Exception e) {
                                    tmLog.error("Unexpected exception in MpInitiator send thread", e);
                                }
                            }
                        }
                    },
                    "MpInitiator send", 1024 * 128);

    @Override
    public RepairAlgo constructRepairAlgo(final Supplier<List<Long>> survivors, int deadHost, final String whoami, boolean balanceSPI) {
        RepairAlgo ra = null;
        if (Thread.currentThread().getId() != m_taskThreadId) {
            FutureTask<RepairAlgo> ft = new FutureTask<RepairAlgo>(new Callable<RepairAlgo>() {
                @Override
                public RepairAlgo call() throws Exception {
                    RepairAlgo ra = new MpPromoteAlgo(survivors.get(), deadHost, MpInitiatorMailbox.this,
                            m_restartSeqGenerator, whoami, balanceSPI);
                    setRepairAlgoInternal(ra);
                    return ra;
                }
            });
            m_taskQueue.offer(ft);
            try {
                ra = ft.get();
            } catch (Exception e) {
                Throwables.propagate(e);
            }
        } else {
            ra = new MpPromoteAlgo(survivors.get(), deadHost, this, m_restartSeqGenerator, whoami, balanceSPI);
            setRepairAlgoInternal(ra);
        }
        return ra;
    }

    public void setLeaderState(final long maxSeenTxnId, final long repairTruncationHandle)
    {
        // LX: this permits only one at a time??
        org.voltdb.VLog.GLog("MpInitiator", "setLeaderState", 115, "insert into taskQueue");
        final CountDownLatch cdl = new CountDownLatch(1);
        m_taskQueue.offer(new Runnable() {
            @Override
            public void run() {
                try {
                    setLeaderStateInternal(maxSeenTxnId);
                    ((MpScheduler)m_scheduler).m_repairLogTruncationHandle = repairTruncationHandle;
                    ((MpScheduler)m_scheduler).m_repairLogAwaitingTruncate = repairTruncationHandle;
                } finally {
                    cdl.countDown();
                }
            }
        });
        try {
            cdl.await();
        } catch (InterruptedException e) {
            Throwables.propagate(e);
        }
    }

    @Override
    public void setMaxLastSeenMultipartTxnId(final long txnId) {
        org.voltdb.VLog.GLog("MpInitiator", "setMaxLastSeenMultipartTxnId", 138, "insert into taskQueue");
        final CountDownLatch cdl = new CountDownLatch(1);
        m_taskQueue.offer(new Runnable() {
            @Override
            public void run() {
                try {
                    setMaxLastSeenMultipartTxnIdInternal(txnId);
                } finally {
                    cdl.countDown();
                }
            }
        });
        try {
            cdl.await();
        } catch (InterruptedException e) {
            Throwables.propagate(e);
        }
    }


    @Override
    public void setMaxLastSeenTxnId(final long txnId) {
        org.voltdb.VLog.GLog("MpInitiator", "setMaxLastSeenTxnId", 160, "insert into taskQueue");
        final CountDownLatch cdl = new CountDownLatch(1);
        m_taskQueue.offer(new Runnable() {
            @Override
            public void run() {
                try {
                    setMaxLastSeenTxnIdInternal(txnId);
                } finally {
                    cdl.countDown();
                }
            }
        });
        try {
            cdl.await();
        } catch (InterruptedException e) {
            Throwables.propagate(e);
        }
    }

    @Override
    public void enableWritingIv2FaultLog() {
        org.voltdb.VLog.GLog("MpInitiator", "enableWritingIv2FaultLog", 181, "insert into taskQueue");
        final CountDownLatch cdl = new CountDownLatch(1);
        m_taskQueue.offer(new Runnable() {
            @Override
            public void run() {
                try {
                    enableWritingIv2FaultLogInternal();
                } finally {
                    cdl.countDown();
                }
            }
        });
        try {
            cdl.await();
        } catch (InterruptedException e) {
            Throwables.propagate(e);
        }
    }


    public MpInitiatorMailbox(int partitionId,
            MpScheduler scheduler,
            HostMessenger messenger, RepairLog repairLog,
            JoinProducerBase rejoinProducer)
    {
        super(partitionId, scheduler, messenger, repairLog, rejoinProducer);
        m_restartSeqGenerator = new MpRestartSequenceGenerator(scheduler.getLeaderId(), false);
        org.voltdb.VLog.GLog("MpInitiatorMailbox", "MpInitiatorMailbox", 200, "taskThread and sendThread created\n");
        m_taskThread.start();
        m_sendThread.start();
    }

    @Override
    public void shutdown() throws InterruptedException {
        org.voltdb.VLog.GLog("MpInitiator", "shutdown", 215, "insert into taskQueue/sendQueue");
        m_taskQueue.offer(new Runnable() {
            @Override
            public void run() {
                try {
                    shutdownInternal();
                } catch (InterruptedException e) {
                    tmLog.info("Interrupted during shutdown", e);
                }
            }
        });
        m_taskQueue.offer(new Runnable() {
            @Override
            public void run() {
                throw new TerminateThreadException();
            }
        });
        m_sendQueue.offer(new Runnable() {
            @Override
            public void run() {
                throw new TerminateThreadException();
            }
        });
        m_taskThread.join();
        m_sendThread.join();
    }

    @Override
    public long[] updateReplicas(final List<Long> replicas, final Map<Integer, Long> partitionMasters,
            TransactionState snapshotTransactionState) {
        org.voltdb.VLog.GLog("MpInitiator", "updateReplicas", 245, "insert into taskQueue");
        m_taskQueue.offer(new Runnable() {
            @Override
            public void run() {
                updateReplicasInternal(replicas, partitionMasters, snapshotTransactionState);
            }
        });
        return new long[0];
    }

    public void updateReplicas(final List<Long> replicas, final Map<Integer, Long> partitionMasters,
            boolean balanceSPI) {
        org.voltdb.VLog.GLog("MpInitiator", "updateReplicas", 257, "insert into taskQueue");
        m_taskQueue.offer(new Runnable() {
            @Override
            public void run() {
                assert(lockingVows());
                Iv2Trace.logTopology(getHSId(), replicas, m_partitionId);
                // If a replica set has been configured and it changed during
                // promotion, must cancel the term
                if (m_algo != null) {
                    m_algo.cancel();
                }
                ((MpScheduler)m_scheduler).updateReplicas(replicas, partitionMasters, balanceSPI);
            }
        });
    }

    @Override
    public void deliver(final VoltMessage message) {
        org.voltdb.VLog.GLog("MpInitiatorMailbox", "deliver", 275, "taskQueueSize = " + m_taskQueue.size());
        m_taskQueue.offer(new Runnable() {
            @Override
            public void run() {
                org.voltdb.VLog.GLog("MpInitiatorMailbox", "deliver", 266, "before deliverInternal");
                deliverInternal(message);
            }
        });
        org.voltdb.VLog.GLog("MpInitiatorMailbox", "deliver", 283, "taskQueueSize = " + m_taskQueue.size());
    }

    @Override
    void repairReplicasWith(final List<Long> needsRepair, final VoltMessage repairWork)
    {
        if (Thread.currentThread().getId() == m_taskThreadId) {
            //When called from MpPromoteAlgo which should be entered from deliver
            repairReplicasWithInternal(needsRepair, repairWork);
        } else {
            org.voltdb.VLog.GLog("MpInitiatorMailbox", "repairReplicasWith", 292, "insert into taskQueue");
            //When called from MpInitiator.acceptPromotion
            final CountDownLatch cdl = new CountDownLatch(1);
            m_taskQueue.offer(new Runnable() {
                @Override
                public void run() {
                    try {
                        repairReplicasWithInternal( needsRepair, repairWork);
                    } finally {
                        cdl.countDown();
                    }
                }
            });
            try {
                cdl.await();
            } catch (InterruptedException e) {
                Throwables.propagate(e);
            }
        }

    }

    private void repairReplicasWithInternal(List<Long> needsRepair, VoltMessage repairWork) {
        assert(lockingVows());
        org.voltdb.VLog.GLog("MpInitiatorMailbox", "repairReplicasWithInternal", 301, "");
        if (repairWork instanceof Iv2InitiateTaskMessage) {
            Iv2InitiateTaskMessage m = (Iv2InitiateTaskMessage)repairWork;
            Iv2InitiateTaskMessage work = new Iv2InitiateTaskMessage(m.getInitiatorHSId(), getHSId(), m);
            m_scheduler.updateLastSeenUniqueIds(work);
            m_scheduler.handleMessageRepair(needsRepair, work);
        }
        else if (repairWork instanceof CompleteTransactionMessage) {
            ((CompleteTransactionMessage) repairWork).setForReplica(false);
            send(com.google_voltpatches.common.primitives.Longs.toArray(needsRepair), repairWork);
        }
        else {
            throw new RuntimeException("During MPI repair: Invalid repair message type: " + repairWork);
        }
    }

    // This will be called from the internal task thread, deliver->deliverInternal->handleInitiateResponse
    // when the MpScheduler needs to log the completion of a transaction to its local repair log
    void deliverToRepairLog(VoltMessage msg) {
        assert(Thread.currentThread().getId() == m_taskThreadId);
        m_repairLog.deliver(msg);
    }

    // Change the send() behavior for the MPI's mailbox so that
    // messages sent from multiple read-only sites will
    // have a serialized order to all hosts.
    private void sendInternal(long destHSId, VoltMessage message)
    {
        message.m_sourceHSId = getHSId();
        m_messenger.send(destHSId, message);
    }

    @Override
    public void send(final long destHSId, final VoltMessage message)
    {
        org.voltdb.VLog.GLog("MpInitiatorMailbox", "send", 351, "insert into sendQueue");
        m_sendQueue.offer(new Runnable() {
            @Override
            public void run() {
                sendInternal(destHSId, message);
            }
        });
    }

    private void sendInternal(long[] destHSIds, VoltMessage message)
    {
        message.m_sourceHSId = getHSId();
        m_messenger.send(destHSIds, message);
    }

    @Override
    public void send(final long[] destHSIds, final VoltMessage message)
    {
        org.voltdb.VLog.GLog("MpInitiatorMailbox", "send", 369, "insert into sendQueue");
        m_sendQueue.offer(new Runnable() {
            @Override
            public void run() {
                sendInternal(destHSIds, message);
            }
        });
    }
}
