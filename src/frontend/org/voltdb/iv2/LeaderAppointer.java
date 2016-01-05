/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

import java.io.File;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.zookeeper_voltpatches.CreateMode;
import org.apache.zookeeper_voltpatches.KeeperException;
import org.apache.zookeeper_voltpatches.WatchedEvent;
import org.apache.zookeeper_voltpatches.Watcher;
import org.apache.zookeeper_voltpatches.ZooDefs.Ids;
import org.apache.zookeeper_voltpatches.ZooKeeper;
import org.json_voltpatches.JSONArray;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONStringer;
import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.HostMessenger;
import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.Pair;
import org.voltcore.zk.BabySitter;
import org.voltcore.zk.LeaderElector;
import org.voltcore.zk.ZKUtil;
import org.voltdb.Promotable;
import org.voltdb.SnapshotFormat;
import org.voltdb.TheHashinator;
import org.voltdb.VoltDB;
import org.voltdb.VoltZK;
import org.voltdb.catalog.SnapshotSchedule;
import org.voltdb.client.ClientResponse;
import org.voltdb.sysprocs.saverestore.SnapshotUtil;
import org.voltdb.sysprocs.saverestore.SnapshotUtil.SnapshotResponseHandler;

import com.google_voltpatches.common.collect.ImmutableMap;
import com.google_voltpatches.common.collect.ImmutableSortedSet;
import com.google_voltpatches.common.util.concurrent.SettableFuture;

/**
 * LeaderAppointer handles centralized appointment of partition leaders across
 * the partition.  This is primarily so that the leaders can be evenly
 * distributed throughout the cluster, reducing bottlenecks (at least at
 * startup).  As a side-effect, this service also controls the initial startup
 * of the cluster, blocking operation until each partition has a k-safe set of
 * replicas, each partition has a leader, and the MPI has started.
 */
public class LeaderAppointer implements Promotable
{
    private static final VoltLogger tmLog = new VoltLogger("TM");

    private enum AppointerState {
        INIT,           // Initial start state, used to inhibit ZK callback actions
        CLUSTER_START,  // indicates that we're doing the initial cluster startup
        DONE            // indicates normal running conditions, including repair
    }

    private final HostMessenger m_hostMessenger;
    private final ZooKeeper m_zk;
    // This should only be accessed through getInitialPartitionCount() on cluster startup.
    private final int m_initialPartitionCount;
    private final Map<Integer, BabySitter> m_partitionWatchers;
    private final LeaderCache m_iv2appointees;
    private final LeaderCache m_iv2masters;
    private final Map<Integer, PartitionCallback> m_callbacks;
    private final int m_kfactor;
    private final JSONObject m_topo;
    private final MpInitiator m_MPI;
    private final AtomicReference<AppointerState> m_state =
        new AtomicReference<AppointerState>(AppointerState.INIT);
    private SettableFuture<Object> m_startupLatch = null;
    private final boolean m_partitionDetectionEnabled;
    private boolean m_partitionDetected = false;
    private boolean m_usingCommandLog = false;
    private final AtomicBoolean m_replayComplete = new AtomicBoolean(false);
    private final boolean m_expectingDrSnapshot;
    private final AtomicBoolean m_snapshotSyncComplete = new AtomicBoolean(false);
    private final KSafetyStats m_stats;

    /*
     * Track partitions that are cleaned up during election/promotion etc.
     * This eliminates the race where the cleanup occurs while constructing babysitters
     * for partitions that end up being removed.
     */
    private HashSet<Integer> m_removedPartitionsAtPromotionTime = null;

    // Provide a single single-threaded executor service to all the BabySitters for each partition.
    // This will guarantee that the ordering of events generated by ZooKeeper is preserved in the
    // handling of callbacks in LeaderAppointer.
    private final ExecutorService m_es =
        CoreUtils.getCachedSingleThreadExecutor("LeaderAppointer-Babysitters", 15000);
    private final SnapshotSchedule m_partSnapshotSchedule;

    private final SnapshotResponseHandler m_snapshotHandler =
        new SnapshotResponseHandler() {
            @Override
            public void handleResponse(ClientResponse resp)
            {
                if (resp == null) {
                    VoltDB.crashLocalVoltDB("Received a null response to a snapshot initiation request.  " +
                            "This should be impossible.", true, null);
                }
                else if (resp.getStatus() != ClientResponse.SUCCESS) {
                    tmLog.info("Failed to complete partition detection snapshot, status: " + resp.getStatus() +
                            ", reason: " + resp.getStatusString());
                    tmLog.info("Retrying partition detection snapshot...");
                    SnapshotUtil.requestSnapshot(0L,
                            m_partSnapshotSchedule.getPath(),
                            m_partSnapshotSchedule.getPrefix() + System.currentTimeMillis(),
                            true, SnapshotFormat.NATIVE, null, m_snapshotHandler,
                            true);
                }
                else if (!SnapshotUtil.didSnapshotRequestSucceed(resp.getResults())) {
                    VoltDB.crashGlobalVoltDB("Unable to complete partition detection snapshot: " +
                        resp.getResults()[0], false, null);
                }
                else {
                    VoltDB.crashGlobalVoltDB("Partition detection snapshot completed. Shutting down.",
                            false, null);
                }
            }
        };

    private class PartitionCallback extends BabySitter.Callback
    {
        final int m_partitionId;
        final Set<Long> m_replicas;
        long m_currentLeader;

        /** Constructor used when we know (or think we know) who the leader for this partition is */
        PartitionCallback(int partitionId, long currentLeader)
        {
            this(partitionId);
            // Try to be clever for repair.  Create ourselves with the current leader set to
            // whatever is in the LeaderCache, and claim that replica exists, then let the
            // first run() call fix the world.
            m_currentLeader = currentLeader;
            m_replicas.add(currentLeader);
        }

        /** Constructor used at startup when there is no leader */
        PartitionCallback(int partitionId)
        {
            m_partitionId = partitionId;
            // A bit of a hack, but we should never end up with an HSID as Long.MAX_VALUE
            m_currentLeader = Long.MAX_VALUE;
            m_replicas = new HashSet<Long>();
        }

        @Override
        public void run(List<String> children)
        {
            List<Long> updatedHSIds = VoltZK.childrenToReplicaHSIds(children);
            // compute previously unseen HSId set in the callback list
            Set<Long> newHSIds = new HashSet<Long>(updatedHSIds);
            newHSIds.removeAll(m_replicas);
            tmLog.debug("Newly seen replicas: " + CoreUtils.hsIdCollectionToString(newHSIds));
            // compute previously seen but now vanished from the callback list HSId set
            Set<Long> missingHSIds = new HashSet<Long>(m_replicas);
            missingHSIds.removeAll(updatedHSIds);
            tmLog.debug("Newly dead replicas: " + CoreUtils.hsIdCollectionToString(missingHSIds));

            tmLog.debug("Handling babysitter callback for partition " + m_partitionId + ": children: " +
                    CoreUtils.hsIdCollectionToString(updatedHSIds));
            if (m_state.get() == AppointerState.CLUSTER_START) {
                // We can't yet tolerate a host failure during startup.  Crash it all
                if (missingHSIds.size() > 0) {
                    VoltDB.crashGlobalVoltDB("Node failure detected during startup.", false, null);
                }
                // ENG-3166: Eventually we would like to get rid of the extra replicas beyond k_factor,
                // but for now we just look to see how many replicas of this partition we actually expect
                // and gate leader assignment on that many copies showing up.
                int replicaCount = m_kfactor + 1;
                JSONArray parts;
                try {
                    parts = m_topo.getJSONArray("partitions");
                    for (int p = 0; p < parts.length(); p++) {
                        JSONObject aPartition = parts.getJSONObject(p);
                        int pid = aPartition.getInt("partition_id");
                        if (pid == m_partitionId) {
                            replicaCount = aPartition.getJSONArray("replicas").length();
                        }
                    }
                } catch (JSONException e) {
                    // Ignore and just assume the normal number of replicas
                }
                if (children.size() == replicaCount) {
                    m_currentLeader = assignLeader(m_partitionId, updatedHSIds);
                }
                else {
                    tmLog.info("Waiting on " + ((m_kfactor + 1) - children.size()) + " more nodes " +
                            "for k-safety before startup");
                }
            }
            else {
                Set<Integer> hostsOnRing = new HashSet<Integer>();
                // Check for k-safety
                if (!isClusterKSafe(hostsOnRing)) {
                    VoltDB.crashGlobalVoltDB("Some partitions have no replicas.  Cluster has become unviable.",
                            false, null);
                }
                // Check if replay has completed
                if (m_replayComplete.get() == false) {
                    VoltDB.crashGlobalVoltDB("Detected node failure during command log replay. Cluster will shut down.",
                                             false, null);
                }
                // If we are a DR replica and starting from a snapshot, check if that has completed
                if (m_expectingDrSnapshot && m_snapshotSyncComplete.get() == false) {
                    VoltDB.crashGlobalVoltDB("Detected node failure during DR snapshot sync. Cluster will shut down.",
                                             false, null);
                }
                // Check to see if there's been a possible network partition and we're not already handling it
                if (m_partitionDetectionEnabled && !m_partitionDetected) {
                    doPartitionDetectionActivities(hostsOnRing);
                }
                // If we survived the above gauntlet of fail, appoint a new leader for this partition.
                if (missingHSIds.contains(m_currentLeader)) {
                    m_currentLeader = assignLeader(m_partitionId, updatedHSIds);
                }
                // If this partition doesn't have a leader yet, and we have new replicas added,
                // elect a leader.
                if (m_currentLeader == Long.MAX_VALUE && !updatedHSIds.isEmpty()) {
                    m_currentLeader = assignLeader(m_partitionId, updatedHSIds);
                }
            }
            m_replicas.clear();
            m_replicas.addAll(updatedHSIds);
        }
    }

    /* We'll use this callback purely for startup so we can discover when all
     * the leaders we have appointed have completed their promotions and
     * published themselves to Zookeeper */
    LeaderCache.Callback m_masterCallback = new LeaderCache.Callback()
    {
        @Override
        public void run(ImmutableMap<Integer, Long> cache) {
            Set<Long> currentLeaders = new HashSet<Long>(cache.values());
            tmLog.debug("Updated leaders: " + currentLeaders);
            if (m_state.get() == AppointerState.CLUSTER_START) {
                try {
                    if (currentLeaders.size() == getInitialPartitionCount()) {
                        tmLog.debug("Leader appointment complete, promoting MPI and unblocking.");
                        m_state.set(AppointerState.DONE);
                        m_MPI.acceptPromotion();
                        m_startupLatch.set(null);
                    }
                } catch (IllegalAccessException e) {
                    // This should never happen
                    VoltDB.crashLocalVoltDB("Failed to get partition count", true, e);
                }
            }
        }
    };


    Watcher m_partitionCallback = new Watcher() {
        @Override
        public void process(WatchedEvent event)
        {
            m_es.submit(new Runnable() {
                @Override
                public void run()
                {
                    try {
                        List<String> children = m_zk.getChildren(VoltZK.leaders_initiators, m_partitionCallback);
                        tmLog.info("Noticed partition change " + children + ", " +
                                "currenctly watching " + m_partitionWatchers.keySet());
                        for (String child : children) {
                            int pid = LeaderElector.getPartitionFromElectionDir(child);
                            if (!m_partitionWatchers.containsKey(pid) && pid != MpInitiator.MP_INIT_PID) {
                                watchPartition(pid, m_es, false);
                            }
                        }
                        tmLog.info("Done " + m_partitionWatchers.keySet());
                    } catch (Exception e) {
                        VoltDB.crashLocalVoltDB("Cannot read leader initiator directory", false, e);
                    }
                }
            });
        }
    };

    public LeaderAppointer(HostMessenger hm, int numberOfPartitions,
            int kfactor, boolean partitionDetectionEnabled,
            SnapshotSchedule partitionSnapshotSchedule,
            boolean usingCommandLog,
            JSONObject topology, MpInitiator mpi,
            KSafetyStats stats, boolean expectingDrSnapshot)
    {
        m_hostMessenger = hm;
        m_zk = hm.getZK();
        m_kfactor = kfactor;
        m_topo = topology;
        m_MPI = mpi;
        m_initialPartitionCount = numberOfPartitions;
        m_callbacks = new HashMap<Integer, PartitionCallback>();
        m_partitionWatchers = new HashMap<Integer, BabySitter>();
        m_iv2appointees = new LeaderCache(m_zk, VoltZK.iv2appointees);
        m_iv2masters = new LeaderCache(m_zk, VoltZK.iv2masters, m_masterCallback);
        m_partitionDetectionEnabled = partitionDetectionEnabled;
        m_partSnapshotSchedule = partitionSnapshotSchedule;
        m_usingCommandLog = usingCommandLog;
        m_stats = stats;
        m_expectingDrSnapshot = expectingDrSnapshot;
        if (m_partitionDetectionEnabled) {
            if (!testPartitionDetectionDirectory(m_partSnapshotSchedule))
            {
                VoltDB.crashLocalVoltDB("Unable to create partition detection snapshot directory at " +
                        m_partSnapshotSchedule.getPath(), false, null);
            }
        }
    }

    @Override
    public void acceptPromotion() throws InterruptedException, ExecutionException
    {
        final SettableFuture<Object> blocker = SettableFuture.create();
        try {
            m_es.submit(new Runnable()  {
                @Override
                public void run() {
                    try {
                        acceptPromotionImpl(blocker);
                    } catch (Throwable t) {
                        blocker.setException(t);
                    }
                }
            });
            blocker.get();
        } catch (RejectedExecutionException e) {
            if (m_es.isShutdown()) return;
            throw new RejectedExecutionException(e);
        }
    }

    private void acceptPromotionImpl(final SettableFuture<Object> blocker) throws InterruptedException, ExecutionException, KeeperException {
        // Crank up the leader caches.  Use blocking startup so that we'll have valid point-in-time caches later.
        m_iv2appointees.start(true);
        m_iv2masters.start(true);
        ImmutableMap<Integer, Long> appointees = m_iv2appointees.pointInTimeCache();
        // Figure out what conditions we assumed leadership under.
        if (appointees.size() == 0)
        {
            tmLog.debug("LeaderAppointer in startup");
            m_state.set(AppointerState.CLUSTER_START);
        }
        //INIT is the default before promotion at runtime. Don't do this startup check
        //Let the rest of the promotion run and determine k-safety which is the else block.
        else if (m_state.get() == AppointerState.INIT && !VoltDB.instance().isRunning()) {
            ImmutableMap<Integer, Long> masters = m_iv2masters.pointInTimeCache();
            try {
                if ((appointees.size() < getInitialPartitionCount()) ||
                        (masters.size() < getInitialPartitionCount()) ||
                        (appointees.size() != masters.size())) {
                    // If we are promoted and the appointees or masters set is partial, the previous appointer failed
                    // during startup (at least for now, until we add remove a partition on the fly).
                    VoltDB.crashGlobalVoltDB("Detected failure during startup, unable to start", false, null);
                }
            } catch (IllegalAccessException e) {
                // This should never happen
                VoltDB.crashLocalVoltDB("Failed to get partition count", true, e);
            }
        }
        else {
            tmLog.debug("LeaderAppointer in repair");
            m_state.set(AppointerState.DONE);
        }

        if (m_state.get() == AppointerState.CLUSTER_START) {
            // Need to block the return of acceptPromotion until after the MPI is promoted.  Wait for this latch
            // to countdown after appointing all the partition leaders.  The
            // LeaderCache callback will count it down once it has seen all the
            // appointed leaders publish themselves as the actual leaders.
            m_startupLatch = SettableFuture.create();
            writeKnownLiveNodes(new HashSet<Integer>(m_hostMessenger.getLiveHostIds()));

            // Theoretically, the whole try/catch block below can be removed because the leader
            // appointer now watches the parent dir for any new partitions. It doesn't have to
            // create the partition dirs all at once, it can pick them up one by one as they are
            // created. But I'm too afraid to remove this block just before the release,
            // so leaving it here till later. - ning
            try {
                final int initialPartitionCount = getInitialPartitionCount();
                for (int i = 0; i < initialPartitionCount; i++) {
                    LeaderElector.createRootIfNotExist(m_zk,
                            LeaderElector.electionDirForPartition(VoltZK.leaders_initiators, i));
                    watchPartition(i, m_es, true);
                }
            } catch (IllegalAccessException e) {
                // This should never happen
                VoltDB.crashLocalVoltDB("Failed to get partition count on startup", true, e);
            }

            //Asynchronously wait for this to finish otherwise it deadlocks
            //on task that need to run on this thread
            m_startupLatch.addListener(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            m_zk.getChildren(VoltZK.leaders_initiators, m_partitionCallback);
                            blocker.set(null);
                        } catch (Throwable t) {
                            blocker.setException(t);
                        }
                    }
            },
            m_es);
        }
        else {
            // If we're taking over for a failed LeaderAppointer, we know when
            // we get here that every partition had a leader at some point in
            // time.  We'll seed each of the PartitionCallbacks for each
            // partition with the HSID of the last published leader.  The
            // blocking startup of the BabySitter watching that partition will
            // call our callback, get the current full set of replicas, and
            // appoint a new leader if the seeded one has actually failed
            Map<Integer, Long> masters = m_iv2masters.pointInTimeCache();
            tmLog.info("LeaderAppointer repairing with master set: " + CoreUtils.hsIdValueMapToString(masters));

            //Setting the map to non-null causes the babysitters to populate it when cleaning up partitions
            //We are only racing with ourselves in that the creation of a babysitter can trigger callbacks
            //that result in partitions being cleaned up. We don't have to worry about some other leader appointer.
            //The iteration order of the partitions doesn't matter
            m_removedPartitionsAtPromotionTime = new HashSet<Integer>();

            for (Entry<Integer, Long> master : masters.entrySet()) {

                //Skip processing the partition if it was cleaned up by a babysitter that was previously
                //instantiated
                if (m_removedPartitionsAtPromotionTime.contains(master.getKey())) {
                    tmLog.info("During promotion partition " + master.getKey() + " was cleaned up. Skipping.");
                    continue;
                }

                int partId = master.getKey();
                String dir = LeaderElector.electionDirForPartition(VoltZK.leaders_initiators, partId);
                m_callbacks.put(partId, new PartitionCallback(partId, master.getValue()));
                Pair<BabySitter, List<String>> sitterstuff =
                        BabySitter.blockingFactory(m_zk, dir, m_callbacks.get(partId), m_es);

                //We could get this far and then find out that creating this particular
                //babysitter triggered cleanup so we need to bail out here as well
                if (!m_removedPartitionsAtPromotionTime.contains(master.getKey())) {
                    m_partitionWatchers.put(partId, sitterstuff.getFirst());
                }
            }
            m_removedPartitionsAtPromotionTime = null;

            // just go ahead and promote our MPI
            m_MPI.acceptPromotion();
            // set up a watcher on the partitions dir so that new partitions will be picked up
            m_zk.getChildren(VoltZK.leaders_initiators, m_partitionCallback);
            blocker.set(null);
        }
    }

    /**
     * Watch the partition ZK dir in the leader appointer.
     *
     * This should be called on the elected leader appointer only. m_callbacks and
     * m_partitionWatchers are only accessed on initialization, promotion,
     * or elastic add node.
     *
     * @param pid The partition ID
     * @param es The executor service to use to construct the baby sitter
     * @param shouldBlock Whether or not to wait for the initial read of children
     * @throws KeeperException
     * @throws InterruptedException
     * @throws ExecutionException
     */
    void watchPartition(int pid, ExecutorService es, boolean shouldBlock)
        throws InterruptedException, ExecutionException
    {
        String dir = LeaderElector.electionDirForPartition(VoltZK.leaders_initiators, pid);
        m_callbacks.put(pid, new PartitionCallback(pid));
        BabySitter babySitter;

        if (shouldBlock) {
            babySitter = BabySitter.blockingFactory(m_zk, dir, m_callbacks.get(pid), es).getFirst();
        } else {
            babySitter = BabySitter.nonblockingFactory(m_zk, dir, m_callbacks.get(pid), es);
        }

        m_partitionWatchers.put(pid, babySitter);
    }

    private long assignLeader(int partitionId, List<Long> children)
    {
        // We used masterHostId = -1 as a way to force the leader choice to be
        // the first replica in the list, if we don't have some other mechanism
        // which has successfully overridden it.
        int masterHostId = -1;
        if (m_state.get() == AppointerState.CLUSTER_START) {
            try {
                // find master in topo
                JSONArray parts = m_topo.getJSONArray("partitions");
                for (int p = 0; p < parts.length(); p++) {
                    JSONObject aPartition = parts.getJSONObject(p);
                    int pid = aPartition.getInt("partition_id");
                    if (pid == partitionId) {
                        masterHostId = aPartition.getInt("master");
                    }
                }
            }
            catch (JSONException jse) {
                tmLog.error("Failed to find master for partition " + partitionId + ", defaulting to 0");
                jse.printStackTrace();
                masterHostId = -1; // stupid default
            }
        }
        else {
            // For now, if we're appointing a new leader as a result of a
            // failure, just pick the first replica in the children list.
            // Could eventually do something more complex here to try to keep a
            // semi-balance, but it's unclear that this has much utility until
            // we add rebalancing on rejoin as well.
            masterHostId = -1;
        }

        long masterHSId = children.get(0);
        for (Long child : children) {
            if (CoreUtils.getHostIdFromHSId(child) == masterHostId) {
                masterHSId = child;
                break;
            }
        }
        tmLog.info("Appointing HSId " + CoreUtils.hsIdToString(masterHSId) + " as leader for partition " +
                partitionId);
        try {
            m_iv2appointees.put(partitionId, masterHSId);
        }
        catch (Exception e) {
            VoltDB.crashLocalVoltDB("Unable to appoint new master for partition " + partitionId, true, e);
        }
        return masterHSId;
    }

    private void writeKnownLiveNodes(Set<Integer> liveNodes)
    {
        try {
            if (m_zk.exists(VoltZK.lastKnownLiveNodes, null) == null)
            {
                // VoltZK.createPersistentZKNodes should have done this
                m_zk.create(VoltZK.lastKnownLiveNodes, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
            JSONStringer stringer = new JSONStringer();
            stringer.object();
            stringer.key("liveNodes").array();
            for (Integer node : liveNodes) {
                stringer.value(node);
            }
            stringer.endArray();
            stringer.endObject();
            JSONObject obj = new JSONObject(stringer.toString());
            tmLog.debug("Writing live nodes to ZK: " + obj.toString(4));
            m_zk.setData(VoltZK.lastKnownLiveNodes, obj.toString(4).getBytes("UTF-8"), -1);
        } catch (Exception e) {
            VoltDB.crashLocalVoltDB("Unable to update known live nodes at ZK path: " +
                    VoltZK.lastKnownLiveNodes, true, e);
        }
    }

    private Set<Integer> readPriorKnownLiveNodes()
    {
        Set<Integer> nodes = new HashSet<Integer>();
        try {
            byte[] data = m_zk.getData(VoltZK.lastKnownLiveNodes, false, null);
            String jsonString = new String(data, "UTF-8");
            tmLog.debug("Read prior known live nodes: " + jsonString);
            JSONObject jsObj = new JSONObject(jsonString);
            JSONArray jsonNodes = jsObj.getJSONArray("liveNodes");
            for (int ii = 0; ii < jsonNodes.length(); ii++) {
                nodes.add(jsonNodes.getInt(ii));
            }
        } catch (Exception e) {
            VoltDB.crashLocalVoltDB("Unable to read prior known live nodes at ZK path: " +
                    VoltZK.lastKnownLiveNodes, true, e);
        }
        return nodes;
    }

    /*
     * Check if the directory specified for the snapshot on partition detection
     * exists, and has permissions set correctly.
     */
    private boolean testPartitionDetectionDirectory(SnapshotSchedule schedule) {
        if (m_partitionDetectionEnabled) {
            File partitionPath = new File(schedule.getPath());
            if (!partitionPath.exists()) {
                tmLog.error("Directory " + partitionPath + " for partition detection snapshots does not exist");
                return false;
            }
            if (!partitionPath.isDirectory()) {
                tmLog.error("Directory " + partitionPath + " for partition detection snapshots is not a directory");
                return false;
            }
            File partitionPathFile = new File(partitionPath, Long.toString(System.currentTimeMillis()));
            try {
                partitionPathFile.createNewFile();
                partitionPathFile.delete();
            } catch (IOException e) {
                tmLog.error(
                        "Could not create a test file in " +
                        partitionPath +
                        " for partition detection snapshots");
                e.printStackTrace();
                return false;
            }
            return true;
        } else {
            return true;
        }
    }

    /**
     * Given a set of the known host IDs before a fault, and the known host IDs in the
     * post-fault cluster, determine whether or not we think a network partition may have happened.
     * NOTE: this assumes that we have already done the k-safety validation for every partition and already failed
     * if we weren't a viable cluster.
     * ALSO NOTE: not private so it may be unit-tested.
     */
    static boolean makePPDDecision(Set<Integer> previousHosts, Set<Integer> currentHosts)
    {
        // Real partition detection stuff would go here
        // find the lowest hostId between the still-alive hosts and the
        // failed hosts. Which set contains the lowest hostId?
        int blessedHostId = Integer.MAX_VALUE;
        boolean blessedHostIdInFailedSet = true;

        // This should be all the pre-partition hosts IDs.  Any new host IDs
        // (say, if this was triggered by rejoin), will be greater than any surviving
        // host ID, so don't worry about including it in this search.
        for (Integer hostId : previousHosts) {
            if (hostId < blessedHostId) {
                blessedHostId = hostId;
            }
        }

        for (Integer hostId : currentHosts) {
            if (hostId.equals(blessedHostId)) {
                blessedHostId = hostId;
                blessedHostIdInFailedSet = false;
            }
        }

        // Evaluate PPD triggers.
        boolean partitionDetectionTriggered = false;
        // Exact 50-50 splits. The set with the lowest survivor host doesn't trigger PPD
        // If the blessed host is in the failure set, this set is not blessed.
        if (currentHosts.size() * 2 == previousHosts.size()) {
            if (blessedHostIdInFailedSet) {
                tmLog.info("Partition detection triggered for 50/50 cluster failure. " +
                        "This survivor set is shutting down.");
                partitionDetectionTriggered = true;
            }
            else {
                tmLog.info("Partition detected for 50/50 failure. " +
                        "This survivor set is continuing execution.");
            }
        }

        // A strict, viable minority is always a partition.
        if (currentHosts.size() * 2 < previousHosts.size()) {
            tmLog.info("Partition detection triggered. " +
                         "This minority survivor set is shutting down.");
            partitionDetectionTriggered = true;
        }

        return partitionDetectionTriggered;
    }

    private void doPartitionDetectionActivities(Set<Integer> currentNodes)
    {
        // We should never re-enter here once we've decided we're partitioned and doomed
        assert(!m_partitionDetected);

        Set<Integer> currentHosts = new HashSet<Integer>(currentNodes);
        Set<Integer> previousHosts = readPriorKnownLiveNodes();

        boolean partitionDetectionTriggered = makePPDDecision(previousHosts, currentHosts);

        if (partitionDetectionTriggered) {
            m_partitionDetected = true;
            if (m_usingCommandLog) {
                // Just shut down immediately
                VoltDB.crashGlobalVoltDB("Use of command logging detected, no additional database snapshot will " +
                        "be generated.  Please use the 'recover' action to restore the database if necessary.",
                        false, null);
            }
            else {
                SnapshotUtil.requestSnapshot(0L,
                        m_partSnapshotSchedule.getPath(),
                        m_partSnapshotSchedule.getPrefix() + System.currentTimeMillis(),
                        true, SnapshotFormat.NATIVE, null, m_snapshotHandler,
                        true);
            }
        }
        // If the cluster host set has changed, then write the new set to ZK
        // NOTE: we don't want to update the known live nodes if we've decided that our subcluster is
        // dying, otherwise a poorly timed subsequent failure might reverse this decision.  Any future promoted
        // LeaderAppointer should make their partition detection decision based on the pre-partition cluster state.
        else if (!currentHosts.equals(previousHosts)) {
            writeKnownLiveNodes(currentNodes);
        }
    }

    private boolean isClusterKSafe(Set<Integer> hostsOnRing)
    {
        boolean retval = true;
        List<String> partitionDirs = null;

        ImmutableSortedSet.Builder<KSafetyStats.StatsPoint> lackingReplication =
                ImmutableSortedSet.naturalOrder();

        try {
            partitionDirs = m_zk.getChildren(VoltZK.leaders_initiators, null);
        } catch (Exception e) {
            VoltDB.crashLocalVoltDB("Unable to read partitions from ZK", true, e);
        }

        //Don't fetch the values serially do it asynchronously
        Queue<ZKUtil.ByteArrayCallback> dataCallbacks = new ArrayDeque<ZKUtil.ByteArrayCallback>();
        Queue<ZKUtil.ChildrenCallback> childrenCallbacks = new ArrayDeque<ZKUtil.ChildrenCallback>();
        for (String partitionDir : partitionDirs) {
            String dir = ZKUtil.joinZKPath(VoltZK.leaders_initiators, partitionDir);
            try {
                ZKUtil.ByteArrayCallback callback = new ZKUtil.ByteArrayCallback();
                m_zk.getData(dir, false, callback, null);
                dataCallbacks.offer(callback);
                ZKUtil.ChildrenCallback childrenCallback = new ZKUtil.ChildrenCallback();
                m_zk.getChildren(dir, false, childrenCallback, null);
                childrenCallbacks.offer(childrenCallback);
            } catch (Exception e) {
                VoltDB.crashLocalVoltDB("Unable to read replicas in ZK dir: " + dir, true, e);
            }
        }
        final long statTs = System.currentTimeMillis();
        for (String partitionDir : partitionDirs) {
            int pid = LeaderElector.getPartitionFromElectionDir(partitionDir);

            String dir = ZKUtil.joinZKPath(VoltZK.leaders_initiators, partitionDir);
            try {
                // The data of the partition dir indicates whether the partition has finished
                // initializing or not. If not, the replicas may still be in the process of
                // adding themselves to the dir. So don't check for k-safety if that's the case.
                byte[] partitionState = dataCallbacks.poll().getData();
                boolean isInitializing = false;
                if (partitionState != null && partitionState.length == 1) {
                    isInitializing = partitionState[0] == LeaderElector.INITIALIZING;
                }

                List<String> replicas = childrenCallbacks.poll().getChildren();
                if (pid == MpInitiator.MP_INIT_PID) continue;
                final boolean partitionNotOnHashRing = partitionNotOnHashRing(pid);
                if (!isInitializing && replicas.isEmpty()) {
                    //These partitions can fail, just cleanup and remove the partition from the system
                    if (partitionNotOnHashRing) {
                        removeAndCleanupPartition(pid);
                        continue;
                    }
                    tmLog.fatal("K-Safety violation: No replicas found for partition: " + pid);
                    retval = false;
                } else if (!partitionNotOnHashRing) {
                    //Record host ids for all partitions that are on the ring
                    //so they are considered for partition detection
                    for (String replica : replicas) {
                        final String split[] = replica.split("/");
                        final long hsId = Long.valueOf(split[split.length - 1].split("_")[0]);
                        final int hostId = CoreUtils.getHostIdFromHSId(hsId);
                        hostsOnRing.add(hostId);
                    }
                }
                if (!isInitializing && !partitionNotOnHashRing) {
                    lackingReplication.add(
                            new KSafetyStats.StatsPoint(statTs, pid, m_kfactor + 1 - replicas.size())
                            );
                }
            }
            catch (Exception e) {
                VoltDB.crashLocalVoltDB("Unable to read replicas in ZK dir: " + dir, true, e);
            }
        }
        m_stats.setSafetySet(lackingReplication.build());

        return retval;
    }

    private void removeAndCleanupPartition(int pid) {
        tmLog.info("Removing and cleanup up partition info for partition " + pid);
        if (m_removedPartitionsAtPromotionTime != null) {
            m_removedPartitionsAtPromotionTime.add(pid);
            tmLog.info("Partition " + pid + " was cleaned up during LeaderAppointer promotion and should be skipped");
        }
        BabySitter sitter = m_partitionWatchers.remove(pid);
        if (sitter != null) {
            sitter.shutdown();
        }
        m_callbacks.remove(pid);
        try {
            ZKUtil.asyncDeleteRecursively(m_zk, ZKUtil.joinZKPath(VoltZK.iv2masters, String.valueOf(pid)));
            ZKUtil.asyncDeleteRecursively(m_zk, ZKUtil.joinZKPath(VoltZK.iv2appointees, String.valueOf(pid)));
            ZKUtil.asyncDeleteRecursively(m_zk, ZKUtil.joinZKPath(VoltZK.leaders_initiators, "partition_" + String.valueOf(pid)));
        } catch (Exception e) {
            tmLog.error("Error removing partition info", e);
        }
    }

    private static boolean partitionNotOnHashRing(int pid) {
        if (TheHashinator.getConfiguredHashinatorType() == TheHashinator.HashinatorType.LEGACY) return false;
        return TheHashinator.getRanges(pid).isEmpty();
    }

    /**
     * Gets the initial cluster partition count on startup. This can only be called during
     * initialization. Calling this after initialization throws, because the partition count may
     * not reflect the actual partition count in the cluster.
     *
     * @return
     */
    private int getInitialPartitionCount() throws IllegalAccessException
    {
        AppointerState currentState = m_state.get();
        if (currentState != AppointerState.INIT && currentState != AppointerState.CLUSTER_START) {
            throw new IllegalAccessException("Getting cached partition count after cluster " +
                    "startup");
        }
        return m_initialPartitionCount;
    }

    public void onReplayCompletion()
    {
        m_replayComplete.set(true);
    }

    public void onSyncSnapshotCompletion() {
        m_snapshotSyncComplete.set(true);
    }

    public void shutdown()
    {
        try {
            m_es.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        m_iv2appointees.shutdown();
                        m_iv2masters.shutdown();
                        for (BabySitter watcher : m_partitionWatchers.values()) {
                            watcher.shutdown();
                        }
                    } catch (Exception e) {
                        // don't care, we're going down
                    }
                }
            });
            m_es.shutdown();
            m_es.awaitTermination(356, TimeUnit.DAYS);
        }
        catch (InterruptedException e) {
            tmLog.warn("Unexpected interrupted exception", e);
        }
    }
}
