
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Created by CuttleFish on 2020/7/19.
 */

package com.github.jobop.gekko.core.replication;

import com.alipay.remoting.InvokeCallback;
import com.github.jobop.gekko.connector.GekkoNodeNettyClient;
import com.github.jobop.gekko.core.config.GekkoConfig;
import com.github.jobop.gekko.core.lifecycle.LifeCycleAdpter;
import com.github.jobop.gekko.core.metadata.NodeState;
import com.github.jobop.gekko.core.metadata.Peer;
import com.github.jobop.gekko.enums.ResultEnums;
import com.github.jobop.gekko.enums.RoleEnum;
import com.github.jobop.gekko.protocols.message.GekkoEntry;
import com.github.jobop.gekko.protocols.message.node.ProbeResp;
import com.github.jobop.gekko.protocols.message.node.PushEntryResp;
import com.github.jobop.gekko.store.Store;
import com.github.jobop.gekko.utils.NotifyableThread;
import com.github.jobop.gekko.utils.Utils;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.Collectors;

@Slf4j
public class EntriesSynchronizer extends LifeCycleAdpter {
    //<term,<peerId,writeIndex>>
    private Map<String, ConcurrentHashMap<String, Long>> peerTermWaterMarks = new HashMap<String, ConcurrentHashMap<String, Long>>();
    GekkoConfig conf;
    Store store;
    GekkoNodeNettyClient client;
    NodeState state;
    NotifyableThread commitIndexUpdateThread;
    private static int COMMIT_INDEX_UPDATE_INTERVAL = 1000;
    private List<Replicator> replicators = new ArrayList<Replicator>();


    @Override
    public void init() {
        commitIndexUpdateThread = new NotifyableThread(COMMIT_INDEX_UPDATE_INTERVAL, TimeUnit.MILLISECONDS, "commitIndexUpdateThread") {
            @Override
            public void doWork() {
                state.setCommitId(getQuorumIndex());
            }
        };
        state.getPeersMap().forEach((k, v) -> {
            replicators.add(new Replicator(k, v));
        });


    }

    @Override
    public void start() {
        commitIndexUpdateThread.start();
    }

    @Override
    public void shutdown() {
        commitIndexUpdateThread.shutdown();
        replicators.forEach(r -> r.shutdown());
    }

    public void triggerProbes() {
        replicators.forEach(r -> r.start());
    }

    public void stopProbes() {
        replicators.forEach(r -> r.stop());
    }

    public void accept(GekkoEntry entry) {
        replicators.forEach(r -> r.accept(entry));
    }

    private Long getQuorumIndex() {
        ConcurrentHashMap<String, Long> termWaterMarks = peerTermWaterMarks.get(state.getTerm());
        if (null == termWaterMarks) {
            return -1l;
        }
        //sort and get the middle one index
        List<Long> sortedWaterMarks = termWaterMarks.values()
                .stream()
                .sorted(Comparator.reverseOrder())
                .collect(Collectors.toList());
        long quorumIndex = sortedWaterMarks.get(sortedWaterMarks.size() / 2);
        return quorumIndex;
    }


    public EntriesSynchronizer(GekkoConfig conf, GekkoNodeNettyClient client, NodeState state, Store store) {
        this.conf = conf;
        this.client = client;
        this.state = state;
        this.store = store;

    }

    public void append(GekkoEntry entry, Consumer callback) {
        List<GekkoEntry> entries = new ArrayList<GekkoEntry>();
        entries.add(entry);
        client.pushDatas(entries, new AcceptCollector(entry, this.state, callback));
    }


    public void push(GekkoEntry entry) {
        if (state.getRole() != RoleEnum.LEADER) {
            return;
        }


//        this.replicators.forEach(r -> r.accept(entry));
    }

    //TODO:
    public void addPeer(String peerId, Peer peer) {
    }

    //TODO:
    public void removePeer(String peerId) {
    }

    class Replicator extends LifeCycleAdpter {
        private Peer peer;
        private String peerId;
        GekkoConfig conf;
        NodeState state;

        NotifyableThread replicateThread;
        volatile long nexReplicateIndex = 0;
        volatile boolean needProbe = true;

        List<PenddingEntryBatch> penddingQueue = new ArrayList<>();

        PenddingEntryBatch lastPenddingEntryBatch;
        volatile AtomicBoolean start = new AtomicBoolean(false);


        public Replicator(String peerId, Peer peer) {
            this.peerId = peerId;
            this.peer = peer;
            replicateThread = new NotifyableThread(conf.getEntriesPushInterval(), TimeUnit.MILLISECONDS, "Replicator-" + peerId) {
                @Override
                public void doWork() {
                    if (!start.get()) {
                        log.info("the replicator has stop!");
                        return;
                    }
                    if (!state.isLeader()) {
                        return;
                    }
                    if (needProbe) {
                        probe();
                    } else {
                        push();
                    }

                }
            };
            replicateThread.start();
        }


        private void push() {
            //TODO:
            long fromIndex = getnextIndex() + 1;
            long maxIndex = store.getMaxIndex();
            long endIndex = (maxIndex - fromIndex) > conf.getEntriesPushMaxCount() ?
                    fromIndex + conf.getEntriesPushMaxCount() : maxIndex;
            GekkoEntry preEntry = store.getByIndex(getnextIndex());
            List<GekkoEntry> entries = store.batchGetByIndex(fromIndex, endIndex);
            if (null == entries) {
                return;
            }
            PenddingEntryBatch penddingEntry = PenddingEntryBatch.builder().
                    startIndex(entries.get(0).getEntryIndex()).
                    endIndex(entries.get(entries.size() - 1).getEntryIndex()).
                    preCommitIndex(preEntry.getEntryIndex()).
                    preCheckSum(preEntry.getChecksum()).
                    entries(entries).
                    preCommitIndex(state.getCommitId()).count(entries.size()).build();

            penddingQueue.add(penddingEntry);
            lastPenddingEntryBatch = penddingEntry;

            client.pushDatas(peer, penddingEntry, new InvokeCallback() {
                @Override
                public void onResponse(Object result) {
                    PushEntryResp resp = (PushEntryResp) result;
                    long remoteIndex = resp.getIndex();
                    //FIXME:why reject?
                    switch (resp.getResult()) {
                        case REJECT:
                            long remoteTerm = resp.getTerm();
                            nexReplicateIndex = remoteIndex;
                            penddingQueue.clear();
                            return;
                        default:
                            if (remoteIndex > nexReplicateIndex) {
                                nexReplicateIndex = remoteIndex;
                                peerTermWaterMarks.getOrDefault(state.getTerm(), new ConcurrentHashMap<String, Long>()).putIfAbsent(peerId, nexReplicateIndex);
                            }
                            penddingQueue.remove(penddingEntry);
                            return;
                    }
                    //TODO:
                }

                @Override
                public void onException(Throwable e) {
                    //TODO:
                    penddingQueue.clear();
                    lastPenddingEntryBatch = null;
                    log.error("", e);
                }

                @Override
                public Executor getExecutor() {
                    return Utils.GOABL_DEFAULT_THREAD_POOL;
                }
            });


        }

        private long getnextIndex() {
            if (penddingQueue.isEmpty()) {
                return nexReplicateIndex;
            } else {
                return lastPenddingEntryBatch.getEndIndex();
            }
        }

        private void probe() {
            if (!start.get()) {
                log.info("the replicator has stop!");
                return;
            }

            if (state.getRole() != RoleEnum.LEADER) {
                return;
            }

            client.sendProbe(peer, new InvokeCallback() {
                @Override
                public void onResponse(Object result) {
                    ProbeResp resp = (ProbeResp) result;
                    if (resp.getResult() == ResultEnums.SUCCESS) {
                        if (resp.getWrotenIndex() > nexReplicateIndex) {
                            nexReplicateIndex = resp.getWrotenIndex();
                            needProbe = false;
                        }

                    } else {
                        Utils.invokeWithDefaultExcutor(() -> probe());
                    }
                }

                @Override
                public void onException(Throwable e) {
                    log.error("send probe to " + peerId + " fail", e);
                    Utils.invokeWithDefaultExcutor(() -> probe());
                }

                @Override
                public Executor getExecutor() {
                    return Utils.GOABL_DEFAULT_THREAD_POOL;
                }
            });
        }


        public void start() {
            penddingQueue.clear();
            this.lastPenddingEntryBatch = null;
            if (start.compareAndSet(false, true)) {

                needProbe = true;
            }

        }

        public void stop() {
            start.compareAndSet(true, false);

        }

        @Override
        public void shutdown() {
            replicateThread.shutdown();

        }

        public void accept(GekkoEntry entry) {
            if (nexReplicateIndex == -1) {
                return;
            }
            if (entry.getEntryIndex() - nexReplicateIndex > conf.getEntriesPushMaxCount()) {
                replicateThread.trigger();
            }
        }

    }

}
