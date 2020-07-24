
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
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.Collectors;

@Slf4j
public class EntriesSynchronizer extends LifeCycleAdpter {
    //<term,<peerId,writeIndex>>
    private Map<Long, ConcurrentHashMap<String, Long>> peerTermWaterMarks = new HashMap<Long, ConcurrentHashMap<String, Long>>();
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
            if (k.equals(state.getSelfId())) {
                return;
            }
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
//        peerTermWaterMarks.getOrDefault(entry.getTerm(), new ConcurrentHashMap<String, Long>()).put(state.getSelfId(), entry.getEntryIndex());

        replicators.forEach(r -> r.start());
    }

    public void stopProbes() {
        replicators.forEach(r -> r.stop());
    }

    public void accept(GekkoEntry entry) {
        ConcurrentHashMap peerMap = peerTermWaterMarks.getOrDefault(entry.getTerm(), new ConcurrentHashMap<String, Long>());
        peerMap.put(state.getSelfId(), entry.getEntryIndex());
        peerTermWaterMarks.put(entry.getTerm(), peerMap);

        replicators.forEach(r -> r.accept(entry));
    }

    private Long getQuorumIndex() {
        ConcurrentHashMap<String, Long> termWaterMarks = peerTermWaterMarks.get(state.getTerm());
        //当前期还没有产生数据，用之前的
        if (null == termWaterMarks) {
            return state.getCommitId();
        }
        //sort and get the middle one index
        List<Long> sortedWaterMarks = termWaterMarks.values()
                .stream()
                .sorted(Comparator.reverseOrder())
                .collect(Collectors.toList());
        //少于一半人情况已知
        if (sortedWaterMarks.size() < (state.getPeersMap().size() / 2) + 1) {
            return state.getCommitId();
        } else {
            //大于等于一半人情况已知
            long quorumIndex = sortedWaterMarks.get(state.getPeersMap().size() / 2);
            return quorumIndex;
        }


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

        NotifyableThread replicateThread;
        volatile long nexReplicateIndex = 0;
        volatile boolean needProbe = true;

        List<PenddingEntryBatch> penddingQueue = new ArrayList<>();

        PenddingEntryBatch lastPenddingEntryBatch;
        volatile AtomicBoolean start = new AtomicBoolean(false);


        public Replicator(String peerId, Peer peer) {
            this.peerId = peerId;
            this.peer = peer;
            //TODO:
            replicateThread = new NotifyableThread(conf.getEntriesPushInterval(), TimeUnit.MILLISECONDS, "Replicator-" + peerId) {
                @Override
                public void doWork() {
                    if (!start.get()) {
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
            long fromIndex = getnextIndex();
            long maxIndex = store.getMaxIndex();
            if (maxIndex < fromIndex) {
                return;
            }

            long endIndex = (maxIndex - fromIndex) > conf.getEntriesPushMaxCount() ?
                    fromIndex + conf.getEntriesPushMaxCount() : maxIndex;
            GekkoEntry preEntry = store.getByIndex(fromIndex - 1);
            List<GekkoEntry> entries = store.batchGetByIndex(fromIndex, endIndex+1);
            if (null == entries || entries.isEmpty()) {
                return;
            }
            long preCheckSum = 0;
            if (null != preEntry) {
                preCheckSum = preEntry.getChecksum();
            }
            log.info("###need push index from " + fromIndex + " to " + endIndex);

            PenddingEntryBatch penddingEntry = PenddingEntryBatch.builder().
                    startIndex(entries.get(0).getEntryIndex()).
                    endIndex(entries.get(entries.size() - 1).getEntryIndex()).
                    preCheckSum(preCheckSum).
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
                                ConcurrentHashMap peerMap = peerTermWaterMarks.getOrDefault(state.getTerm(), new ConcurrentHashMap<String, Long>());
                                peerMap.put(peerId, remoteIndex - 1);
                                peerTermWaterMarks.put(state.getTerm(), peerMap);
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
                    log.error("push data exception", e);
                }

                @Override
                public Executor getExecutor() {
                    return Executors.newSingleThreadExecutor();
                }
            });


        }

        private long getnextIndex() {
            if (penddingQueue.isEmpty()) {
                return nexReplicateIndex;
            } else {
                return lastPenddingEntryBatch.getEndIndex() + 1;
            }
        }

        private void probe() {
            if (!start.get()) {
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
                        if (resp.getTerm() != state.getTerm()) {
                            return;
                        }
                        log.info("### probe remoteNextId=" + resp.getNextIndex() + " remoteTerm=" + resp.getTerm() + " localIndex=" + state.getWriteId());

                        ConcurrentHashMap peerMap = peerTermWaterMarks.getOrDefault(resp.getTerm(), new ConcurrentHashMap<String, Long>());
                        peerMap.put(peerId, resp.getCommitIndex());
                        peerTermWaterMarks.put(state.getTerm(), peerMap);

                        if (resp.getNextIndex() > state.getWriteId() + 1) {
                            //TODO:如果对方比主大，则要truncate它
                            nexReplicateIndex = state.getCommitId() + 1;
                            needProbe = false;

                        } else if (resp.getNextIndex() >= nexReplicateIndex) {
                            nexReplicateIndex = resp.getNextIndex();
                            needProbe = false;
                        }
                    }
                }

                @Override
                public void onException(Throwable e) {
                    log.error("send probe to " + peerId + " fail", e);
                }

                @Override
                public Executor getExecutor() {
                    return Executors.newSingleThreadExecutor();
                }
            });
        }


        public void start() {
            clear();
            if (start.compareAndSet(false, true)) {

                needProbe = true;
            }

        }

        private void clear() {
            this.penddingQueue.clear();
            this.lastPenddingEntryBatch = null;
            this.nexReplicateIndex = 0;
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
