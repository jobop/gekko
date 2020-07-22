
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

import com.github.jobop.gekko.connector.GekkoNodeNettyClient;
import com.github.jobop.gekko.core.config.GekkoConfig;
import com.github.jobop.gekko.core.lifecycle.LifeCycleAdpter;
import com.github.jobop.gekko.core.metadata.NodeState;
import com.github.jobop.gekko.core.metadata.Peer;
import com.github.jobop.gekko.enums.RoleEnum;
import com.github.jobop.gekko.protocols.message.GekkoEntry;
import com.github.jobop.gekko.utils.NotifyableThread;
import com.lmax.disruptor.EventFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class EntriesSynchronizer extends LifeCycleAdpter {
    //<term,<peerId,writeIndex>>
    private Map<String, ConcurrentHashMap<String, Long>> peerTermWaterMarks = new HashMap<String, ConcurrentHashMap<String, Long>>();
    GekkoConfig conf;
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
    }

    private Long getQuorumIndex() {
        ConcurrentHashMap<String, Long> termWaterMarks = peerTermWaterMarks.get(state.getTerm());
        //sort and get the middle one index
        List<Long> sortedWaterMarks = termWaterMarks.values()
                .stream()
                .sorted(Comparator.reverseOrder())
                .collect(Collectors.toList());
        long quorumIndex = sortedWaterMarks.get(sortedWaterMarks.size() / 2);
        return quorumIndex;
    }


    public EntriesSynchronizer(GekkoConfig conf, GekkoNodeNettyClient client, NodeState state) {
        this.conf = conf;
        this.client = client;
        this.state = state;
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

        this.replicators.forEach(r -> r.accept(entry));
    }

    //TODO:
    public void addPeer(String peerId, Peer peer) {
    }

    //TODO:
    public void removePeer(String peerId) {
    }

    static class Replicator {
        private Peer peer;
        private String peerId;

        public Replicator(String peerId, Peer peer) {
            this.peerId = peerId;
            this.peer = peer;

        }

        //TODO:
        public void accept(GekkoEntry entry) {
        }
        //ditrupter queue

        class LongEventFactory implements EventFactory<EntryBatch> {
            @Override
            public EntryBatch newInstance() {
                return new EntryBatch();
            }
        }
    }

}
