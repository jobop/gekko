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
 * Created by CuttleFish on 2020/7/2.
 */

package com.github.jobop.gekko.connector;


import com.github.jobop.gekko.core.metadata.NodeState;
import com.github.jobop.gekko.core.replication.EntriesSynchronizer;
import com.github.jobop.gekko.core.statemachine.StateMachine;
import com.github.jobop.gekko.enums.PushResultEnums;
import com.github.jobop.gekko.protocols.GekkoInboundProtocol;
import com.github.jobop.gekko.protocols.message.GekkoEntry;
import com.github.jobop.gekko.protocols.message.api.*;
import com.github.jobop.gekko.protocols.message.node.*;
import com.github.jobop.gekko.store.Store;

import java.util.List;
import java.util.function.Consumer;


public class GekkoInboundMsgHelper implements GekkoInboundProtocol {
    Store store;
    StateMachine stateMachine;
    NodeState nodeState;

    EntriesSynchronizer entriesSynchronizer;

    public GekkoInboundMsgHelper(Store store, StateMachine stateMachine, NodeState nodeState, EntriesSynchronizer entriesSynchronizer) {
        this.store = store;
        this.stateMachine = stateMachine;
        this.nodeState = nodeState;
        this.entriesSynchronizer = entriesSynchronizer;
    }

    /**
     * from flower or user
     *
     * @param req
     * @return
     */
    @Override
    public PullEntryResp handleGetEntries(PullEntryReq req) {
        List<GekkoEntry> entries = store.batchGetByIndex(req.getFromIndex(), req.getToIndex());
        return PullEntryResp.builder().enries(entries).build();
    }

    /**
     * from user
     *
     * @param req
     * @return
     */
    @Override
    public void handleAppendEntry(AppendEntryReq req, Consumer consumer) {
        GekkoEntry entry = req.getGekkoEntry();
        store.append(entry);
        if (entry.getPos() == -1) {
            consumer.accept(entry);
            return;
        }

        entriesSynchronizer.append(entry, consumer);
    }

    /**
     * from flower or user
     *
     * @param req
     * @return
     */
    @Override
    public GetMetadataResp handleGetMetadata(GetMetadataReq req) {
        return null;
    }


    /**
     * from nodes
     *
     * @param req
     * @return
     */
    @Override
    public VoteResp handleVote(VoteReq req) {
        return null;
    }

    /**
     * from nodes
     *
     * @param req
     * @return
     */
    @Override
    public HeartBeatResp handleHeartBeat(HeartBeatReq req) {
        return null;
    }

    /**
     * TODO:
     * from leader
     *
     * @param req
     * @return
     */
    @Override
    public synchronized PushEntryResp handlePushDatas(PushEntryReq req) {
        GekkoEntry entry = req.getEntries().get(0);
        if (this.nodeState.getCommitId() >= entry.getEntryIndex()) {
            return PushEntryResp.builder().acceptNodeId(nodeState.getSelfId()).index(entry.getEntryIndex()).term(nodeState.getTerm()).result(PushResultEnums.AGREE).build();
        }
        if (this.nodeState.getWriteId() >= entry.getEntryIndex()) {
            GekkoEntry oleEntry = this.store.getByIndex(entry.getEntryIndex());
            if (oleEntry.getChecksum() == entry.getChecksum()) {
                return PushEntryResp.builder().acceptNodeId(nodeState.getSelfId()).index(entry.getEntryIndex()).term(nodeState.getTerm()).result(PushResultEnums.AGREE).build();
            }
        }

        //normal
        if (this.nodeState.getLastChecksum() == req.getPreCheckSum()) {
            nodeState.setCommitId(req.getLastCommitIndex());
            this.store.append(entry);
            if (entry.getPos() != -1) {
                this.nodeState.setLastChecksum(entry.getChecksum());
                return PushEntryResp.builder().acceptNodeId(nodeState.getSelfId()).index(entry.getEntryIndex()).term(nodeState.getTerm()).result(PushResultEnums.AGREE).build();
            } else {
                return PushEntryResp.builder().acceptNodeId(nodeState.getSelfId()).term(nodeState.getTerm()).result(PushResultEnums.REJECT).build();
            }

        } else {
            //roll back their uncommitted and batch pull from leader
            //roll back
            long fromIndex = nodeState.getCommitId();
            long toIndex = entry.getEntryIndex();
            this.store.trimAfter(fromIndex);
            List<GekkoEntry> entries = this.store.batchGetByIndex(fromIndex, toIndex);
            for (GekkoEntry e : entries) {
                this.store.append(e);
            }
            return PushEntryResp.builder().acceptNodeId(nodeState.getSelfId()).index(entry.getEntryIndex()).term(nodeState.getTerm()).result(PushResultEnums.AGREE).build();
        }

    }
}
