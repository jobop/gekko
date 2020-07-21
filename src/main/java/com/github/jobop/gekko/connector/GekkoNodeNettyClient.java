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


import com.alipay.remoting.exception.RemotingException;
import com.alipay.remoting.rpc.RpcClient;
import com.alipay.remoting.rpc.RpcResponseFuture;
import com.github.jobop.gekko.core.GekkoNode;
import com.github.jobop.gekko.core.config.GekkoConfig;
import com.github.jobop.gekko.core.election.PreVoteCollector;
import com.github.jobop.gekko.core.election.VoteCollector;
import com.github.jobop.gekko.core.lifecycle.LifeCycleAdpter;
import com.github.jobop.gekko.core.metadata.NodeState;
import com.github.jobop.gekko.core.metadata.Peer;
import com.github.jobop.gekko.core.replication.AcceptCollector;
import com.github.jobop.gekko.enums.RoleEnum;
import com.github.jobop.gekko.protocols.GekkoNodeConnectProtocol;
import com.github.jobop.gekko.protocols.message.GekkoEntry;
import com.github.jobop.gekko.protocols.message.api.PullEntryReq;
import com.github.jobop.gekko.protocols.message.api.PullEntryResp;
import com.github.jobop.gekko.protocols.message.node.HeartBeatReq;
import com.github.jobop.gekko.protocols.message.node.PreVoteReq;
import com.github.jobop.gekko.protocols.message.node.PushEntryReq;
import com.github.jobop.gekko.protocols.message.node.VoteReq;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * the client to connect to orther nodes
 */
@Slf4j
public class GekkoNodeNettyClient extends LifeCycleAdpter implements GekkoNodeConnectProtocol {
    GekkoConfig conf;
    NodeState nodeState;
    //    ConcurrentHashMap<String, RpcClient> orderNodesRpcClient = new ConcurrentHashMap<String, RpcClient>();
    RpcClient orderNodesRpcClient;
    static int WAIT_FOR_VOTE_TIME_OUT = 150;

    static int WAIT_FOR_PUSH_TIME_OUT = 150;

    public GekkoNodeNettyClient(GekkoConfig conf, NodeState nodeState) {
        this.conf = conf;
        this.nodeState = nodeState;
    }

    @Override
    public void init() {
        orderNodesRpcClient = new RpcClient();

    }

    @Override
    public void start() {
        orderNodesRpcClient.startup();
    }

    @Override
    public void shutdown() {
        orderNodesRpcClient.shutdown();
    }

    @Override
    public void sendHeartBeat() {
        if (this.nodeState.getRole() != RoleEnum.LEADER) {
            log.warn("not a leader can not send hearbeat! the role is " + this.nodeState.getRole());
            return;
        }
        for (Map.Entry<String, Peer> e : this.nodeState.getPeersMap().entrySet()) {
            String peerId = e.getKey();
            if (peerId.equals(this.nodeState.getSelfId())) {
                continue;
            }
            Peer peer = e.getValue();
            try {
                orderNodesRpcClient.oneway(peer.getHost() + ":" + peer.getNodePort(), HeartBeatReq.builder().group(nodeState.getGroup()).remoteNodeId(nodeState.getSelfId()).term(nodeState.getTerm()).build());
            } catch (RemotingException remotingException) {
                log.warn("waiting the node " + peer.getHost() + ":" + peer.getNodePort() + " to connect!");
            } catch (InterruptedException interruptedException) {
                interruptedException.printStackTrace();
            }
        }
    }


    @Override
    public void preVote(PreVoteCollector preVoteCollector) {
        for (Map.Entry<String, Peer> e : this.nodeState.getPeersMap().entrySet()) {
            String peerId = e.getKey();
            if (peerId.equals(this.nodeState.getSelfId())) {
                continue;
            }
            Peer peer = e.getValue();
            try {
                orderNodesRpcClient.invokeWithCallback(peer.getHost() + ":" + peer.getNodePort(), PreVoteReq.builder().group(nodeState.getGroup()).term(preVoteCollector.getVoteTerm()).candidateId(nodeState.getSelfId()).lastIndex(nodeState.getCommitId()).build(), preVoteCollector, WAIT_FOR_VOTE_TIME_OUT);
            } catch (RemotingException remotingException) {
                log.warn("waiting for " + peer.getHost() + ":" + peer.getNodePort() + " to connect!");
            } catch (InterruptedException interruptedException) {
                interruptedException.printStackTrace();
            }
        }
    }

    @Override
    public void reqVote(VoteCollector voteCollector) {
        for (Map.Entry<String, Peer> e : this.nodeState.getPeersMap().entrySet()) {
            String peerId = e.getKey();
            if (peerId.equals(this.nodeState.getSelfId())) {
                continue;
            }
            Peer peer = e.getValue();
            try {
                //FIXME:
                orderNodesRpcClient.invokeWithCallback(peer.getHost() + ":" + peer.getNodePort(), VoteReq.builder().group(nodeState.getGroup()).term(voteCollector.getVoteTerm()).candidateId(nodeState.getSelfId()).lastIndex(nodeState.getCommitId()).build(), voteCollector, WAIT_FOR_VOTE_TIME_OUT);
            } catch (RemotingException remotingException) {
                remotingException.printStackTrace();
            } catch (InterruptedException interruptedException) {
                interruptedException.printStackTrace();
            }
        }
    }

    @Override
    public void pushDatas(List<GekkoEntry> entries, AcceptCollector callback) {
        if (this.nodeState.getRole() != RoleEnum.LEADER) {
            log.warn("not a leader can not push data!");
            return;
        }
        for (Map.Entry<String, Peer> e : this.nodeState.getPeersMap().entrySet()) {
            String peerId = e.getKey();
            if (peerId.equals(this.nodeState.getSelfId())) {
                continue;
            }
            Peer peer = e.getValue();
            //TODO:
            try {
                orderNodesRpcClient.invokeWithCallback(peer.getHost() + ":" + peer.getNodePort(), PushEntryReq.builder().group(nodeState.getGroup()).entries(entries).remoteNodeId(nodeState.getSelfId()).term(nodeState.getTerm()).lastCommitIndex(nodeState.getCommitId()).preCheckSum(nodeState.getPreChecksum()).build(), callback, WAIT_FOR_PUSH_TIME_OUT);
            } catch (RemotingException remotingException) {
                remotingException.printStackTrace();
            } catch (InterruptedException interruptedException) {
                interruptedException.printStackTrace();
            }
        }
    }

    @Override
    public List<GekkoEntry> pullEntriesByFollower(long fromIndex, long toIndex) {
        Peer leaderPeer = nodeState.getPeersMap().get(nodeState.getLeaderId());
        try {
            RpcResponseFuture future = this.orderNodesRpcClient.invokeWithFuture(leaderPeer.getHost() + ":" + leaderPeer.getNodePort(), PullEntryReq.builder().fromIndex(fromIndex).toIndex(toIndex).build(), 5000);
            PullEntryResp reps = (PullEntryResp) future.get();
            return reps.getEnries();
        } catch (RemotingException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return new ArrayList<GekkoEntry>();
    }
}
