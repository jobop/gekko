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


import com.alipay.remoting.InvokeCallback;
import com.alipay.remoting.exception.RemotingException;
import com.alipay.remoting.rpc.RpcClient;
import com.github.jobop.gekko.core.GekkoConfig;
import com.github.jobop.gekko.core.election.VoteCollector;
import com.github.jobop.gekko.core.lifecycle.LifeCycleAdpter;
import com.github.jobop.gekko.core.metadata.NodeState;
import com.github.jobop.gekko.core.metadata.Peer;
import com.github.jobop.gekko.protocols.GekkoNodeConnectProtocol;
import com.github.jobop.gekko.protocols.message.GekkoEntry;
import com.github.jobop.gekko.protocols.message.node.HeartBeatReq;
import com.github.jobop.gekko.protocols.message.node.PushEntryReq;
import com.github.jobop.gekko.protocols.message.node.VoteReq;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * the client to connect to orther nodes
 */
public class GekkoNodeNettyClient extends LifeCycleAdpter implements GekkoNodeConnectProtocol {
    GekkoConfig conf;
    NodeState nodeState;
    //    ConcurrentHashMap<String, RpcClient> orderNodesRpcClient = new ConcurrentHashMap<String, RpcClient>();
    RpcClient orderNodesRpcClient;
    static int WAIT_FOR_VOTE_TIME_OUT = 150;

    public GekkoNodeNettyClient(GekkoConfig conf, NodeState nodeState) {
        this.conf = conf;
        this.nodeState = nodeState;
    }

    @Override
    public void init() {
//        for (Map.Entry<String, Peer> e : this.nodeState.getPeersMap().entrySet()) {
//            String peerId = e.getKey();
//            Peer peer = e.getValue();
//
//            orderNodesRpcClient.put(peerId,new RpcClient())
//        }
        orderNodesRpcClient = new RpcClient();

    }

    @Override
    public void start() {
    }

    @Override
    public void shutdown() {
        orderNodesRpcClient.shutdown();
    }

    @Override
    public void sendHeartBeat() {
        for (Map.Entry<String, Peer> e : this.nodeState.getPeersMap().entrySet()) {
            String peerId = e.getKey();
            Peer peer = e.getValue();
            try {
                orderNodesRpcClient.oneway(peer.getHost() + ":" + peer.getPort(), HeartBeatReq.builder().remoteNodeId(nodeState.getSelfId()).termId(nodeState.getTerm()).build());
            } catch (RemotingException remotingException) {
                remotingException.printStackTrace();
            } catch (InterruptedException interruptedException) {
                interruptedException.printStackTrace();
            }
        }
    }

    @Override
    public void reqVote(VoteCollector voteCollector) {
        for (Map.Entry<String, Peer> e : this.nodeState.getPeersMap().entrySet()) {
            String peerId = e.getKey();
            Peer peer = e.getValue();
            try {
                //FIXME:
                orderNodesRpcClient.invokeWithCallback(peer.getHost() + ":" + peer.getPort(), VoteReq.builder().term(voteCollector.getVoteTerm()).candidateId(nodeState.getSelfId()).build(), voteCollector, WAIT_FOR_VOTE_TIME_OUT);
            } catch (RemotingException remotingException) {
                remotingException.printStackTrace();
            } catch (InterruptedException interruptedException) {
                interruptedException.printStackTrace();
            }
        }
    }

    @Override
    public void pushDatas(List<GekkoEntry> entries) {
        for (Map.Entry<String, Peer> e : this.nodeState.getPeersMap().entrySet()) {
            String peerId = e.getKey();
            Peer peer = e.getValue();
            //TODO:
            try {
                orderNodesRpcClient.oneway(peer.getHost() + ":" + peer.getPort(), PushEntryReq.builder().entries(entries).build());
            } catch (RemotingException remotingException) {
                remotingException.printStackTrace();
            } catch (InterruptedException interruptedException) {
                interruptedException.printStackTrace();
            }
        }
    }
}
