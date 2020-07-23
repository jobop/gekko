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


import com.alipay.remoting.Connection;
import com.alipay.remoting.config.switches.GlobalSwitch;
import com.alipay.remoting.exception.RemotingException;
import com.alipay.remoting.rpc.RpcConfigs;
import com.alipay.remoting.rpc.RpcServer;
import com.github.jobop.gekko.connector.processors.*;
import com.github.jobop.gekko.core.config.GekkoConfig;
import com.github.jobop.gekko.core.election.GekkoLeaderElector;
import com.github.jobop.gekko.core.lifecycle.LifeCycleAdpter;
import com.github.jobop.gekko.core.metadata.NodeState;
import com.github.jobop.gekko.core.metadata.Peer;
import com.github.jobop.gekko.enums.RoleEnum;
import com.github.jobop.gekko.protocols.GekkoInboundProtocol;
import com.github.jobop.gekko.protocols.message.api.RefreshPeersReq;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * This class use to handle all the requests from another node and client sides
 */
@Slf4j
public class GekkoNettyServer extends LifeCycleAdpter {
    GekkoInboundProtocol inboundHelper;
    NodeState nodeState;
    GekkoConfig conf;
    RpcServer nodeRpcServer;

    RpcServer apiRpcServer;
    GekkoLeaderElector elector;

    public GekkoNettyServer(GekkoConfig conf, GekkoInboundProtocol inboundHelper, NodeState nodeState, GekkoLeaderElector elector) {
        this.inboundHelper = inboundHelper;
        this.conf = conf;
        this.nodeState = nodeState;
        this.elector = elector;
    }

    public void init() {
        System.setProperty(RpcConfigs.DISPATCH_MSG_LIST_IN_DEFAULT_EXECUTOR, "false");

        Peer selfPeer = this.nodeState.getPeersMap().get(conf.getSelfId());
        nodeRpcServer = new RpcServer(selfPeer.getNodePort());
        apiRpcServer = new RpcServer(selfPeer.getApiPort());
        apiRpcServer.switches().turnOn(GlobalSwitch.SERVER_MANAGE_CONNECTION_SWITCH);


        nodeRpcServer.registerUserProcessor(new PullEntryProcessor(inboundHelper));
        nodeRpcServer.registerUserProcessor(new HeartBeatProcessor(inboundHelper, elector));
        nodeRpcServer.registerUserProcessor(new PushEntriesProcessor(inboundHelper, elector));
        nodeRpcServer.registerUserProcessor(new PreReqVoteProcessor(inboundHelper, elector));
        nodeRpcServer.registerUserProcessor(new ReqVoteProcessor(inboundHelper, elector));
        nodeRpcServer.registerUserProcessor(new GetMetadataProcessor(inboundHelper, elector));
        nodeRpcServer.registerUserProcessor(new ProbeProcessor(inboundHelper, elector));


        nodeRpcServer.switches().turnOn(GlobalSwitch.SERVER_MANAGE_CONNECTION_SWITCH);


        //api
        apiRpcServer.registerUserProcessor(new PullEntryProcessor(inboundHelper));
        apiRpcServer.registerUserProcessor(new AppendEntryProcessor(inboundHelper, elector));
        apiRpcServer.registerUserProcessor(new GetMetadataProcessor(inboundHelper, elector));

    }

    public void start() {
        nodeRpcServer.startup();
        apiRpcServer.startup();
    }

    public void shutdown() {
        nodeRpcServer.shutdown();
        apiRpcServer.shutdown();
    }

    public void reFreshPeersToClients() {
        if (nodeState.getRole() != RoleEnum.LEADER) {
            log.warn("not a leader cannot push peers info to clients!");
            return;
        }
        List<String> peerIds = new ArrayList<>();
        List<Peer> peers = new ArrayList<>();
        for (Map.Entry<String, Peer> e : nodeState.getPeersMap().entrySet()) {
            peerIds.add(e.getKey());
            peers.add(e.getValue());
        }
        for (Map.Entry<String, List<Connection>> e : apiRpcServer.getConnectionManager().getAll().entrySet()) {
            for (Connection connection : e.getValue()) {

                try {
                    apiRpcServer.oneway(connection, RefreshPeersReq.builder().leaderId(nodeState.getLeaderId()).peersMap(nodeState.getPeersMap()).term(nodeState.getTerm()).build());
                } catch (RemotingException remotingException) {
                    log.warn("push to client " + connection.getRemoteAddress().getHostString() + " not success!");
                }
            }
        }
    }
}
