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


import com.alipay.remoting.rpc.RpcConnectionFactory;
import com.alipay.remoting.rpc.RpcServer;
import com.github.jobop.gekko.connector.processors.GetEntryProcessor;
import com.github.jobop.gekko.core.GekkoConfig;
import com.github.jobop.gekko.core.lifecycle.LifeCycleAdpter;
import com.github.jobop.gekko.core.metadata.NodeState;
import com.github.jobop.gekko.core.metadata.Peer;
import com.github.jobop.gekko.protocols.GekkoInboundProtocol;

/**
 * This class use to handle all the requests from another node and client sides
 */
public class GekkoNettyServer extends LifeCycleAdpter {
    GekkoInboundProtocol inboundHelper;
    NodeState nodeState;
    GekkoConfig conf;
    RpcServer rpcServer;

    public GekkoNettyServer(GekkoConfig conf, GekkoInboundProtocol inboundHelper, NodeState nodeState) {
        this.inboundHelper = inboundHelper;
        this.conf = conf;
        this.nodeState = nodeState;
    }

    public void init() {
        Peer selfPeer = this.nodeState.getPeersMap().get(conf.getSelfId());
        rpcServer = new RpcServer(selfPeer.getPort());

        rpcServer.registerUserProcessor(new GetEntryProcessor(inboundHelper));
    }

    public void start() {
        rpcServer.startup();
    }

    public void shutdown() {
        rpcServer.shutdown();
    }
}
