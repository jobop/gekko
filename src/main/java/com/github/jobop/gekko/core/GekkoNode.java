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
package com.github.jobop.gekko.core;


import com.github.jobop.gekko.connector.GekkoInboundMsgHelper;
import com.github.jobop.gekko.core.election.GekkoLeaderElector;
import com.github.jobop.gekko.connector.GekkoNettyServer;
import com.github.jobop.gekko.connector.GekkoNodeNettyClient;
import com.github.jobop.gekko.core.lifecycle.LifeCycleAdpter;
import com.github.jobop.gekko.core.metadata.NodeState;
import com.github.jobop.gekko.core.replication.EntriesPusher;
import com.github.jobop.gekko.core.statemachine.StateMachine;
import com.github.jobop.gekko.enums.StoreEnums;
import com.github.jobop.gekko.protocols.GekkoInboundProtocol;
import com.github.jobop.gekko.store.FileStore;
import com.github.jobop.gekko.store.MemoryStore;
import com.github.jobop.gekko.store.RockDbStore;
import com.github.jobop.gekko.store.Store;


public class GekkoNode extends LifeCycleAdpter {
    NodeState nodeState;
    GekkoNettyServer server;
    GekkoNodeNettyClient nodeClient;
    GekkoConfig conf;
    GekkoInboundProtocol inboundHelper;
    Store store;
    StateMachine stateMachine;
    GekkoLeaderElector elector;
    EntriesPusher pusher;

    public GekkoNode(GekkoConfig conf) {
        this.conf = conf;
        this.nodeState = new NodeState(this.conf);
        if (conf.getStoreType() == StoreEnums.MEMORY) {
            this.store = new MemoryStore(conf);
        } else if (conf.getStoreType() == StoreEnums.FILE) {
            this.store = new FileStore(conf);
        } else if (conf.getStoreType() == StoreEnums.ROCKDB) {
            this.store = new RockDbStore(conf);
        }

        this.stateMachine = conf.getStateMachine();

        this.nodeClient = new GekkoNodeNettyClient(conf, nodeState);
        this.elector = new GekkoLeaderElector(conf, nodeClient, nodeState);
        this.pusher=new EntriesPusher(conf, nodeClient, nodeState);
        this.inboundHelper = new GekkoInboundMsgHelper(this.store, this.stateMachine);
        this.server = new GekkoNettyServer(conf, this.inboundHelper, this.nodeState,this.elector);



    }


    @Override
    public void init() {
        this.nodeState.init();
        this.store.init();
        this.server.init();
        this.nodeClient.init();
        this.elector.init();
        this.pusher.init();
    }

    @Override
    public void start() {
        this.nodeState.start();
        this.store.start();
        this.server.start();
        this.nodeClient.start();
        this.elector.start();
        this.pusher.start();
    }

    @Override
    public void shutdown() {
        this.nodeState.shutdown();
        this.store.shutdown();
        this.server.shutdown();
        this.nodeClient.shutdown();
        this.elector.shutdown();
        this.pusher.shutdown();
    }
}
