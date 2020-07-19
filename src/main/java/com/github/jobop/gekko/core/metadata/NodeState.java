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
 * Created by CuttleFish on 2020/7/3.
 */

package com.github.jobop.gekko.core.metadata;

import com.github.jobop.gekko.core.GekkoConfig;
import com.github.jobop.gekko.core.exception.GekkoException;
import com.github.jobop.gekko.core.lifecycle.LifeCycleAdpter;
import com.github.jobop.gekko.enums.ResultEnums;
import com.github.jobop.gekko.enums.RoleEnum;
import lombok.Data;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;


@Data
public class NodeState extends LifeCycleAdpter {
    private GekkoConfig config;

    public NodeState(GekkoConfig config) {
        this.config = config;
    }

    private String selfId;
    private volatile String leaderId;
    private volatile RoleEnum role;
    private volatile AtomicLong termAtomic;
    private volatile long writeId;
    private volatile long commitId;
    private volatile Map<String, Peer> peersMap = new ConcurrentHashMap<String, Peer>();

    public void init() {
        this.selfId = this.config.getSelfId();
        this.leaderId = "";
        this.role = RoleEnum.FOLLOWER;
        this.termAtomic = new AtomicLong(-1);
        this.writeId = -1;
        this.commitId = -1;
        if (this.config.getPeers() == null || this.config.getPeerIds() == null) {
            throw new GekkoException(ResultEnums.PEER_OR_PEERID_CANNOT_BE_NULL);
        }
        if (this.config.getPeers().size() != this.config.getPeerIds().size()) {
            throw new GekkoException(ResultEnums.PEER_AND_PEERID_SIZE_NOT_MATCH);
        }
        String[] peerArray = new String[this.config.getPeers().size()];
        String[] peerIdArray = new String[this.config.getPeerIds().size()];
        this.config.getPeers().toArray(peerArray);
        this.config.getPeerIds().toArray(peerIdArray);

        for (int i = 0; i < peerArray.length; i++) {
            String peer = peerArray[i];
            String host = peer.split(":")[0];
            String port = peer.split(":")[1];
            this.peersMap.put(peerIdArray[i], Peer.builder().host(host).port(Integer.valueOf(port)).build());
        }

    }

    public long getTerm() {
        return termAtomic.get();
    }


}
