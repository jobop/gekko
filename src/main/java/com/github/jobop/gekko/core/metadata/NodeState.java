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

import com.github.jobop.gekko.core.config.GekkoConfig;
import com.github.jobop.gekko.core.exception.GekkoException;
import com.github.jobop.gekko.core.lifecycle.LifeCycleAdpter;
import com.github.jobop.gekko.enums.ResultEnums;
import com.github.jobop.gekko.enums.RoleEnum;
import com.github.jobop.gekko.utils.FileUtils;
import com.github.jobop.gekko.utils.IOUtils;
import com.github.jobop.gekko.utils.NotifyableThread;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;


@Data
@Slf4j
public class NodeState extends LifeCycleAdpter {
    private static String LAST_CHECK_SUM_KEY = "LAST_CHECK_SUM_KEY";
    private static String PRE_CHECK_SUM_KEY = "PRE_CHECK_SUM_KEY";
    private static String COMMITTED_INDEX_KEY = "COMMITTED_INDEX_KEY";
    private static String WROTTEN_INDEX_KEY = "WROTTEN_INDEX_KEY";

    private GekkoConfig config;

    public NodeState(GekkoConfig config) {
        this.config = config;
    }

    private String group;
    private String selfId;
    private volatile String leaderId;
    private volatile RoleEnum role;
    private volatile AtomicLong termAtomic;
    private volatile long writeId;
    private volatile long commitId;
    private volatile long preChecksum;
    private volatile long lastChecksum;
    //avoid the inode thich cannot connect to the leader send a vote or prevote to it,make it cannot handle the append push from the leader
    private volatile long lastCommunityToLeaderTime;

    private volatile Map<String, Peer> peersMap = new ConcurrentHashMap<String, Peer>();

    NotifyableThread saveCheckPointThread;

    public void init() {
        this.group = this.config.getGroup();
        this.selfId = this.config.getSelfId();
        this.leaderId = this.config.getLeaderId();
        this.role = RoleEnum.FOLLOWER;
        this.termAtomic = new AtomicLong(-1);
        this.writeId = 0;
        this.commitId = 0;
        if (this.config.getPeers() == null || this.config.getPeerIds() == null) {
            throw new GekkoException(ResultEnums.PEER_OR_PEERID_CANNOT_BE_NULL);
        }
        if (this.config.getPeers().size() != this.config.getPeerIds().size()) {
            throw new GekkoException(ResultEnums.PEER_AND_PEERID_SIZE_NOT_MATCH);
        }

        if (this.config.getPeerApiPorts().size() != this.config.getPeerIds().size()) {
            throw new GekkoException(ResultEnums.PEER_AND_API_PORTS_SIZE_NOT_MATCH);
        }
        String[] peerArray = new String[this.config.getPeers().size()];
        String[] peerIdArray = new String[this.config.getPeerIds().size()];
        Integer[] peerApiPortArray = new Integer[this.config.getPeerApiPorts().size()];
        this.config.getPeers().toArray(peerArray);
        this.config.getPeerIds().toArray(peerIdArray);
        this.config.getPeerApiPorts().toArray(peerApiPortArray);

        for (int i = 0; i < peerArray.length; i++) {
            String peer = peerArray[i];
            String host = peer.split(":")[0];
            String port = peer.split(":")[1];
            Integer apiPort = peerApiPortArray[i];
            this.peersMap.put(peerIdArray[i], Peer.builder().host(host).nodePort(Integer.valueOf(port)).apiPort(apiPort).build());
        }

        recoverCheckPoint();

        this.saveCheckPointThread = new NotifyableThread(config.getSaveCheckPointInterval(), TimeUnit.SECONDS, "saveCheckPointThread") {

            @Override
            public void doWork() {
                saveCheckPoint();
            }
        };

    }

    @Override
    public void start() {
        super.start();
        this.saveCheckPointThread.start();
    }

    @Override
    public void shutdown() {
        saveCheckPointThread.shutdown();
    }

    public void saveCheckPoint() {
        Properties properties = new Properties();
        properties.put(LAST_CHECK_SUM_KEY, this.getLastChecksum());
        properties.put(PRE_CHECK_SUM_KEY, this.getPreChecksum());
        properties.put(COMMITTED_INDEX_KEY, this.getCommitId());
        properties.put(WROTTEN_INDEX_KEY, this.getWriteId());
        String data = IOUtils.properties2String(properties);
        try {
            IOUtils.string2File(data, config.getBaseFilePath() + File.separator + "nodeState.checkpoint");
        } catch (IOException e) {
            log.warn("saveCheckPoint fail!", e);
        }


    }

    public void recoverCheckPoint() {
        try {
            String data = IOUtils.file2String(config.getBaseFilePath() + File.separator + "nodeState.checkpoint");
            Properties properties = IOUtils.string2Properties(data);
            this.setLastChecksum(Long.valueOf(properties.getProperty(LAST_CHECK_SUM_KEY)));
            this.setPreChecksum(Long.valueOf(properties.getProperty(PRE_CHECK_SUM_KEY)));
            this.setCommitId(Long.valueOf(properties.getProperty(COMMITTED_INDEX_KEY)));
            this.setWriteId(Long.valueOf(properties.getProperty(WROTTEN_INDEX_KEY)));

        } catch (Throwable t) {

        }
    }

    public long getTerm() {
        return termAtomic.get();
    }


}
