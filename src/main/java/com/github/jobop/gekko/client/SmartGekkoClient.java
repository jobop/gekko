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
package com.github.jobop.gekko.client;


import com.alipay.remoting.exception.RemotingException;
import com.alipay.remoting.rpc.RpcClient;
import com.alipay.remoting.rpc.RpcResponseFuture;
import com.github.jobop.gekko.connector.processors.ClientRefreshPeersProcessor;
import com.github.jobop.gekko.core.config.GekkoClientConfig;
import com.github.jobop.gekko.core.exception.GekkoException;
import com.github.jobop.gekko.core.lifecycle.LifeCycleAdpter;
import com.github.jobop.gekko.core.metadata.Peer;
import com.github.jobop.gekko.enums.ResultEnums;
import com.github.jobop.gekko.protocols.message.GekkoEntry;
import com.github.jobop.gekko.protocols.message.api.AppendEntryReq;
import com.github.jobop.gekko.protocols.message.api.GetMetadataReq;
import com.github.jobop.gekko.protocols.message.api.GetMetadataResp;
import com.github.jobop.gekko.utils.PreConditions;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Data
public class SmartGekkoClient extends LifeCycleAdpter {
    GekkoClientConfig config;
    RpcClient rpcClient;
    private volatile Map<String, Peer> peersMap = new ConcurrentHashMap<String, Peer>();
    private volatile String leaderPeerId;

    public SmartGekkoClient(GekkoClientConfig conf) {
        this.config = conf;
    }

    @Override
    public void init() {
        rpcClient = new RpcClient();

        rpcClient.registerUserProcessor(new ClientRefreshPeersProcessor(this));


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
            this.peersMap.put(peerIdArray[i], Peer.builder().host(host).apiPort(Integer.valueOf(port)).build());
        }

    }

    @Override
    public void start() {
        rpcClient.startup();

        //refresh peers
        boolean startSuccess = false;
        for (Map.Entry<String, Peer> e : this.peersMap.entrySet()) {
            Peer peer = e.getValue();
            try {
                RpcResponseFuture respFuture = rpcClient.invokeWithFuture(peer.getHost() + ":" + peer.getApiPort(), GetMetadataReq.builder().group(config.getGroup()).build(), config.getConnectTimeout());

                GetMetadataResp resp = (GetMetadataResp) respFuture.get(config.getReadTimeout());
                if (resp.getResult() == ResultEnums.SUCCESS) {
                    this.peersMap = resp.getPeersMap();
                    this.leaderPeerId = resp.getLeaderId();
                    startSuccess = true;
                    break;
                }
            } catch (RemotingException remotingException) {
                remotingException.printStackTrace();
//                log.warn("waiting the node " + peer.getHost() + ":" + peer.getPort() + " to connect!", remotingException);
            } catch (InterruptedException interruptedException) {
                interruptedException.printStackTrace();
            }
        }

        PreConditions.check(startSuccess, ResultEnums.CLIENT_LOAD_FAIL);
    }


    public RpcResponseFuture append(byte[] data) {
        Peer leaderPeer = this.getPeersMap().get(this.leaderPeerId);

        GekkoEntry entry = GekkoEntry.builder().data(data).magic(0xCAFE_BABE).build();
        RpcResponseFuture respFuture = null;
        try {
            respFuture = rpcClient.invokeWithFuture(leaderPeer.getHost() + ":" + leaderPeer.getApiPort(), AppendEntryReq.builder().gekkoEntry(entry).build(), config.getConnectTimeout());
        } catch (RemotingException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return respFuture;
    }

    @Override
    public void shutdown() {
        rpcClient.shutdown();
    }


}
