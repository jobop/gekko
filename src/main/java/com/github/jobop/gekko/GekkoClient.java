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
package com.github.jobop.gekko;


import com.alipay.remoting.exception.RemotingException;
import com.alipay.remoting.rpc.RpcClient;
import com.alipay.remoting.rpc.RpcResponseFuture;
import com.github.jobop.gekko.core.config.GekkoClientConfig;
import com.github.jobop.gekko.core.lifecycle.LifeCycleAdpter;
import com.github.jobop.gekko.core.metadata.Peer;
import com.github.jobop.gekko.enums.ResultEnums;
import com.github.jobop.gekko.protocols.message.api.GetMetadataReq;
import com.github.jobop.gekko.protocols.message.api.GetMetadataResp;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;

@Slf4j
public class GekkoClient extends LifeCycleAdpter {
    GekkoClientConfig config;
    RpcClient rpcClient;
    private volatile List<Peer> peers = new ArrayList<Peer>();

    public GekkoClient(GekkoClientConfig conf) {
        this.config = conf;
    }

    @Override
    public void init() {
        rpcClient = new RpcClient();
        rpcClient.startup();

        String[] peerArray = new String[this.config.getPeers().size()];
        this.config.getPeers().toArray(peerArray);

        for (int i = 0; i < peerArray.length; i++) {
            String peer = peerArray[i];
            String host = peer.split(":")[0];
            String port = peer.split(":")[1];
            this.peers.add(Peer.builder().host(host).port(Integer.valueOf(port)).build());
        }
    }

    @Override
    public void start() {


        //refresh peers
        for (Peer peer : this.peers) {
            try {
                RpcResponseFuture respFuture = rpcClient.invokeWithFuture(peer.getHost() + ":" + peer.getPort(), GetMetadataReq.builder().group(config.getGroup()).build(), 99999999);

                GetMetadataResp resp = (GetMetadataResp) respFuture.get();
                if (resp.getResult() == ResultEnums.SUCCESS) {
                    this.peers = resp.getPeers();
                    break;
                }
            } catch (RemotingException remotingException) {
                remotingException.printStackTrace();
//                log.warn("waiting the node " + peer.getHost() + ":" + peer.getPort() + " to connect!", remotingException);
            } catch (InterruptedException interruptedException) {
                interruptedException.printStackTrace();
            }
        }
    }

    @Override
    public void shutdown() {
        rpcClient.shutdown();
    }


}
