
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
 * Created by CuttleFish on 2020/7/20.
 */

package com.github.jobop.gekko.connector.processors;

import com.alipay.remoting.AsyncContext;
import com.alipay.remoting.BizContext;
import com.github.jobop.gekko.core.election.GekkoLeaderElector;
import com.github.jobop.gekko.core.metadata.NodeState;
import com.github.jobop.gekko.core.metadata.Peer;
import com.github.jobop.gekko.enums.ResultEnums;
import com.github.jobop.gekko.protocols.GekkoInboundProtocol;
import com.github.jobop.gekko.protocols.message.api.GetMetadataReq;
import com.github.jobop.gekko.protocols.message.api.GetMetadataResp;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
public class GetMetadataProcessor extends DefaultProcessor<GetMetadataReq> {
    GekkoLeaderElector elector;

    public GetMetadataProcessor(GekkoInboundProtocol helper, GekkoLeaderElector elector) {
        super(helper);
        this.elector = elector;
    }

    @Override
    public void handleRequest(BizContext bizCtx, AsyncContext asyncCtx, GetMetadataReq request) {
        NodeState nodeState = elector.getState();
        List<String> peerIds = new ArrayList<>();
        List<Peer> peers = new ArrayList<>();
        for (Map.Entry<String, Peer> e : nodeState.getPeersMap().entrySet()) {
            peerIds.add(e.getKey());
            peers.add(e.getValue());
        }
        asyncCtx.sendResponse(GetMetadataResp.builder().result(ResultEnums.SUCCESS).leaderId(nodeState.getLeaderId()).peersMap(nodeState.getPeersMap()).term(nodeState.getTerm()).build());

        log.info("#### returned metadata");
    }

    @Override
    public String interest() {
        return GetMetadataReq.class.getName();
    }
}
