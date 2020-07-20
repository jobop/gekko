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
package com.github.jobop.gekko.connector.processors;

import com.alipay.remoting.AsyncContext;
import com.alipay.remoting.BizContext;
import com.github.jobop.gekko.core.election.GekkoLeaderElector;
import com.github.jobop.gekko.core.metadata.NodeState;
import com.github.jobop.gekko.enums.RoleEnum;
import com.github.jobop.gekko.enums.VoteResultEnums;
import com.github.jobop.gekko.protocols.GekkoInboundProtocol;
import com.github.jobop.gekko.protocols.message.node.PreVoteResp;
import com.github.jobop.gekko.protocols.message.node.VoteReq;
import com.github.jobop.gekko.protocols.message.node.VoteResp;
import com.github.jobop.gekko.utils.ElectionUtils;
import lombok.extern.slf4j.Slf4j;

/**
 * process the vote req from condicatior
 */
@Slf4j
public class ReqVoteProcessor extends DefaultProcessor<VoteReq> {
    GekkoLeaderElector elector;

    public ReqVoteProcessor(GekkoInboundProtocol helper, GekkoLeaderElector elector) {
        super(helper);
        this.elector = elector;
    }

    public void handleRequest(BizContext bizCtx, AsyncContext asyncCtx, VoteReq request) {
        NodeState nodeState = elector.getState();
        long nowTerm = nodeState.getTerm();
        long voteTerm = request.getTerm();

        long nowLastIndex = nodeState.getCommitId();
        long remoteLastIndex = request.getLastIndex();
        if (!elector.getState().getGroup().equals(request.getGroup())) {
            log.info("term " + voteTerm + " voted to " + request.getCandidateId() + " reject");
            asyncCtx.sendResponse(VoteResp.builder().group(elector.getState().getGroup()).term(voteTerm).voteMemberId(elector.getState().getSelfId()).result(VoteResultEnums.REJECT).build());
            return;
        }
        if (ElectionUtils.judgVote(nowTerm, voteTerm, nowLastIndex, remoteLastIndex)) {
            if (nodeState.getTermAtomic().compareAndSet(nowTerm, voteTerm)) {
                log.info("term " + voteTerm + " voted to " + request.getCandidateId());
                asyncCtx.sendResponse(VoteResp.builder().group(nodeState.getGroup()).term(voteTerm).voteMemberId(nodeState.getSelfId()).result(VoteResultEnums.AGREE).build());
            } else {
                asyncCtx.sendResponse(VoteResp.builder().group(nodeState.getGroup()).term(voteTerm).voteMemberId(nodeState.getSelfId()).result(VoteResultEnums.REJECT).build());
            }
        } else {
            asyncCtx.sendResponse(VoteResp.builder().group(nodeState.getGroup()).term(voteTerm).voteMemberId(nodeState.getSelfId()).result(VoteResultEnums.REJECT).build());
        }
    }

    public String interest() {
        return VoteReq.class.getName();
    }
}
