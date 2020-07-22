
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
 * Created by CuttleFish on 2020/7/17.
 */

package com.github.jobop.gekko.core.election;

import com.alipay.remoting.InvokeCallback;
import com.github.jobop.gekko.core.metadata.NodeState;
import com.github.jobop.gekko.enums.VoteResultEnums;
import com.github.jobop.gekko.protocols.message.node.VoteResp;
import com.github.jobop.gekko.utils.Utils;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
@Data
public class VoteCollector implements InvokeCallback {
    NodeState nodeState;
    private long voteTerm;

    private Set<String> agreeSet = Collections.synchronizedSet(new HashSet<>());


    private volatile AtomicBoolean available = new AtomicBoolean(true);
    GekkoLeaderElector elector;

    public VoteCollector(NodeState nodeState, GekkoLeaderElector elector) {
        this.nodeState = nodeState;
        this.voteTerm = nodeState.getTermAtomic().incrementAndGet();
        agreeSet.add(nodeState.getSelfId());
        this.elector = elector;
    }

    @Override
    public void onResponse(Object result) {
        synchronized (nodeState) {
            VoteResp resp = (VoteResp) result;
            if (resp.getTerm() != this.voteTerm) {
                log.warn("this vote term has expired! term=" + this.getVoteTerm());
                return;
            }

            long oldTerm = nodeState.getTerm();
            if (this.voteTerm < oldTerm) {
                log.warn("this vote term has expired! term=" + this.getVoteTerm());
                return;
            }

            if (available.get() == true) {
                if (resp.getResult() == VoteResultEnums.AGREE) {
                    agreeSet.add(resp.getVoteMemberId());
                    //become a leader
                    if (agreeSet.size() >= (nodeState.getPeersMap().size() / 2) + 1) {
                        //upgrade to leader and disable this collector
                        if (available.compareAndSet(true, false)) {
//                        this.nodeState.getTermAtomic().compareAndSet(this.voteTerm, this.voteTerm + 1);
                            this.elector.asLeader();

                        }
                    }
                }
            }
        }
    }

    @Override
    public void onException(Throwable e) {
        log.error("", e);
    }

    @Override
    public Executor getExecutor() {
        return Utils.GOABL_DEFAULT_THREAD_POOL;
    }

    public void disAble() {
        if (this.available.compareAndSet(true, false)) {
            agreeSet.clear();
            agreeSet = null;
        }
    }
}
