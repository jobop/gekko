
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
 * Created by CuttleFish on 2020/7/19.
 */

package com.github.jobop.gekko.core.replication;

import com.alipay.remoting.InvokeCallback;
import com.github.jobop.gekko.core.metadata.NodeState;
import com.github.jobop.gekko.enums.PushResultEnums;
import com.github.jobop.gekko.enums.VoteResultEnums;
import com.github.jobop.gekko.protocols.message.GekkoEntry;
import com.github.jobop.gekko.protocols.message.node.PushEntryResp;
import com.github.jobop.gekko.protocols.message.node.VoteResp;
import com.github.jobop.gekko.utils.Utils;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

@Slf4j
public class AcceptCollector implements InvokeCallback {
    NodeState nodeState;
    private Consumer callback;
    private Set<String> agreeSet = Collections.synchronizedSet(new HashSet<>());
    private AtomicBoolean hasNotify = new AtomicBoolean(false);
    private GekkoEntry entry;

    public AcceptCollector(GekkoEntry entry, NodeState nodeState, Consumer callback) {
        this.callback = callback;
        this.nodeState = nodeState;
        this.entry = entry;
    }


    @Override
    public void onResponse(Object result) {
        PushEntryResp resp = (PushEntryResp) result;
        if (resp.getResult() == PushResultEnums.AGREE) {
            agreeSet.add(resp.getAcceptNodeId());
            if (agreeSet.size() >= (nodeState.getPeersMap().size() / 2) + 1) {
                if (hasNotify.compareAndSet(false, true)) {
                    if (nodeState.getCommitId() < resp.getIndex()) {
                        nodeState.setCommitId(resp.getIndex());
                    }
                    callback.accept(this.entry);
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
}
