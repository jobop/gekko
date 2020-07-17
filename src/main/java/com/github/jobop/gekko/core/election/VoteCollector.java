
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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class VoteCollector implements InvokeCallback {
    NodeState nodeState;

    private ConcurrentHashMap agreeNodeMap = new ConcurrentHashMap();



    private volatile AtomicBoolean available = new AtomicBoolean(true);

    public VoteCollector(NodeState nodeState) {
        this.nodeState = nodeState;
    }

    @Override
    public void onResponse(Object result) {
        if (available.get() == true) {
            int nodeCount = nodeState.getPeersMap().size();


        }


    }

    @Override
    public void onException(Throwable e) {

    }

    @Override
    public Executor getExecutor() {
        return null;
    }
}
