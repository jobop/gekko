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
package com.github.jobop.gekko.store;

import com.github.jobop.gekko.core.GekkoConfig;
import com.github.jobop.gekko.core.lifecycle.LifeCycleAdpter;
import com.github.jobop.gekko.core.metadata.NodeState;
import com.github.jobop.gekko.protocols.message.GekkoEntry;

import java.util.List;


public  class AbstractStore extends LifeCycleAdpter implements com.github.jobop.gekko.store.Store {
    protected GekkoConfig conf;
    protected   NodeState nodeState;

    public AbstractStore(GekkoConfig conf, NodeState nodeState) {
        this.conf = conf;
        this.nodeState=nodeState;
    }

    @Override
    public void init() {
        super.init();
    }

    @Override
    public void start() {
        super.start();
    }

    @Override
    public void shutdown() {
        super.shutdown();
    }

    public void append(GekkoEntry entry) {

    }

    public List<GekkoEntry> batchGet(long offset, long length) {
        return null;
    }

    @Override
    public GekkoEntry get(long offset, long length) {
        return null;
    }

    @Override
    public GekkoEntry getByIndex(long index) {
        return null;
    }

    @Override
    public List<GekkoEntry> batchGetByIndex(long fromIndex, long toIndex) {
        return null;
    }

    @Override
    public void trimAfter(long fromIndex) {

    }

    @Override
    public void trimBefore(long toIndex) {

    }
}
