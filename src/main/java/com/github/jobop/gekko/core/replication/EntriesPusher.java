
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

import com.github.jobop.gekko.connector.GekkoNodeNettyClient;
import com.github.jobop.gekko.core.GekkoConfig;
import com.github.jobop.gekko.core.lifecycle.LifeCycleAdpter;
import com.github.jobop.gekko.core.metadata.NodeState;
import com.github.jobop.gekko.protocols.message.GekkoEntry;

import java.util.*;
import java.util.function.Consumer;

public class EntriesPusher extends LifeCycleAdpter {
    GekkoConfig conf;
    GekkoNodeNettyClient client;
    NodeState state;
    private Set<AcceptCollector> acceptCollector = Collections.synchronizedSet(new HashSet<>());


    public EntriesPusher(GekkoConfig conf, GekkoNodeNettyClient client, NodeState state) {
        this.conf = conf;
        this.client = client;
        this.state = state;
    }

    public void append(GekkoEntry entry, Consumer callback) {
        List<GekkoEntry> entries = new ArrayList<GekkoEntry>();
        entries.add(entry);
        client.pushDatas(entries, new AcceptCollector(entry,this.state, callback));
    }

}
