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
 * Created by CuttleFish on 2020/7/16.
 */

package com.github.jobop.gekko.core.election;

import com.github.jobop.gekko.connector.GekkoNodeNettyClient;
import com.github.jobop.gekko.core.GekkoConfig;
import com.github.jobop.gekko.core.lifecycle.LifeCycleAdpter;
import com.github.jobop.gekko.core.metadata.NodeState;
import com.github.jobop.gekko.core.timout.DelayChangeableTimeoutHolder;
import com.github.jobop.gekko.enums.RoleEnum;
import io.netty.util.Timeout;
import io.netty.util.TimerTask;

import java.util.Random;
import java.util.concurrent.TimeUnit;

public class GekkoLeaderElector extends LifeCycleAdpter {
    GekkoConfig conf;
    GekkoNodeNettyClient client;
    NodeState state;

    DelayChangeableTimeoutHolder heartBeatTimer;
    VoteCollector voteCollector;

    static int MAX_HEART_BEAT_TIMEOUT = 300;

    Random random = new Random();

    public GekkoLeaderElector(GekkoConfig conf, GekkoNodeNettyClient client, NodeState state) {
        this.conf = conf;
        this.client = client;
        this.state = state;

    }

    @Override
    public void init() {
        heartBeatTimer = new DelayChangeableTimeoutHolder(new TimerTask() {
            @Override
            public void run(Timeout timeout) throws Exception {
                state.setRole(RoleEnum.CANDIDATE);
                //TODO:
                client.reqVote();
            }
        }, random.nextInt(MAX_HEART_BEAT_TIMEOUT), TimeUnit.MILLISECONDS);
    }


    @Override
    public void start() {
        heartBeatTimer.refresh();
    }

    @Override
    public void shutdown() {
        heartBeatTimer.cancel();
    }

    public void cancelVote(){

    }
    public void resetHeartBeatTimeout() {
        heartBeatTimer.refresh(random.nextInt(MAX_HEART_BEAT_TIMEOUT), TimeUnit.MILLISECONDS);
    }


}
