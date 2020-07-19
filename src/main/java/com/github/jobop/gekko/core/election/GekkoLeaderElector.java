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
import com.github.jobop.gekko.core.timout.RefreshableTimeoutHolder;
import com.github.jobop.gekko.enums.RoleEnum;
import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import lombok.Data;

import java.util.*;
import java.util.concurrent.TimeUnit;
@Data
public class GekkoLeaderElector extends LifeCycleAdpter {
    GekkoConfig conf;
    GekkoNodeNettyClient client;
    NodeState state;

    DelayChangeableTimeoutHolder electionTimeoutChecker;

    RefreshableTimeoutHolder heartBeatSender;

    private Set<VoteCollector> voteCollectors = Collections.synchronizedSet(new HashSet<>());

    static int MAX_ELECTION_TIMEOUT = 300;
    static int MIN_ELECTION_TIMEOUT = 150;

    static int HEART_BEAT_INTERVAL = 80;

    Random random = new Random(MIN_ELECTION_TIMEOUT);

    public GekkoLeaderElector(GekkoConfig conf, GekkoNodeNettyClient client, NodeState state) {
        this.conf = conf;
        this.client = client;
        this.state = state;

    }

    @Override
    public void init() {
        GekkoLeaderElector thisElector = this;
        electionTimeoutChecker = new DelayChangeableTimeoutHolder(new TimerTask() {
            @Override
            public void run(Timeout timeout) throws Exception {
                state.setRole(RoleEnum.CANDIDATE);
                state.getTermAtomic().incrementAndGet();
                VoteCollector voteCollector = new VoteCollector(state, thisElector);
                voteCollectors.add(voteCollector);
                client.reqVote(voteCollector);
                //when no outer trigger the reset,it will reset by itself
                thisElector.resetElectionTimeout();
            }
        }, random.nextInt(MAX_ELECTION_TIMEOUT), TimeUnit.MILLISECONDS);


        heartBeatSender = new RefreshableTimeoutHolder(new TimerTask() {
            @Override
            public void run(Timeout timeout) throws Exception {
                client.sendHeartBeat();
                //wait for the next heartbeat trigger
                thisElector.refreshSendHeartBeatToFollower();
            }
        }, HEART_BEAT_INTERVAL, TimeUnit.MILLISECONDS);
    }


    @Override
    public void start() {
        electionTimeoutChecker.refresh();
    }

    @Override
    public void shutdown() {
        electionTimeoutChecker.cancel();
    }

    /**
     * when get a heartbeat or append,it means that a leader has born,so need to cancel all votes belong to this node
     */
    public void cancelAllVoteCollectors() {
        state.setRole(RoleEnum.FOLLOWER);
        if (!voteCollectors.isEmpty()) {
            for (VoteCollector collector : voteCollectors) {
                collector.disAble();
            }
            voteCollectors.clear();
        }
    }

    public void refreshSendHeartBeatToFollower() {
        this.heartBeatSender.refresh();
    }

    public void stoptSendHeartBeatToFollower() {
        this.heartBeatSender.cancel();
    }

    public void stopElectionTimeout() {
        electionTimeoutChecker.cancel();
    }

    public void resetElectionTimeout() {
        electionTimeoutChecker.refresh(random.nextInt(MAX_ELECTION_TIMEOUT), TimeUnit.MILLISECONDS);
    }


    public void becomeAFollower() {
        this.cancelAllVoteCollectors();
        this.stoptSendHeartBeatToFollower();
        this.resetElectionTimeout();
    }

    public void becomeALeader() {
        this.cancelAllVoteCollectors();
        this.stopElectionTimeout();
        this.refreshSendHeartBeatToFollower();
    }
}
