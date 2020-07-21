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
import com.github.jobop.gekko.core.config.GekkoConfig;
import com.github.jobop.gekko.core.lifecycle.LifeCycleAdpter;
import com.github.jobop.gekko.core.metadata.NodeState;
import com.github.jobop.gekko.core.timout.DelayChangeableTimeoutHolder;
import com.github.jobop.gekko.core.timout.RefreshableTimeoutHolder;
import com.github.jobop.gekko.enums.RoleEnum;
import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.lang.ref.WeakReference;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * the core logic of election
 */
@Data
@Slf4j
public class GekkoLeaderElector extends LifeCycleAdpter {
    GekkoConfig conf;
    GekkoNodeNettyClient client;
    volatile NodeState state;
    private Set<WeakReference<VoteCollector>> voteCollectors = Collections.synchronizedSet(new HashSet<>());
    private Set<WeakReference<PreVoteCollector>> preVoteCollectors = Collections.synchronizedSet(new HashSet<>());
    DelayChangeableTimeoutHolder electionTimeoutChecker;

    RefreshableTimeoutHolder heartBeatSender;


    static int MAX_ELECTION_TIMEOUT = 5000;
    static int MIN_ELECTION_TIMEOUT = 2000;

    static int HEART_BEAT_INTERVAL = 1000;

    Random random = new Random();

    public static void main(String[] args) {
        Random random = new Random(MIN_ELECTION_TIMEOUT);
        int delay = random.nextInt(MAX_ELECTION_TIMEOUT - MIN_ELECTION_TIMEOUT + 1) + MIN_ELECTION_TIMEOUT;
        while (true) {
            if (delay < MIN_ELECTION_TIMEOUT || delay > MAX_ELECTION_TIMEOUT) {
                System.out.println(delay);
            }

            delay = random.nextInt(MAX_ELECTION_TIMEOUT - MIN_ELECTION_TIMEOUT + 1) + MIN_ELECTION_TIMEOUT;
        }

    }

    public GekkoLeaderElector(GekkoConfig conf, GekkoNodeNettyClient client, NodeState state) {
        this.conf = conf;
        this.client = client;
        this.state = state;

    }

    @Override
    public void init() {
        GekkoLeaderElector thisElector = this;
        int delay = random.nextInt(MAX_ELECTION_TIMEOUT - MIN_ELECTION_TIMEOUT + 1) + MIN_ELECTION_TIMEOUT;
        electionTimeoutChecker = new DelayChangeableTimeoutHolder(new TimerTask() {
            @Override
            public void run(Timeout timeout) throws Exception {

                log.info("start election to prevote timeout=" + timeout);
                state.setRole(RoleEnum.PRE_CANDIDATE);
                PreVoteCollector prevoteCollector = new PreVoteCollector(state, thisElector);
                preVoteCollectors.add(new WeakReference<>(prevoteCollector));
                client.preVote(prevoteCollector);
                //when no outer trigger the reset,it will reset by itself
                thisElector.resetElectionTimeout();
            }
        }, delay, TimeUnit.MILLISECONDS);


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

        if (!voteCollectors.isEmpty()) {
            for (WeakReference<VoteCollector> wf : voteCollectors) {
                if (null != wf.get()) {
                    wf.get().disAble();
                }
            }
            voteCollectors.clear();
        }

        if (!preVoteCollectors.isEmpty()) {
            for (WeakReference<PreVoteCollector> wf : preVoteCollectors) {
                if (null != wf.get()) {
                    wf.get().disAble();
                }
            }
            preVoteCollectors.clear();
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
        int delay = random.nextInt(MAX_ELECTION_TIMEOUT - MIN_ELECTION_TIMEOUT + 1) + MIN_ELECTION_TIMEOUT;
        electionTimeoutChecker.refresh(delay, TimeUnit.MILLISECONDS);
    }


    public void asFollower(long term, String leaderId) {
        log.info("this node is a Follower");
        this.cancelAllVoteCollectors();
        this.state.setRole(RoleEnum.FOLLOWER);
        this.state.getTermAtomic().set(term);
        this.state.setLeaderId(leaderId);
        this.stoptSendHeartBeatToFollower();
        this.resetElectionTimeout();
    }

    public void asLeader() {
        log.info("this node becomeALeader the term is " + this.state.getTerm());
        this.stopElectionTimeout();
        this.state.setLeaderId(state.getSelfId());
        this.state.setRole(RoleEnum.LEADER);
        this.cancelAllVoteCollectors();
        this.refreshSendHeartBeatToFollower();
    }
}
