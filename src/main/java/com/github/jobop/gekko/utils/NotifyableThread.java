
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
 * Created by CuttleFish on 2020/7/13.
 */

package com.github.jobop.gekko.utils;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * provide a thread which can dowork in interval second,and can trigger the job immediately when the trigger method called
 */
@Slf4j
public abstract class NotifyableThread extends Thread {
    private CyclicBarrier cb = new CyclicBarrier(2);
    private int interval;
    private TimeUnit timeUnit;
    private volatile AtomicBoolean shutdowned = new AtomicBoolean(false);
    /**
     * 1、avoid to hasNotify more then twice
     * 2、when the dowork method run too long ，and the shutdown method has trigger，it can return immediately without waiting
     */
    protected volatile AtomicBoolean hasNotified = new AtomicBoolean(false);
    private CountDownLatch latch = new CountDownLatch(1);

    public void trigger() {
        if (hasNotified.compareAndSet(false, true)) {
            try {
                cb.await(0, timeUnit);
            } catch (Exception e) {
            }
        }
    }

    public void shutdown() {
        if (shutdowned.compareAndSet(false, true)) {
            this.trigger();
            //wait for awhile
            try {
                latch.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
            }
        }

    }

    public NotifyableThread(int interval, TimeUnit timeUnit, String name) {
        this.interval = interval;
        this.setName(name);
        this.timeUnit = timeUnit;
    }

    @Override
    public void run() {
        while (!shutdowned.get()) {
            try {
                doWork();
            } catch (Exception e) {
                log.error("", e);
            }
            waiting();
        }

        latch.countDown();

    }

    public void waiting() {
        if (hasNotified.compareAndSet(true, false)) {
            return;
        }
        try {
            cb.reset();//2
            cb.await(this.interval, timeUnit);//1
        } catch (Exception e) {
        } finally {
            hasNotified.set(false);
        }

    }

    public abstract void doWork();
}
