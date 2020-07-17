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
 * Created by CuttleFish on 2020/7/4.
 */

package com.github.jobop.gekko.core.timout;

import com.alipay.remoting.TimerHolder;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import io.netty.util.TimerTask;

import java.util.concurrent.TimeUnit;


public class RefreshableTimeoutHolder {
    TimerTask task;
    long delay;
    TimeUnit unit;
    Timeout timeout;

    public RefreshableTimeoutHolder(TimerTask task, long delay, TimeUnit unit) {
        this.task = task;
        this.delay = delay;
        this.unit = unit;
    }

    private Timeout newTimeout() {
        Timeout timeout = TimerHolder.getTimer().newTimeout(task, delay, unit);
        this.timeout = timeout;
        return timeout;
    }


    public Timeout refresh() {
        if (null != timeout) {
            if (timeout.isExpired()) {
                //TODO:
            } else {
                timeout.cancel();
            }
            return this.newTimeout();
        } else {
            return this.newTimeout();
        }
    }

    public void cancel() {
        if (null != timeout) {
            this.timeout.cancel();
            this.timeout = null;
        }
    }
}
