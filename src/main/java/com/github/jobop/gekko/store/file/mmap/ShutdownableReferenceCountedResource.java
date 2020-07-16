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
 * Created by CuttleFish on 2020/7/6.
 */
package com.github.jobop.gekko.store.file.mmap;

import com.alipay.remoting.TimerHolder;
import com.github.jobop.gekko.core.lifecycle.GekkoReferenceCounted;
import com.github.jobop.gekko.enums.ResultEnums;
import com.github.jobop.gekko.utils.PreConditions;
import io.netty.util.Timeout;
import io.netty.util.TimerTask;

import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;


public abstract class ShutdownableReferenceCountedResource extends ReferenceCountedResource {

    protected volatile boolean available = true;

    @Override
    public GekkoReferenceCounted retain(int increment) {
        PreConditions.check(available, ResultEnums.REFERENCE_COUNTED, "retain resource fail,avavlibale=false");
        return super.retain(increment);
    }

    public void shutdownNow(Consumer<Object>... callbacks) {
        this.shutdown(0, callbacks);
    }

    /**
     * FIXME:
     * provide a method that can gracefully shutdown
     * onece invoke this method,the available flag turn to false and the file can not be retain any more
     * after intervalForcibly secondes,the timer will trigger the releaseNow method and force to close the file
     *
     * 提供一个用于优雅关闭shutdown文件的方法
     * 一旦调用此方法，此文件就再也不能对外提供appand服务（此时正在执行的写操作不影响）
     * 当等待时间到达了intervalForcibly制定的秒数后，时间轮timer会触发releaseNow方法强制关闭此文件。
     *
     * @param intervalForcibly
     * @param callbacks
     */
    public void shutdown(final long intervalForcibly, Consumer<Object>... callbacks) {
        if (available) {
            this.available = false;
        }
        if (intervalForcibly <= 0) {
            this.releaseNow(callbacks);
            return;
        }
        this.release(callbacks);

        if (!this.hashClean) {
            //usde the HashedWheelTimer to trigger the job
            TimerHolder.getTimer().newTimeout(new TimerTask() {
                @Override
                public void run(Timeout timeout) throws Exception {
                    releaseNow(callbacks);
                }
            }, intervalForcibly, TimeUnit.SECONDS);
        }
    }
}
