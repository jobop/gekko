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


import com.github.jobop.gekko.core.lifecycle.GekkoReferenceCounted;
import com.github.jobop.gekko.enums.ResultEnums;
import com.github.jobop.gekko.utils.PreConditions;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.function.Consumer;
import java.util.function.Function;


public abstract class ReferenceCountedResource implements GekkoReferenceCounted {
    protected volatile int refCnt = 1;
    protected volatile boolean hashClean;
    private AtomicIntegerFieldUpdater updater = AtomicIntegerFieldUpdater.newUpdater(ReferenceCountedResource.class, "refCnt");

    public int refCnt() {
        return this.refCnt;
    }

    public GekkoReferenceCounted retain() {
        return this.retain(1);
    }

    public GekkoReferenceCounted retain(int increment) {
        for (; ; ) {
            int oldRefCnt = this.refCnt;

            final int nextRefCnt = oldRefCnt + increment;
            if (updater.compareAndSet(this, oldRefCnt, nextRefCnt)) {
                PreConditions.check(oldRefCnt > 0, ResultEnums.REFERENCE_COUNTED, " retain resource fail,oldRefCnt=%d", oldRefCnt);
                break;
            }
        }
        return this;
    }


    public void releaseNow(Consumer<Object>... callbacks) {
        this.refCnt = -Integer.MAX_VALUE + 1000;
        this.release(callbacks);
    }

    public void release(Consumer<Object>... callbacks) {
        this.release(1, callbacks);
    }

    public void release(int decrement, Consumer<Object>... callbacks) {
        for (; ; ) {
            int oldRefCnt = this.refCnt;
            final int nextRefCnt = oldRefCnt - decrement;

            if (updater.compareAndSet(this, oldRefCnt, nextRefCnt)) {
                if (nextRefCnt <= 0) {
                    synchronized (this) {
                        hashClean = cleanup(callbacks);
                    }
                }
                return;
            }
        }
    }


    protected <T, R> Object autoReleaseTemplate(Function<T, R> f) {
        this.retain();
        try {
            return f.apply(null);
        } finally {
            this.release();
        }

    }


    protected abstract boolean cleanup(Consumer<Object>... callbacks);
}
