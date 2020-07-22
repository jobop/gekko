
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
 * Created by CuttleFish on 2020/7/22.
 */

package com.github.jobop.gekko.utils;

import java.util.concurrent.*;

public class Utils {
    public static ThreadPoolExecutor GOABL_DEFAULT_THREAD_POOL = new ThreadPoolExecutor(cpus() * 2,
            cpus() * 16,
            60, TimeUnit.SECONDS,
            new SynchronousQueue<>());

    public static int CPUS = Runtime.getRuntime().availableProcessors();

    public static int cpus() {
        return CPUS;
    }

    public static void invokeWithDefaultExcutor(Runnable r) {
        GOABL_DEFAULT_THREAD_POOL.submit(r);
    }



}
