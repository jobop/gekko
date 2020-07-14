
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
 * Created by CuttleFish on 2020/7/14.
 */

package com.github.jobop.gekko.utils;

import com.github.jobop.gekko.store.file.mmap.SlicedByteBuffer;

import java.util.List;

public class SlicedByteBufferUtils {
    public static void safeRelease(SlicedByteBuffer buffer) {
        if (null != buffer) {
            try {
                buffer.release();
            } catch (Exception e) {
            }
        }

    }

    public static void safeRelease(List<SlicedByteBuffer> buffers) {
        if (null != buffers && !buffers.isEmpty()) {
            for (SlicedByteBuffer buffer : buffers) {
                safeRelease(buffer);
            }
        }
    }
}
