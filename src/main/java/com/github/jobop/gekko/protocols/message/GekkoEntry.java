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
 * Created by CuttleFish on 2020/7/2.
 */
package com.github.jobop.gekko.protocols.message;

import com.github.jobop.gekko.utils.CrcUtil;
import lombok.Builder;
import lombok.Data;


@Data
@Builder
public class GekkoEntry implements Checksum {
    public static int HEADER_SIZE = 4 + 4 + 8 + 8 + 8 + 8;
    public static int BODY_OFFSET = 4 + 4 + 8 + 8 + 8 + 8;
    public static int POS_OFFSET = 4 + 4 + 8 + 8;

    int totalSize;//headerSize+bodySize
    int magic = 0xCAFEDADD;
    long term;
    long entryIndex;
    long pos;
    long checksum;
    byte[] data;

    public int computSizeInBytes() {
        this.totalSize = BODY_OFFSET + data.length;
        return totalSize;
    }

    public boolean isIntact() {
        return this.checksum == this.checksum();
    }

    @Override
    public long checksum() {
        long c = 0;
        c = checksum(this.totalSize, c);
        c = checksum(this.magic, c);
        c = checksum(this.term, c);
        c = checksum(this.entryIndex, c);
        c = checksum(this.pos, c);
        if (null != this.data) {
            c = checksum(CrcUtil.crc64(this.data), c);
        }

        return c;
    }
}
