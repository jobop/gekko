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
 * Created by CuttleFish on 2020/7/12.
 */
package com.github.jobop.gekko.utils;


import com.github.jobop.gekko.protocols.message.GekkoEntry;
import com.github.jobop.gekko.store.file.mmap.SlicedByteBuffer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;


public class CodecUtils {
    public static void encode(GekkoEntry entry, ByteBuffer bb) {
        bb.clear();
        int bodySize = bb.array().length;
        bb.putInt(entry.getTotalSize());//totalsize
        bb.putInt(entry.getMagic());
        bb.putLong(entry.getTerm());
        bb.putLong(entry.getEntryIndex());
        bb.putLong(entry.getPos());
        bb.putLong(entry.getChecksum());
        bb.put(entry.getData());
        bb.flip();

    }

//    public static List<GekkoEntry> decodeToList(List<SlicedByteBuffer> slicedByteBuffers) {
//        List<GekkoEntry> entries = new ArrayList<GekkoEntry>();
//        for (SlicedByteBuffer slicedByteBuffer : slicedByteBuffers) {
//            long limit = slicedByteBuffer.getBelongFile().getLimit();
//            ByteBuffer bb = slicedByteBuffer.getByteBuffer();
//            bb.
//            while (bb.hasRemaining()) {
//                GekkoEntry entry = decode(bb);
//                entries.add(entry);
//            }
//        }
//        return entries;
//    }

    public static List<GekkoEntry> decodeToList(List<ByteBuffer> bbs) {
        List<GekkoEntry> entries = new ArrayList<GekkoEntry>();
        for (ByteBuffer bb : bbs) {
            while (bb.hasRemaining()) {
                GekkoEntry entry = decode(bb);
                entries.add(entry);
            }
        }
        return entries;
    }

    public static GekkoEntry decode(ByteBuffer bb) {
        int totalsize = bb.getInt();
        int magic = bb.getInt();
        long term = bb.getLong();
        long entryIndex = bb.getLong();
        long pos = bb.getLong();
        long checksum = bb.getLong();
        byte[] data = new byte[totalsize - GekkoEntry.BODY_OFFSET];

        bb.get(data);
        GekkoEntry entry = GekkoEntry.builder().totalSize(totalsize).magic(magic).term(term).entryIndex(entryIndex).pos(pos)
                .checksum(checksum).data(data).build();
        return entry;
    }
}
