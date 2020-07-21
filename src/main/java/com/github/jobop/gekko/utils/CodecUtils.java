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
import com.github.jobop.gekko.protocols.message.GekkoIndex;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;


public class CodecUtils {
    public static void encodeData(GekkoEntry entry, ByteBuffer bb) {
        bb.clear();
        int bodySize = bb.array().length;
        bb.putInt(entry.getMagic());

        bb.putInt(entry.getTotalSize());//totalsize
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

    public static List<GekkoEntry> decodeToDataList(List<ByteBuffer> bbs) {
        List<GekkoEntry> entries = new ArrayList<GekkoEntry>();
        for (ByteBuffer bb : bbs) {
            while (bb.hasRemaining()) {
                GekkoEntry entry = decodeData(bb);
                entries.add(entry);
            }
        }
        return entries;
    }

    public static GekkoEntry decodeData(ByteBuffer bb) {
        int magic = bb.getInt();
        int totalsize = bb.getInt();
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


    public static void encodeIndex(GekkoIndex index, ByteBuffer bb) {
        bb.clear();
        bb.putInt(index.getMagic());
        bb.putInt(index.getTotalSize());
        bb.putLong(index.getDataPos());
        bb.putLong(index.getDataIndex());
        bb.putInt(index.getDataSize());
        bb.flip();
    }


    public static GekkoIndex decodeIndex(ByteBuffer bb) {
        int magic = bb.getInt();//magic
        if (magic != GekkoIndex.MAGIC) {
            return null;
        }
        int totalSize = bb.getInt();
        long pos = bb.getLong();
        long index = bb.getLong();
        int size = bb.getInt();
        return GekkoIndex.builder().magic(magic).totalSize(totalSize).dataPos(pos).dataIndex(index).dataSize(size).build();
    }

    public static List<GekkoIndex> decodeToIndexList(List<ByteBuffer> bbs) {
        List<GekkoIndex> entries = new ArrayList<GekkoIndex>();
        for (ByteBuffer bb : bbs) {
            while (bb.hasRemaining()) {
                GekkoIndex index = decodeIndex(bb);
                entries.add(index);
            }
        }
        return entries;
    }
}
