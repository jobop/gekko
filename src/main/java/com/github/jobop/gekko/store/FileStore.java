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
package com.github.jobop.gekko.store;

import com.github.jobop.gekko.core.GekkoConfig;
import com.github.jobop.gekko.core.metadata.NodeState;
import com.github.jobop.gekko.protocols.message.GekkoEntry;
import com.github.jobop.gekko.protocols.message.GekkoIndex;
import com.github.jobop.gekko.store.file.mmap.AutoRollMMapFile;
import com.github.jobop.gekko.store.file.mmap.SlicedByteBuffer;
import com.github.jobop.gekko.utils.CodecUtils;
import com.github.jobop.gekko.utils.NotifyableThread;
import com.github.jobop.gekko.utils.SlicedByteBufferUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;


@Slf4j
public class FileStore extends AbstractStore {
    private String BASE_FILE_PATH = "cekko";

    private AutoRollMMapFile dataFile;
    private AutoRollMMapFile indexFile;

    private NotifyableThread fileFlushThread;


    private ThreadLocal<ByteBuffer> localDataBuffer = ThreadLocal.withInitial(() -> {
        return ByteBuffer.allocate(1024 * 1024);
    });
    private ThreadLocal<ByteBuffer> localIndexBuffer = ThreadLocal.withInitial(() -> {
        return ByteBuffer.allocate(GekkoIndex.INDEX_SIZE * 1024);
    });

    public FileStore(GekkoConfig conf, NodeState nodeState) {
        super(conf, nodeState);
    }

    @Override
    public void init() {
        BASE_FILE_PATH = conf.getBaseFilePath();
        File baseDir = new File(BASE_FILE_PATH);
        try {
            FileUtils.forceMkdir(baseDir);
        } catch (IOException e) {
            log.error("", e);
        }
        dataFile = new AutoRollMMapFile(BASE_FILE_PATH + File.separator + "data", conf.getStoreFileSize(), conf.getOsPageSize());
        indexFile = new AutoRollMMapFile(BASE_FILE_PATH + File.separator + "index", GekkoIndex.INDEX_SIZE * conf.getIndexCountPerFile(), conf.getOsPageSize());
        dataFile.load();
        indexFile.load();

        this.fileFlushThread = new NotifyableThread(this.conf.getFlushInterval(), TimeUnit.SECONDS, "flush-thread") {
            @Override
            public void doWork() {
                dataFile.flush(1);
                indexFile.flush(1);
            }
        };

    }

    @Override
    public void start() {
        this.fileFlushThread.start();
    }

    @Override
    public void shutdown() {
        this.fileFlushThread.shutdown();
    }

    @Override
    public void append(GekkoEntry entry) {
        synchronized (this) {
            if (nodeState.getSelfId() == nodeState.getLeaderId()) {
                fillEntry(entry);
            }
            //save data
            saveData(entry);
            //save index
            GekkoIndex index = GekkoIndex.builder().magic(0xCAFEDADE).totalSize(GekkoIndex.INDEX_SIZE).dataPos(entry.getPos()).dataIndex(entry.getEntryIndex()).dataSize(entry.getTotalSize()).build();
            saveIndex(index);

            if (entry.getPos() != -1) {
                if (nodeState.getWriteId() < entry.getEntryIndex()) {
                    nodeState.setWriteId(entry.getEntryIndex());
                    nodeState.setLastChecksum(entry.getChecksum());
                }
            }
        }

    }

    private void fillEntry(GekkoEntry entry) {
        //set pos
        long pos = dataFile.allocPos(entry.getTotalSize());
        entry.setPos(pos);

        //set index
        long dataIndex = indexFile.getMaxOffset() / GekkoIndex.INDEX_SIZE;
        entry.setEntryIndex(dataIndex);
        //set term
        entry.setTerm(this.nodeState.getTerm());
        entry.computSizeInBytes();
        entry.setChecksum(entry.checksum());
    }


    @Override
    public List<GekkoEntry> batchGet(long fromPos, long toPos) {
        List<GekkoEntry> entries = null;
        List<SlicedByteBuffer> slicedByteBuffers = null;
        try {
            slicedByteBuffers = dataFile.selectMutilBufferToRead(fromPos, (int) toPos);
            if (null == slicedByteBuffers || slicedByteBuffers.isEmpty()) {
                return null;
            }
            List<ByteBuffer> byteBuffers = slicedByteBuffers.stream().map(bb -> {
                return bb.getByteBuffer();
            }).collect(Collectors.toList());

            entries = CodecUtils.decodeToDataList(byteBuffers);
            return entries;
        } finally {
            SlicedByteBufferUtils.safeRelease(slicedByteBuffers);
        }

    }

    @Override
    public GekkoEntry get(long offset, long length) {
        SlicedByteBuffer slicedByteBuffer = null;
        try {
            slicedByteBuffer = dataFile.selectMappedBuffer(offset, (int) length);
            return CodecUtils.decodeData(slicedByteBuffer.getByteBuffer());
        } finally {
            SlicedByteBufferUtils.safeRelease(slicedByteBuffer);
        }

    }

    @Override
    public GekkoEntry getByIndex(long dataIndex) {
        GekkoIndex index = getGekkoIndex(dataIndex);
        if (index == null) return null;
        return this.get(index.getDataPos(), index.getDataSize());

    }

    private GekkoIndex getGekkoIndex(long dataIndex) {
        SlicedByteBuffer indexslicedByteBuffer = null;
        GekkoIndex index = null;
        try {
            indexslicedByteBuffer = indexFile.selectMappedBuffer(dataIndex * GekkoIndex.INDEX_SIZE, GekkoIndex.INDEX_SIZE);
            index = CodecUtils.decodeIndex(indexslicedByteBuffer.getByteBuffer());
        } finally {
            SlicedByteBufferUtils.safeRelease(indexslicedByteBuffer);
        }
        if (null == index) {
            return null;
        }
        return index;
    }

    @Override
    public List<GekkoEntry> batchGetByIndex(long fromIndex, long toIndex) {
        GekkoIndex fromGekkoIndex = getGekkoIndex(fromIndex);
        GekkoIndex toGekkoIndex = getGekkoIndex(toIndex);
        return this.batchGet(fromGekkoIndex.getDataPos(), toGekkoIndex.getDataPos());
    }

    private long saveIndex(GekkoIndex index) {
        CodecUtils.encodeIndex(index, localIndexBuffer.get());
        byte[] indexbytes = new byte[localIndexBuffer.get().remaining()];
        localIndexBuffer.get().get(indexbytes);
        return indexFile.appendMessage(indexbytes);
    }

    private void saveData(GekkoEntry entry) {
        CodecUtils.encodeData(entry, localDataBuffer.get());
        byte[] bytes = new byte[localDataBuffer.get().remaining()];
        localDataBuffer.get().get(bytes);
        dataFile.appendMessage(bytes);
    }

    public void trimAfter(long fromIndex) {
        GekkoIndex index = getGekkoIndex(fromIndex);
        this.dataFile.trimAfter(index.getDataPos());
        this.indexFile.trimAfter(fromIndex * GekkoIndex.INDEX_SIZE);

        this.nodeState.setWriteId(fromIndex);
        this.nodeState.setCommitId(fromIndex);
        this.nodeState.setLastChecksum(this.getByIndex(fromIndex).getChecksum());
    }

    public void trimBefore(long toIndex) {
    }
}
