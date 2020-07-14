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
import com.github.jobop.gekko.protocols.message.GekkoEntry;
import com.github.jobop.gekko.store.file.mmap.AutoRollMMapFile;
import com.github.jobop.gekko.store.file.mmap.SlicedByteBuffer;
import com.github.jobop.gekko.utils.CodecUtils;
import com.github.jobop.gekko.utils.NotifyableThread;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.stream.Collectors;


@Slf4j
public class FileStore extends AbstractStore {
    private String BASE_FILE_PATH = "cekko";

    private AutoRollMMapFile dataFile;
    private AutoRollMMapFile indexFile;

    private Thread fileFlushThread;

    private ThreadLocal<ByteBuffer> localBuffer = ThreadLocal.withInitial(() -> {
        return ByteBuffer.allocate(1024 * 1024);
    });

    public FileStore(GekkoConfig conf) {
        super(conf);
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
        indexFile = new AutoRollMMapFile(BASE_FILE_PATH + File.separator + "index", conf.getStoreFileSize(), conf.getOsPageSize());
        dataFile.load();
        indexFile.load();

        this.fileFlushThread = new NotifyableThread(this.conf.getFlushInterval(), "flush-thread") {
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
    public void append(GekkoEntry entry) {
        //FIXME:
        synchronized (this) {

            //TODO:
            //set pos
            long pos = dataFile.allocPos(entry.getTotalSize());
            entry.setPos(pos);

            //after set attributes,set cheksum
            entry.computSizeInBytes();
            entry.setChecksum(entry.checksum());
            //set term
            CodecUtils.encode(entry, localBuffer.get());

            byte[] bytes=new byte[localBuffer.get().remaining()];
            localBuffer.get().get(bytes);
            dataFile.appendMessage(bytes);

        }

    }

    @Override
    public List<GekkoEntry> batchGet(long offset, long toOffset) {
        List<GekkoEntry> entries = null;
        List<SlicedByteBuffer> slicedByteBuffers = dataFile.selectMutilBufferToRead(offset, (int) toOffset);
        if (null == slicedByteBuffers || slicedByteBuffers.isEmpty()) {
            return null;
        }
        List<ByteBuffer> byteBuffers = slicedByteBuffers.stream().map(bb -> {
            return bb.getByteBuffer();
        }).collect(Collectors.toList());

        entries = CodecUtils.decodeToList(byteBuffers);
        return entries;
    }

    @Override
    public GekkoEntry get(long offset, long length) {
        SlicedByteBuffer slicedByteBuffer = dataFile.selectMappedBuffer(offset, (int) length);
        return CodecUtils.decode(slicedByteBuffer.getByteBuffer());
    }
}
