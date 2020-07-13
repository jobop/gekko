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
 * Created by CuttleFish on 2020/7/5.
 */
package com.github.jobop.gekko.store.mmap;

import com.github.jobop.gekko.core.exception.GekkoException;
import com.github.jobop.gekko.utils.FileUtils;
import com.github.jobop.gekko.utils.MmapUtils;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;


@Slf4j
public class DefaultMMapFile extends ShutdownableReferenceCountedResource implements MmapFile {
    //4k pagecache
    private int osPageSize = 1024 * 4;
    private int fileSize;
    private long fileFromOffset;
    private AtomicInteger startPos = new AtomicInteger(0);
    private AtomicInteger wrotePos = new AtomicInteger(0);
    private AtomicInteger flushedPos = new AtomicInteger(0);
    protected volatile boolean hashClean = false;
    /**
     * file full path
     */
    private String fileName;
    protected File file;
    private FileChannel fileChannel;
    private MappedByteBuffer mappedByteBuffer;

    public DefaultMMapFile(String fileName, int fileSize, int osPageSize) {
        this.fileName = fileName;
        this.fileSize = fileSize;
        File file = new File(fileName);
        this.file = file;
        this.fileFromOffset = Long.valueOf(this.file.getName());
        FileUtils.forceMkdir(file.getParentFile());
        initFileChannel(file);
        initMappedByteBuffer();
        if (0 != osPageSize) {
            this.osPageSize = osPageSize;
        }

    }

    private void initMappedByteBuffer() {
        try {
            this.mappedByteBuffer = this.fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, this.fileSize);
        } catch (IOException e) {
            log.error("", e);
            throw new GekkoException(e);
        }
    }

    private void initFileChannel(File file) {
        try {
            this.fileChannel = new RandomAccessFile(file, "rw").getChannel();
        } catch (FileNotFoundException e) {
            log.error("filename=" + file.getName(), e);
            throw new GekkoException(e);
        }
    }


    public String getFileName() {
        return fileName;
    }

    public int getFileSize() {
        return fileSize;
    }

    public FileChannel getFileChannel() {
        return fileChannel;
    }

    public boolean isFull() {
        return wrotePos.get() == fileSize;
    }

    public boolean isAvailable() {
        return this.available;
    }

    public long appendMessage(byte[] data) {
        return this.appendMessage(data, 0, data.length);
    }

    public long appendMessage(byte[] data, long offset, int length) {
        return (Long) autoReleaseTemplate(x -> {
            int currentWrotePos = this.getWrotePosition();
            if (currentWrotePos + length <= this.getFileSize()) {
                ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
                byteBuffer.position(currentWrotePos);
                byteBuffer.put(data, (int) offset, length);
                this.wrotePos.addAndGet(length);
                return Long.valueOf(currentWrotePos);
            } else {
                return -1l;
            }
        });
    }

    public long getFileFromOffset() {
        return this.fileFromOffset;
    }

    public int flush(int flushLeastPages) {
        return (int) autoReleaseTemplate(x -> {
            if (this.isAvailable()) {
                if (this.isAbleToFlush(flushLeastPages)) {
                    this.getMappedByteBuffer().force();
                    this.flushedPos.set(this.getWrotePosition());
                }
            }
            return this.getFlushedPosition();
        });
    }

    /**
     * this resource can not autorelease ,the release opration will responsible by the user who use it
     * 这个资源不会自动回收，回收的操作应该交给使用它的人
     *
     * @param pos  the given position
     * @param size the size of the returned sub-region
     * @return
     */
    @Override
    public SlicedByteBuffer selectMappedBuffer(int pos, int size) {
        this.retain();
        ByteBuffer targetbyteBuffer = sliceByteBuffer(pos, size);
        return SlicedByteBuffer.builder()
                .belongFile(this)
                .filePos(pos)
                .byteBuffer(targetbyteBuffer)
                .gobalPos(this.getFileFromOffset() + pos)
                .size(size).
                        build();
    }

    @Override
    public SlicedByteBuffer selectMappedBuffer(int pos) {
        int size = this.getWrotePosition() - pos;
        return this.selectMappedBuffer(pos, size);
    }

    private boolean isAbleToFlush(final int flushLeastPages) {
        int flushedPos = this.getFlushedPosition();
        int writePos = this.getWrotePosition();

        if (this.isFull()) {
            return writePos > flushedPos;
        }

        if (flushLeastPages > 0) {
            return ((writePos / osPageSize) - (flushedPos / osPageSize)) >= flushLeastPages;
        }

        return writePos > flushedPos;
    }


    public MappedByteBuffer getMappedByteBuffer() {
        return mappedByteBuffer;
    }

    public ByteBuffer sliceByteBuffer() {
        return this.mappedByteBuffer.slice();
    }

    public long getLastModifiedTimestamp() {
        return this.file.lastModified();
    }


    public int getFlushedPosition() {
        return this.flushedPos.get();
    }

    public void setFlushedPosition(int flushedPosition) {
        this.flushedPos.set(flushedPosition);
    }

    public int getStartPosition() {
        return this.startPos.get();
    }

    public void setStartPosition(int startPosition) {
        this.startPos.set(startPosition);
    }

    public int getWrotePosition() {
        return wrotePos.get();
    }

    public void setWrotePosition(int wrotePosition) {
        this.wrotePos.set(wrotePosition);
    }


    public long transferTo(long pos, int length, WritableByteChannel target) {
        return (Long) this.autoReleaseTemplate(x -> {
            try {
                return this.fileChannel.transferTo(pos, length, target);
            } catch (IOException e) {
                log.error("", e);
            }
            return -1l;
        });

    }

    public boolean destroy(long intervalForcibly) {
        this.shutdown(intervalForcibly, file -> {
            DefaultMMapFile mmapfile = (DefaultMMapFile) file;
            if (mmapfile.fileChannel != null && mmapfile.fileChannel.isOpen()) {
                try {
                    mmapfile.fileChannel.close();
                    log.info("close file channel " + mmapfile.fileName + " OK");
                } catch (IOException e) {
                    log.error("", e);
                }
            }
            if (null != mmapfile.file && mmapfile.file.exists()) {
                try {
                    mmapfile.file.delete();
                } catch (Exception e) {
                    log.error("", e);
                }
            }
        });
        return true;
    }


    public boolean isFirstCreateInQueue() {
        return false;
    }

    public void setFirstCreateInQueue(boolean firstCreateInQueue) {

    }


    public int getData(long pos, int size, byte[] dest) {
        if (null == dest || dest.length < size) {
            log.warn("not enough size to load the data");
            return -1;
        }
        if (pos + size > this.getWrotePosition()) {
            log.warn("the pos+size>wrotePosition, request pos: " + pos + ", size: " + size
                    + ", wrotePosition: " + this.getWrotePosition());
            return -1;
        }

        return (Integer) this.autoReleaseTemplate(x -> {
            if (null != this.mappedByteBuffer) {
                ByteBuffer sliceByteBuffer = this.sliceByteBuffer(pos, size);
                if (null == sliceByteBuffer) {
                    return -1;
                }
                sliceByteBuffer.get(dest, 0, size);
                return size;
            } else {
                return -1;
            }
        });

    }


    private ByteBuffer sliceByteBuffer(long pos, int size) {
        if (pos + size > this.getWrotePosition()) {
            log.warn("the pos+size>wrotePosition, request pos: " + pos + ", size: " + size
                    + ", wrotePosition: " + this.getWrotePosition());
            return null;
        }
        ByteBuffer srcByteBuffer = this.mappedByteBuffer.slice();
        srcByteBuffer.position((int) pos);
        ByteBuffer newByteBuffer = srcByteBuffer.slice();
        newByteBuffer.limit(size);
        return newByteBuffer;
    }

    /**
     * @return
     */
    protected boolean cleanup(Consumer<Object>... callbacks) {
        //cleanup virtual momery
        if (!this.available && !this.hashClean && this.refCnt <= 0) {
            MmapUtils.clean(this.mappedByteBuffer);
            this.hashClean = true;
            if (null != callbacks) {
                for (Consumer callback : callbacks) {
                    callback.accept(this);
                }
            }
        }

        return true;
    }


}
