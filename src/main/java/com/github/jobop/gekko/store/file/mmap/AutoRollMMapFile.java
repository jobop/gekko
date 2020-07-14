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


import com.github.jobop.gekko.enums.ResultEnums;
import com.github.jobop.gekko.store.file.ComposeMMapFile;
import com.github.jobop.gekko.store.file.MmapFile;
import com.github.jobop.gekko.store.file.SequenceFile;
import com.github.jobop.gekko.store.file.SlicedAble;
import com.github.jobop.gekko.utils.FileUtils;
import com.github.jobop.gekko.utils.PreConditions;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.FilenameFilter;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

@Slf4j
public class AutoRollMMapFile implements ComposeMMapFile, SequenceFile, SlicedAble {

    private MmapFile currentMMapFile;

    private final static String REGEX_NUMERIC = "^\\d+$"; //$NON-NLS-1$
    private Pattern pattern = Pattern.compile(REGEX_NUMERIC);
    private long index;
    private CopyOnWriteArrayList<MmapFile> allFiles = new CopyOnWriteArrayList<MmapFile>();
    private final String storePath;
    private int singleFileSize = 1024 * 1024 * 40;
    private int osPageSize = 1024 * 4;
    private static final byte[] EOF = new byte[]{0xC, 0xA, 0xF, 0xE, 0xD, 0xA, 0xD, 0xD};
    private static int BLANK_THRESHOLD = 8;
    private AtomicInteger hasLoad = new AtomicInteger(0);

    public AutoRollMMapFile(String storePath, int singleFileSize, int osPageSize) {
        this.storePath = storePath;
        this.singleFileSize = singleFileSize;
        this.osPageSize = osPageSize;

    }

    @Override
    public void load() {
        if (!hasLoad.compareAndSet(0, 1)) {
            return;
        }
        //search storePath to find out all files
        File storeDir = new File(this.storePath);
        if (!storeDir.exists()) {
            FileUtils.forceMkdir(storeDir);
        }
        File[] files = storeDir.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return pattern.matcher(name).matches();
            }
        });
        if (null == files || files.length <= 0) {
            return;
        }

        List<MmapFile> localfiles = new ArrayList<MmapFile>();
        for (File file : files) {
            DefaultMMapFile mmapFile = new DefaultMMapFile(file.getPath(), this.singleFileSize, this.osPageSize);
            mmapFile.setWrotePosition(this.singleFileSize);
            mmapFile.setFlushedPosition(this.singleFileSize);
            localfiles.add(mmapFile);

        }
        localfiles.sort(new Comparator<MmapFile>() {
            @Override
            public int compare(MmapFile o1, MmapFile o2) {
                if (o1.getFileFromOffset() < o2.getFileFromOffset()) {
                    return -1;
                } else {
                    return 1;
                }
            }
        });
        this.allFiles.addAll(localfiles);
        if (this.allFiles.size() > 0) {
            this.currentMMapFile = this.allFiles.get(this.allFiles.size() - 1);
        }
        log.info("[1] load step pass!");
        PreConditions.check(this.checksum(), ResultEnums.LOAD_FILE_FAIL, "the checksum step can not pass!");
        log.info("[2] checksum step pass!");
        PreConditions.check(this.recover(), ResultEnums.LOAD_FILE_FAIL, "the recover step can not pass!");
        log.info("[3] recover step pass!");
    }

    private boolean recover() {
        return true;
    }

    @Override
    public boolean checksum() {
        if (!this.allFiles.isEmpty()) {
            long prefromOffset = 0;
            for (int i = 0; i < allFiles.size(); i++) {
                MmapFile thisFile = this.allFiles.get(i);
                long thisFileFromOffset = thisFile.getFileFromOffset();
                if (i == 0) {
                    prefromOffset = thisFileFromOffset;
                    continue;
                }
                if (prefromOffset + this.singleFileSize != thisFileFromOffset) {
                    return false;
                }
                prefromOffset = thisFileFromOffset;
            }
        }
        return true;
    }

    @Override
    public void flush(int flushLeastPages) {
        if (null != this.currentMMapFile) {
            this.currentMMapFile.flush(1);
        }
    }


    public long appendMessage(byte[] data) {
        return this.appendMessage(data, 0, data.length);
    }


    public long allocPos(int length) {
        MmapFile mmapFile = chooseMMapFileToWrite(this.currentMMapFile, length);
        if (null == mmapFile) {
            return -1;
        }
        return mmapFile.getFileFromOffset() + mmapFile.getWrotePosition();
    }

    public long appendMessage(byte[] data, long offset, int length) {
        this.allocPos(data.length);
        long pos = this.currentMMapFile.getFileFromOffset() + this.currentMMapFile.getWrotePosition();
        if (-1 == this.currentMMapFile.appendMessage(data, offset, length)) {
            return -1;
        }
        return pos;
    }

    @Override
    public int getData(long pos, int size, byte[] dest) {
        SlicedByteBuffer slicedByteBuffer = this.selectMappedBuffer(pos, size);
        if (null == slicedByteBuffer) {
            return -1;
        }
        int readedSize = 0;
        if (size == -1) {
            slicedByteBuffer.get(dest);
            readedSize = dest.length;
        } else {
            slicedByteBuffer.get(dest, 0, size);
            readedSize = size;
        }

        slicedByteBuffer.release();
        return readedSize;
    }


    public List<SlicedByteBuffer> selectMutilBufferToRead(long fromPos, int toPos) {
        long size = toPos - fromPos;
        List<SlicedByteBuffer> buffers = new ArrayList<SlicedByteBuffer>();
        //calac the pos belong to which file
        int pretFileIndex = (int) fromPos / this.singleFileSize;
        int pretPosInFile = (int) (fromPos % this.singleFileSize);

        long hasSelectedSize = 0;
        do {
            if (pretFileIndex > this.allFiles.size() - 1) {
                log.warn("the ops overflow ops=" + fromPos + " and filelist size=" + this.allFiles.size());
                return null;
            }
            MmapFile preFile = this.allFiles.get(pretFileIndex);

            long preFileSelectedRemaining = preFile.getWrotePosition() - pretPosInFile;
            hasSelectedSize = preFileSelectedRemaining + hasSelectedSize;

            long remainingToselect = size - hasSelectedSize;

            long preFileReadRemaining = preFile.getLimit() - pretPosInFile;

            if (preFile.getFileFromOffset() + preFile.getFileSize() >= toPos) {
                if (remainingToselect < 0) {
                    buffers.add(preFile.selectMappedBuffer(pretPosInFile, (int) (preFileSelectedRemaining + remainingToselect)));
                } else {
                    buffers.add(preFile.selectMappedBuffer(pretPosInFile, (int) remainingToselect));
                }
                break;
            } else {
                buffers.add(preFile.selectMappedBuffer(pretPosInFile, (int) preFileReadRemaining));
            }
            ++pretFileIndex;
            pretPosInFile = 0;
        } while (true);


        return buffers;
    }

    @Override
    public SlicedByteBuffer selectMappedBuffer(long pos, int size) {

        MmapFile file = chooseMMapFileToRead(pos);
        if (null == file) {
            return null;
        }
        int posInFile = (int) (pos % this.singleFileSize);
        if (posInFile > file.getWrotePosition()) {
            log.warn("the posInFile overflow posInFile=" + posInFile + " and filewrotepos=" + file.getWrotePosition());
            return null;
        }
        SlicedByteBuffer slicedByteBuffer;
        if (size == -1) {
            slicedByteBuffer = file.selectMappedBuffer(posInFile);
        } else {
            slicedByteBuffer = file.selectMappedBuffer(posInFile, size);
        }

        return slicedByteBuffer;
    }

    @Override
    public SlicedByteBuffer selectMappedBuffer(long pos) {
        return this.selectMappedBuffer(pos, -1);
    }

    private MmapFile chooseMMapFileToRead(long pos) {
        //calac the pos belong to which file
        int fileIndex = (int) pos / this.singleFileSize;
        if (fileIndex > this.allFiles.size()) {
            log.warn("the ops overflow ops=" + pos + " and filelist size=" + this.allFiles.size());
            return null;
        }
        MmapFile file = this.allFiles.get(fileIndex);
        return file;
    }

    private MmapFile chooseMMapFileToWrite(MmapFile currentMMapFile, int length) {
        if (null != currentMMapFile && !currentMMapFile.isFull()) {
            if (currentMMapFile.getFileSize() - currentMMapFile.getWrotePosition() >= length + BLANK_THRESHOLD) {
                return currentMMapFile;
            } else {
                //write a flag what means the file has full
                int oldLimit = currentMMapFile.getLimit();

                //for recover
                ByteBuffer bb = ByteBuffer.allocate(EOF.length + 4);
                bb.put(EOF);
                bb.putInt(oldLimit);
                currentMMapFile.appendMessage(bb.array());

                currentMMapFile.setFlushedPosition(currentMMapFile.getFileSize());
                currentMMapFile.setWrotePosition(currentMMapFile.getFileSize());
                currentMMapFile.setLimit(oldLimit);
                return createNewMMapFile(currentMMapFile);
            }

        } else {
            return createNewMMapFile(currentMMapFile);
        }

    }

    private MmapFile createNewMMapFile(MmapFile currentMMapFile) {
        String fileName;
        if (null == currentMMapFile) {
            fileName = "0";
        } else {
            fileName = String.valueOf(currentMMapFile.getFileFromOffset() + currentMMapFile.getFileSize());
        }
        MmapFile newFile = new DefaultMMapFile(storePath + File.separator + fileName, this.singleFileSize, this.osPageSize);
        this.currentMMapFile = newFile;
        this.allFiles.add(newFile);
        return newFile;
    }

    public long trimBefore(long pos) {
        List<MmapFile> needRemoveFiles = new ArrayList<MmapFile>();
        for (MmapFile file : this.allFiles) {
            if (file.getFileFromOffset() >= pos) {
                break;
            }
            if (file.getFileFromOffset() + this.singleFileSize < pos) {
                needRemoveFiles.add(file);
            }
            if (file.getFileFromOffset() <= pos && file.getFileFromOffset() + this.singleFileSize > pos) {
                file.trimBefore(pos % this.singleFileSize);
            }
        }
        clearExpireFiles(needRemoveFiles);
        return pos;
    }

    public long trimAfter(long pos) {
        List<MmapFile> needRemoveFiles = new ArrayList<MmapFile>();
        for (MmapFile file : this.allFiles) {
            if (file.getFileFromOffset() <= pos) {
                continue;
            }
            if (file.getFileFromOffset() > pos) {
                needRemoveFiles.add(file);
            }
            if (file.getFileFromOffset() <= pos && file.getFileFromOffset() + this.singleFileSize > pos) {
                file.trimAfter(pos % this.singleFileSize);
            }
        }
        clearExpireFiles(needRemoveFiles);
        return pos;
    }

    private void clearExpireFiles(List<MmapFile> needRemoveFiles) {
        for (MmapFile file : needRemoveFiles) {
            file.destroy(1000);
            if (this.allFiles.contains(file)) {
                this.allFiles.remove(file);
            }
        }
    }


    @Override
    public long getMinOffset() {
        if (this.allFiles.isEmpty()) {
            return 0;
        }
        MmapFile file = this.allFiles.get(0);
        return file.getStartPosition();
    }

    @Override
    public long getMaxOffset() {
        if (this.allFiles.isEmpty()) {
            return 0;
        }
        MmapFile file = this.allFiles.get(this.allFiles.size() - 1);
        return file.getFileFromOffset() + file.getMaxOffset();
    }
}
