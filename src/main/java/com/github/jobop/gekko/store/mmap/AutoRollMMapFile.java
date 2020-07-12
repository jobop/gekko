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
package com.github.jobop.gekko.store.mmap;


import com.github.jobop.gekko.utils.BytesUtil;
import com.github.jobop.gekko.utils.FileUtils;

import java.io.File;
import java.io.FilenameFilter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;


public class AutoRollMMapFile implements ComposeMMapFile {

    private com.github.jobop.gekko.store.mmap.MmapFile currentMMapFile;

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

    public long appendMessage(byte[] data) {
        return this.appendMessage(data, 0, data.length);
    }

    public long allocPos(byte[] data) {
        return this.allocPos(data, 0, data.length);
    }

    public long allocPos(byte[] data, int offset, int length) {
        MmapFile mmapFile = chooseMMapFile(this.currentMMapFile, length);
        if (null == mmapFile) {
            return -1;
        }
        return mmapFile.getFileFromOffset() + mmapFile.getWrotePosition();
    }

    public long appendMessage(byte[] data, int offset, int length) {
        this.allocPos(data, 0, data.length);
        long pos = this.currentMMapFile.getFileFromOffset() + this.currentMMapFile.getWrotePosition();
        if (-1 == this.currentMMapFile.appendMessage(data, offset, length)) {
            return -1;
        }
        return pos;
    }

    @Override
    public int getData(int pos, int size, byte[] dest) {
        return 0;
    }

    private MmapFile chooseMMapFile(MmapFile currentMMapFile, int length) {
        if (null != currentMMapFile && !currentMMapFile.isFull()) {
            if (currentMMapFile.getFileSize() - currentMMapFile.getWrotePosition() >= length + BLANK_THRESHOLD) {
                return currentMMapFile;
            } else {
                //write a flag what means the file has full
                currentMMapFile.appendMessage(EOF);
                currentMMapFile.setFlushedPosition(currentMMapFile.getFileSize());
                currentMMapFile.setWrotePosition(currentMMapFile.getFileSize());
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
}
