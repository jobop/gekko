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

    private static int FILE_META_DATA_SIZE = 8 + 4;
    private MmapFile currentMMapFile;

    private final static String DATA_FILE_NAME_REGEX_NUMERIC = "^\\d+$"; //$NON-NLS-1$

    private final static String META_DATA_FILE_NAME_REGEX_NUMERIC = ".*(.meta)$"; //$NON-NLS-1$
    private static Pattern DATA_FILE_PATTERN = Pattern.compile(DATA_FILE_NAME_REGEX_NUMERIC);

    private static Pattern META_DATA_FILE_PATTERN = Pattern.compile(META_DATA_FILE_NAME_REGEX_NUMERIC);
    private long index;

    private CopyOnWriteArrayList<MmapFile> allMetaDataFiles = new CopyOnWriteArrayList<MmapFile>();
    private CopyOnWriteArrayList<MmapFile> allFiles = new CopyOnWriteArrayList<MmapFile>();
    private final String storePath;
    private int singleFileSize = 1024 * 1024 * 40;
    private int osPageSize = 1024 * 4;
    private static final int EOF_MAGIC = 0xCAFEFFFF;
    private static int BLANK_THRESHOLD = 0;
    private AtomicInteger hasLoad = new AtomicInteger(0);
    private AtomicInteger hasLoadMetaDataFile = new AtomicInteger(0);

    private ThreadLocal<ByteBuffer> localByteBuffer = ThreadLocal.withInitial(() -> {
        return ByteBuffer.allocate(1024 * 1024);
    });

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


        loadDataFiles(storeDir);
        log.info("[1] load datafile pass!");

        PreConditions.check(this.checksum(), ResultEnums.LOAD_FILE_FAIL, "the checksum step can not pass!");
        log.info("[2] datafile checksum step pass!");

        PreConditions.check(this.recover(), ResultEnums.LOAD_FILE_FAIL, "the recover step can not pass!");
        log.info("[3] recover datafile pass!");


    }

//    private void loadMetaDataFiles(File storeDir) {
//        List<MmapFile> localfiles = loadMmapFilesByNamePattern(storeDir);
//        if (localfiles == null) return;
//        this.allFiles.addAll(localfiles);
//        if (this.allFiles.size() > 0) {
//            this.currentMMapFile = this.allFiles.get(this.allFiles.size() - 1);
//        }
//        return;
//    }

    private void loadMetaDataFiles(File storeDir) {
        List<MmapFile> loadfiles = loadMmapFilesByNamePattern(storeDir, META_DATA_FILE_PATTERN, FILE_META_DATA_SIZE);
        if (loadfiles == null) return;
        this.allMetaDataFiles.addAll(loadfiles);
        return;
    }

    private void loadDataFiles(File storeDir) {
        List<MmapFile> loadfiles = loadMmapFilesByNamePattern(storeDir, DATA_FILE_PATTERN, this.singleFileSize);
        if (loadfiles == null) return;
        this.allFiles.addAll(loadfiles);
        if (this.allFiles.size() > 0) {
            this.currentMMapFile = this.allFiles.get(this.allFiles.size() - 1);
        }
        return;
    }

    private List<MmapFile> loadMmapFilesByNamePattern(File storeDir, Pattern pattern, int fileSize) {
        List<MmapFile> localfiles = new ArrayList<MmapFile>();
        File[] files = storeDir.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return pattern.matcher(name).matches();
            }
        });
        if (null == files || files.length <= 0) {
            return null;
        }


        for (File file : files) {
            DefaultMMapFile mmapFile = new DefaultMMapFile(file.getPath(), fileSize, this.osPageSize);
            mmapFile.setWrotePosition(fileSize);
            mmapFile.setFlushedPosition(fileSize);
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
        return localfiles;
    }


    private boolean recover() {
        for (MmapFile dataFile : this.allFiles) {
            String metaDataFileName = dataFile.getFileName() + ".meta";
            MmapFile metaDataFile = new DefaultMMapFile(metaDataFileName, FILE_META_DATA_SIZE, this.osPageSize);
            boolean needRepairMetaData = false;
            if (metaDataFile.getLimit() != FILE_META_DATA_SIZE) {
                needRepairMetaData = true;
                log.warn("the metadatafile not match the length,will repair!");
            } else {
                ByteBuffer metaDataByteBuffer = metaDataFile.sliceByteBuffer();
                Long dataFileFromOffet = metaDataByteBuffer.getLong();
                int limit = metaDataByteBuffer.getInt();

                if (limit == dataFile.getFileSize()) {
                    dataFile.setWrotePosition(dataFile.getFileSize());
                    dataFile.setLimit(limit);
                } else {
                    int eofPos = limit;
                    if (eofPos > dataFile.getFileSize()) {
                        needRepairMetaData = true;
                    } else {
                        metaDataByteBuffer.position(eofPos);
                        if (metaDataByteBuffer.getInt() == EOF_MAGIC) {
                            dataFile.setWrotePosition(dataFile.getFileSize());
                            dataFile.setLimit(limit);
                        } else {
                            needRepairMetaData = true;
                        }
                    }
                }

            }

            if (needRepairMetaData) {
                repairMetaData(dataFile, metaDataFile);
            }
        }

        return true;
    }
    //FIXME:it's not good to dependency to the data what we write,we can wrap a protocol the identify the size
    private void repairMetaData(MmapFile dataFile, MmapFile metaDataFile) {
        ByteBuffer byteBuffer = dataFile.sliceByteBuffer();

        int posInfile = 0;
        while (true) {
            int magic = byteBuffer.getInt();
            if (magic == EOF_MAGIC) {
                int limit = byteBuffer.getInt();
                dataFile.setLimit(limit);
                dataFile.setWrotePosition(dataFile.getFileSize());
                log.warn("the limit in the metadatafile will repair to " + limit + " filename=" + dataFile.getFileName());
                writeMetaDataFile(dataFile, metaDataFile);
                this.allMetaDataFiles.add(dataFile);
                break;
            }
            int entrySize = byteBuffer.getInt();
            if (entrySize == 0) {
                dataFile.setLimit(posInfile);
                dataFile.setWrotePosition(posInfile);
                break;
            }
            posInfile += entrySize;
            if (posInfile >= dataFile.getFileSize()) {
                dataFile.setLimit(dataFile.getFileSize());
                dataFile.setWrotePosition(dataFile.getFileSize());
                writeMetaDataFile(dataFile, metaDataFile);
                this.allMetaDataFiles.add(dataFile);

                break;
            }
            byteBuffer.position(posInfile);
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

    public List<SlicedByteBuffer> selectMutilBufferToRead(long fromPos, long toPos) {
        if(toPos==-1){
            toPos=this.getMaxOffset();
        }
        long size = toPos - fromPos;
        List<SlicedByteBuffer> buffers = new ArrayList<SlicedByteBuffer>();
        //calac the pos belong to which file
        int fileIndex = (int) fromPos / this.singleFileSize;
        //calac the pos in the file
        int posInFile = (int) (fromPos % this.singleFileSize);

        long willSelectedSize = 0;
        do {
            if (fileIndex > this.allFiles.size() - 1) {
                log.warn("the ops overflow ops=" + fromPos + " and filelist size=" + this.allFiles.size());
                return null;
            }
            MmapFile file = this.allFiles.get(fileIndex);
            long fileCanSelectSize = file.getWrotePosition() - posInFile;
            willSelectedSize = fileCanSelectSize + willSelectedSize;
            long remainingToselectSize = size - willSelectedSize;
            long fileCanReadSize = file.getLimit() - posInFile;

            if (file.getFileFromOffset() + file.getFileSize() >= toPos) {
                if (remainingToselectSize < 0) {
                    buffers.add(file.selectMappedBuffer(posInFile, (int) (fileCanSelectSize + remainingToselectSize)));
                } else {
                    buffers.add(file.selectMappedBuffer(posInFile, (int) willSelectedSize));
                }
                break;
            } else {
                buffers.add(file.selectMappedBuffer(posInFile, (int) fileCanReadSize));
            }
            ++fileIndex;
            posInFile = 0;
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
        if (fileIndex > this.allFiles.size()-1) {
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
                ByteBuffer bb = ByteBuffer.allocate(4 + 4);
                bb.putInt(EOF_MAGIC);
                bb.putInt(oldLimit);


                currentMMapFile.appendMessage(bb.array());


                this.currentMMapFile.setFlushedPosition(currentMMapFile.getFileSize());
                this.currentMMapFile.setWrotePosition(currentMMapFile.getFileSize());
                this.currentMMapFile.setLimit(oldLimit);
                //save this limit to metadata file
                saveFileMetaData(this.currentMMapFile);
                return createNewMMapFile(currentMMapFile);
            }

        } else {
            return createNewMMapFile(currentMMapFile);
        }

    }

    private void saveFileMetaData(MmapFile archiveFile) {
        String fileName = archiveFile.getFileName() + ".meta";
        MmapFile newFile = new DefaultMMapFile(fileName, FILE_META_DATA_SIZE, this.osPageSize);
        writeMetaDataFile(archiveFile, newFile);


        this.allMetaDataFiles.add(newFile);
    }

    private void writeMetaDataFile(MmapFile archiveFile, MmapFile metaFile) {
        ByteBuffer bb = metaFile.sliceByteBuffer();
        bb.putLong(archiveFile.getFileFromOffset());
        bb.putInt(archiveFile.getLimit());
    }

    private MmapFile createNewMMapFile(MmapFile currentMMapFile) {

        String fileName;
        if (null == currentMMapFile) {
            fileName = "0";
        } else {
            fileName = String.valueOf(currentMMapFile.getFileFromOffset() + currentMMapFile.getFileSize());
            saveFileMetaData(this.currentMMapFile);
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
