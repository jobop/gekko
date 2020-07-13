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
 */

package com.github.jobop.gekko.store.mmap;

import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;

public interface MmapFile extends SequenceFile {
    /**
     * Returns the file name of the {@code MappedFile}.
     *
     * @return the file name
     */
    String getFileName();

    /**
     * Returns the file size of the {@code MappedFile}.
     *
     * @return the file size
     */
    int getFileSize();

    /**
     * Returns the {@code FileChannel} behind the {@code MappedFile}.
     *
     * @return the file channel
     */
    FileChannel getFileChannel();

    /**
     * Returns true if this {@code MappedFile} is full and no new messages can be added.
     *
     * @return true if the file is full
     */
    boolean isFull();

    /**
     * Returns true if this {@code MappedFile} is available.
     * <p>
     * The mapped file will be not available if it's shutdown or destroyed.
     *
     * @return true if the file is available
     */
    boolean isAvailable();


    /**
     * Returns the global offset of the current {code MappedFile}, it's a long value of the file name.
     *
     * @return the offset of this file
     */
    long getFileFromOffset();

    /**
     * Flushes the data in cache to disk immediately.
     *
     * @param flushLeastPages the least pages to flush
     * @return the flushed position after the method call
     */
    int flush(int flushLeastPages);


    /**
     * Selects a slice of the mapped byte buffer's sub-region behind the mapped file,
     * starting at the given position.
     *
     * @param pos  the given position
     * @param size the size of the returned sub-region
     * @return a {@code SelectMappedBufferResult} instance contains the selected slice
     */
    SlicedByteBuffer selectMappedBuffer(int pos, int size);

    /**
     * Selects a slice of the mapped byte buffer's sub-region behind the mapped file,
     * starting at the given position.
     *
     * @param pos the given position
     * @return a {@code SelectMappedBufferResult} instance contains the selected slice
     */
    SlicedByteBuffer selectMappedBuffer(int pos);

    /**
     * Returns the mapped byte buffer behind the mapped file.
     *
     * @return the mapped byte buffer
     */
    MappedByteBuffer getMappedByteBuffer();

    /**
     * Returns a slice of the mapped byte buffer behind the mapped file.
     *
     * @return the slice of the mapped byte buffer
     */
    ByteBuffer sliceByteBuffer();

    /**
     * Returns the last modified timestamp of the file.
     *
     * @return the last modified timestamp
     */
    long getLastModifiedTimestamp();

    /**
     * Destroys the file and delete it from the file system.
     *
     * @param intervalForcibly If {@code true} then this method will destroy the file forcibly and ignore the reference
     * @return true if success; false otherwise.
     */
    boolean destroy(long intervalForcibly);


    /**
     * Returns true if the current file is first mapped file of some consume queue.
     *
     * @return true or false
     */
    boolean isFirstCreateInQueue();

    /**
     * Sets the flag whether the current file is first mapped file of some consume queue.
     *
     * @param firstCreateInQueue true or false
     */
    void setFirstCreateInQueue(boolean firstCreateInQueue);

    /**
     * Returns the flushed position of this mapped file.
     *
     * @return the flushed posotion
     */
    int getFlushedPosition();

    /**
     * Sets the flushed position of this mapped file.
     *
     * @param flushedPosition the specific flushed position
     */
    void setFlushedPosition(int flushedPosition);

    /**
     * Returns the start position of this mapped file, before which the data is truncated
     *
     * @return the wrote position
     */
    int getStartPosition();

    /**
     * Sets the start position of this mapped file.
     *
     * @param startPosition the specific start position
     */
    void setStartPosition(int startPosition);

    /**
     * Returns the wrote position of this mapped file.
     *
     * @return the wrote position
     */
    int getWrotePosition();

    /**
     * Sets the wrote position of this mapped file.
     *
     * @param wrotePosition the specific wrote position
     */
    void setWrotePosition(int wrotePosition);


    long transferTo(long pos, int length, WritableByteChannel target);
}
