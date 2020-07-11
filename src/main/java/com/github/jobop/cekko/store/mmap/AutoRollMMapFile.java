package com.github.jobop.cekko.store.mmap;

import java.util.ArrayList;
import java.util.List;

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
public class AutoRollMMapFile {
    private DefaultMMapFile currentMMapFile;
    private DefaultMMapFile indexFile;
    private long index;
    private List<DefaultMMapFile> allFiles = new ArrayList<DefaultMMapFile>();


    boolean appendMessage(byte[] data) {
        return true;
    }

    boolean appendMessage(byte[] data, int offset, int length) {
        return true;
    }

    private DefaultMMapFile chooseMMapFile(DefaultMMapFile currentMMapFile) {
        if (null != currentMMapFile && !currentMMapFile.isFull()) {
            return currentMMapFile;
        } else {
            return createNewMMapFile(currentMMapFile);
        }

    }

    private DefaultMMapFile createNewMMapFile(DefaultMMapFile currentMMapFile) {
        if (null == currentMMapFile) {
            return new DefaultMMapFile(String.valueOf(index), 1024 * 1024 * 40, 1024 * 4);
        } else {
            //TODO:
            return null;
        }
    }
}
