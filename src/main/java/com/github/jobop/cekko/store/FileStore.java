package com.github.jobop.cekko.store;

import com.alipay.remoting.util.IoUtils;
import com.github.jobop.cekko.core.CekkoConfig;
import com.github.jobop.cekko.protocols.message.GekkoEntry;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;

import java.io.File;
import java.io.IOException;
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
 * Created by CuttleFish on 2020/7/2.
 */
@Slf4j
public class FileStore extends AbstractStore {
    private String BASE_FILE_PATH = "cekko";
    private final String TERM_FILE_NAME = "term.cek";
    private final String LOG_FILE_NAME = "term.cek";
    private File termFile;
    private File indexFile;

    public FileStore(CekkoConfig conf) {
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
        this.termFile = new File(BASE_FILE_PATH + File.separator + TERM_FILE_NAME);
        try {
            FileUtils.touch(this.termFile);
        } catch (IOException e) {
            log.error("", e);
        }
//        this.logFile = new File(BASE_FILE_PATH + File.separator + LOG_FILE_NAME);
//        try {
//            FileUtils.touch(this.logFile);
//        } catch (IOException e) {
//            log.error("", e);
//        }

    }

    @Override
    public void append(GekkoEntry entry) {

    }

    @Override
    public List<GekkoEntry> get(long offset, long length) {
        return null;
    }
}
