package com.github.jobop.cekko.connector;
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

import com.github.jobop.cekko.core.CekkoConfig;
import com.github.jobop.cekko.core.lifecycle.LifeCycleAdpter;
import com.github.jobop.cekko.protocols.GekkoInboundProtocol;

/**
 * This class use to handle all the requests from another node and client sides
 */
public class GekkoNettyServer extends LifeCycleAdpter {
    GekkoInboundProtocol inboundHelper;
    CekkoConfig conf;

    public GekkoNettyServer(CekkoConfig conf, GekkoInboundProtocol inboundHelper) {
        this.inboundHelper = inboundHelper;
        this.conf = conf;
    }

    public void init() {

    }

    public void start() {

    }

    public void shutdown() {

    }
}
