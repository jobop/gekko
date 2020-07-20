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
 * Created by CuttleFish on 2020/7/3.
 */

import com.github.jobop.gekko.GekkoCli;
import com.github.jobop.gekko.core.GekkoConfig;
import com.github.jobop.gekko.core.statemachine.StateMachineAdapter;
import com.github.jobop.gekko.enums.StoreEnums;
import com.github.jobop.gekko.protocols.message.GekkoEntry;

public class TestNode3 {
    public static void main(String[] args) {
        GekkoConfig conf = GekkoConfig.builder()
                .group("group1")
                .peer("127.0.0.1:8080").peerId("1")
                .peer("127.0.0.1:9090").peerId("2")
                .peer("127.0.0.1:7070").peerId("3")
                .selfId("3")
                .indexCountPerFile(1000000)
                .baseFilePath("cekko3")
                .flushInterval(1)
                .storeFileSize(1024 * 1024 )
                .osPageSize(1024*4)
                .storeType(StoreEnums.FILE)
                .stateMachine(new StateMachineAdapter() {
                    @Override
                    public void onAppend(GekkoEntry entry) {
                        super.onAppend(entry);
                    }
                }).build();


        GekkoCli cli = new GekkoCli(conf);
        cli.start();
    }
}
