
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
 * Created by CuttleFish on 2020/7/21.
 */

import com.github.jobop.gekko.client.SmartGekkoClient;
import com.github.jobop.gekko.core.config.GekkoClientConfig;

import java.util.concurrent.CountDownLatch;

public class TestClient {
    public static void main(String[] args) throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        SmartGekkoClient client = new SmartGekkoClient(GekkoClientConfig.builder().group("group1")
                .peer("127.0.0.1:8081").peerId("1")
                .peer("127.0.0.1:9091").peerId("2")
                .peer("127.0.0.1:7071").peerId("3")
                .connectTimeout(5000).readTimeout(5000)
                .build());
        client.init();
        client.start();

        for(int i=0;i<1;i++){
            client.append("我日日".getBytes());
        }

        latch.await();
    }
}
