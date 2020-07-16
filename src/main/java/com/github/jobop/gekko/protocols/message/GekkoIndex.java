
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
 * Created by CuttleFish on 2020/7/14.
 */

package com.github.jobop.gekko.protocols.message;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class GekkoIndex {
    public static int INDEX_SIZE = 4 + 4 + 8 + 8 + 4;
    int magic = 0xCAFEDADE;
    int totalSize = INDEX_SIZE;
    long dataPos;
    long dataIndex;
    int dataSize;
}
