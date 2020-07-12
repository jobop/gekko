
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
 * Created by CuttleFish on 2020/7/12.
 */

package com.github.jobop.gekko.utils;

public class BytesUtil {
    public static int bytes2Int(byte[] bytes) {
        int result = 0;
        result = bytes[0] & 0xff;
        result = result << 8 | bytes[1] & 0xff;
        result = result << 8 | bytes[2] & 0xff;
        result = result << 8 | bytes[3] & 0xff;
        return result;
    }

    public static byte[] int2Bytes(int num) {
        byte[] bytes = new byte[4];
        bytes[0] = (byte)(num >>> 24);
        bytes[1] = (byte)(num >>> 16);
        bytes[2] = (byte)(num >>> 8);
        bytes[3] = (byte)num;
        return bytes;
    }
}
