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

import com.github.jobop.gekko.protocols.message.GekkoEntry;
import com.github.jobop.gekko.utils.CodecUtils;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;


public class CodecUtilTest {
    @Test
    public void testEncodeAndDecode() {

        GekkoEntry entry = GekkoEntry.builder().entryIndex(11).pos(999).term(10).data("卧槽".getBytes()).magic(0xCAFE_BABE).build();
        entry.computSizeInBytes();
        entry.setChecksum(entry.checksum());

        ByteBuffer bb = ByteBuffer.allocate(1024 * 1024);
        CodecUtils.encodeData(entry, bb);

        GekkoEntry entry2 = CodecUtils.decodeData(bb);

        System.out.println(entry2.getMagic());
        System.out.println(new String(entry2.getData()));
        System.out.println();
        Assert.assertTrue(entry2.isIntact());

    }
}
