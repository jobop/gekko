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
 * Created by CuttleFish on 2020/7/11.
 */

import com.github.jobop.gekko.store.mmap.DefaultMMapFile;
import org.junit.Assert;
import org.junit.Test;

public class DefaultMMapFileTest {
    @Test
    public void testGeneraterMmapFile() {
        DefaultMMapFile file = new DefaultMMapFile("/Users/zhengwei/Desktop/aaa/1234567", 1024 * 1024 * 4, 1024 * 4);
        Assert.assertEquals(1234567, file.getFileFromOffset());
    }

    @Test
    public void testAppendMmapFile() {
        DefaultMMapFile file = new DefaultMMapFile("/Users/zhengwei/Desktop/aaa/12345678", 1024 * 1024 * 4, 1024 * 4);
        byte[] needAppendBytes = "我来了".getBytes();

        for (int i = 0; i < 10000; i++) {
            int beforeWrotePosition = file.getWrotePosition();
            file.appendMessage(needAppendBytes);
            int afterWrotePosition = file.getWrotePosition();
            Assert.assertTrue("assert wrotePosition", (beforeWrotePosition + needAppendBytes.length == afterWrotePosition));

        }
    }


    @Test
    public void testGetData() {
        DefaultMMapFile file = new DefaultMMapFile("/Users/zhengwei/Desktop/aaa/123456789", 1024 * 1024 * 4, 1024 * 4);
        byte[] needAppendBytes = "我来了".getBytes();

        for (int i = 0; i < 10000; i++) {
            int beforeWrotePosition = file.getWrotePosition();
            file.appendMessage(needAppendBytes);
            int afterWrotePosition = file.getWrotePosition();
            Assert.assertTrue("assert wrotePosition", (beforeWrotePosition + needAppendBytes.length == afterWrotePosition));
        }

//        Assert.assertTrue(file.isAvailable());
//        Assert.assertTrue(file.getFileChannel().isOpen());
//        Assert.assertTrue(file.refCnt() > 0);
//        file.destroy(1000);
//
//        Assert.assertFalse(file.isAvailable());
//        Assert.assertFalse(file.getFileChannel().isOpen());
//        Assert.assertFalse(file.refCnt() > 0);


        byte[] destBytes=new byte[needAppendBytes.length];
        file.getData(needAppendBytes.length*9999,needAppendBytes.length,destBytes);
        String readedStr=new String(destBytes);
        System.out.println(readedStr);

        Assert.assertEquals("我来了",readedStr);
    }

    @Test
    public void testDestroyMmapFile() {
        DefaultMMapFile file = new DefaultMMapFile("/Users/zhengwei/Desktop/aaa/123456789", 1024 * 1024 * 4, 1024 * 4);
        byte[] needAppendBytes = "我来了".getBytes();

        for (int i = 0; i < 10000; i++) {
            int beforeWrotePosition = file.getWrotePosition();
            file.appendMessage(needAppendBytes);
            int afterWrotePosition = file.getWrotePosition();
            Assert.assertTrue("assert wrotePosition", (beforeWrotePosition + needAppendBytes.length == afterWrotePosition));
        }

        Assert.assertTrue(file.isAvailable());
        Assert.assertTrue(file.getFileChannel().isOpen());
        Assert.assertTrue(file.refCnt() > 0);
        file.destroy(1000);

        Assert.assertFalse(file.isAvailable());
        Assert.assertFalse(file.getFileChannel().isOpen());
        Assert.assertFalse(file.refCnt() > 0);
    }
}
