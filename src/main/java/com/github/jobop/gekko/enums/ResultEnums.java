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
 * Created by CuttleFish on 2020/7/5.
 */
package com.github.jobop.gekko.enums;


public enum ResultEnums {
    SUCCESS("SUCCESS", "SUCCESS"),



    PEER_OR_PEERID_CANNOT_BE_NULL("PEER_OR_PEERID_CANNOT_BE_NULL", "PEER_OR_PEERID_CANNOT_BE_NULL"),
    PEER_AND_PEERID_SIZE_NOT_MATCH("PEER_AND_PEERID_SIZE_NOT_MATCH", "PEER_AND_PEERID_SIZE_NOT_MATCH"),
    PEER_AND_API_PORTS_SIZE_NOT_MATCH("PEER_AND_API_PORTS_SIZE_NOT_MATCH", "PEER_AND_API_PORTS_SIZE_NOT_MATCH"),

    REFERENCE_COUNTED("ReferenceCounted","ReferenceCounted"),

    LOAD_FILE_FAIL("LoadFileFail","LoadFileFail"),

    CLIENT_LOAD_FAIL("CLIENT_LOAD_FAIL","CLIENT_LOAD_FAIL")

    ;
    private String code;
    private String msg;

    ResultEnums(String code, String msg) {
        this.code = code;
        this.msg = msg;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }
}
