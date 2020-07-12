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

package com.github.jobop.gekko.core.exception;


import com.github.jobop.gekko.enums.ResultEnums;


public class GekkoException extends RuntimeException {
    Throwable t;
    private String code;
    private String msg;

    public GekkoException(Throwable t) {
        this.t = t;
    }

    public GekkoException(ResultEnums result) {
        this.code = result.getCode();
        this.msg = result.getMsg();
    }

    public GekkoException(String code, String msg) {
        this.code = code;
        this.msg = msg;
    }

}
