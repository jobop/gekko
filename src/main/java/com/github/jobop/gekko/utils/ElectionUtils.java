
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
 * Created by CuttleFish on 2020/7/19.
 */

package com.github.jobop.gekko.utils;

import com.github.jobop.gekko.core.metadata.NodeState;
import com.github.jobop.gekko.enums.VoteResultEnums;
import com.github.jobop.gekko.protocols.message.node.VoteResp;

public class ElectionUtils {
    public static boolean judgVote(long nowTerm, long voteTerm, long nowLastIndex, long remoteLastIndex) {
        if (nowTerm < voteTerm) {
            if (remoteLastIndex >= nowLastIndex) {
                return true;
            } else {
                return false;
            }
        } else {
            return false;
        }

    }
}
