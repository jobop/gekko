package com.github.jobop.gekko.utils;

import java.util.stream.Stream;

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
 * Created by CuttleFish on 2020/7/22.
 */
public interface TailRecursion<T> {
    /**
     * 用于递归栈帧之间的连接,惰性求值
     *
     * @return 下一个递归栈帧
     */
    TailRecursion<T> apply();

    /**
     * 判断当前递归是否结束
     *
     * @return 默认为false, 因为正常的递归过程中都还未结束
     */
    default boolean isFinished() {
        return false;
    }

    /**
     * 获得递归结果,只有在递归结束才能调用,这里默认给出异常,通过工具类的重写来获得值
     *
     * @return 递归最终结果
     */
    default T getResult() {
        throw new Error("递归还没有结束,调用获得结果异常!");
    }

    /**
     * 及早求值,执行者一系列的递归,因为栈帧只有一个,所以使用findFirst获得最终的栈帧,接着调用getResult方法获得最终递归值
     *
     * @return 及早求值, 获得最终递归结果
     */
    default T invoke() {
        return Stream.iterate(this, TailRecursion::apply)
                .filter(TailRecursion::isFinished)
                .findFirst()
                .get()
                .getResult();
    }
}

