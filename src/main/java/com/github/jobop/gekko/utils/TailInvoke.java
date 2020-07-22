
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

package com.github.jobop.gekko.utils;

public class TailInvoke {
    /**
     * 统一结构的方法,获得当前递归的下一个递归
     *
     * @param nextFrame 下一个递归
     * @param <T>       T
     * @return 下一个递归
     */
    public static <T> TailRecursion<T> call(final TailRecursion<T> nextFrame) {
        return nextFrame;
    }

    /**
     * 结束当前递归，重写对应的默认方法的值,完成状态改为true,设置最终返回结果,设置非法递归调用
     *
     * @param value 最终递归值
     * @param <T>   T
     * @return 一个isFinished状态true的尾递归, 外部通过调用接口的invoke方法及早求值, 启动递归求值。
     */
    public static <T> TailRecursion<T> done(T value) {
        return new TailRecursion<T>() {
            @Override
            public TailRecursion<T> apply() {
                throw new Error("递归已经结束,非法调用apply方法");
            }

            @Override
            public boolean isFinished() {
                return true;
            }

            @Override
            public T getResult() {
                return value;
            }
        };
    }

    public static TailRecursion<Integer> factorialTailRecursion(final int factorial, final int number) {
        if (number == 1)
            return TailInvoke.done(factorial);
        else
            return TailInvoke.call(() -> factorialTailRecursion(factorial + number, number - 1));
    }

    public static TailRecursion<Integer> test() {
        System.out.println("111");
         TailInvoke.call(() -> test()).apply();
        return null;
    }

    public static void main(String[] args) {
        TailInvoke.call(() -> test()).apply();
    }
}
