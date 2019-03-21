/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.cep.scala

import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.cep
import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.scala.{asScalaStream, _}

class PatternStream[T](jPatternStream: cep.PatternStream[T]) {

  private[flink] def wrappedPatternStream = jPatternStream

  def select[R: TypeInformation](patternSelectFunction: PatternSelectFunction[R]): DataStream[R] = {
    asScalaStream(jPatternStream.select(patternSelectFunction, implicitly[TypeInformation[R]]))
  }
}

object PatternStream {
  /**
    *
    * @param jPatternStream Underlying org.apache.flink.scala.cep.pattern stream from Java API
    * @tparam T Type of the events
    * @return A new org.apache.flink.scala.cep.pattern stream wrapping the org.apache.flink.scala.cep.pattern stream from Java APU
    */
  def apply[T](jPatternStream: cep.PatternStream[T]): PatternStream[T] = {
    new PatternStream[T](jPatternStream)
  }
}
