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
package org.apache.flink.cep.scala.pattern

import org.apache.flink.cep.pattern.conditions.IterativeCondition
import org.apache.flink.cep.pattern.{GroupPattern => JGroupPattern}

class GroupPattern(jGroupPattern: JGroupPattern)
  extends Pattern(jGroupPattern) {

  override def where(condition: IterativeCondition ) =
    throw new UnsupportedOperationException ("GroupPattern does not support where clause.")

  override def or(condition: IterativeCondition ) =
    throw new UnsupportedOperationException ("GroupPattern does not support or clause.")

}

object GroupPattern {

  /**
    * Constructs a new GroupPattern by wrapping a given Java API GroupPattern
    *
    * @param jGroupPattern Underlying Java API GroupPattern.
    * @tparam T Base type of the elements appearing in the org.apache.flink.scala.cep.pattern
    * @tparam F Subtype of T to which the current org.apache.flink.scala.cep.pattern operator is constrained
    * @return New wrapping GroupPattern object
    */
  def apply(jGroupPattern: JGroupPattern) = new GroupPattern(jGroupPattern)

}
