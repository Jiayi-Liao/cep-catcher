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

import org.apache.flink.cep.event.EventWrapper
import org.apache.flink.cep.pattern.{Pattern => JPattern}
import org.apache.flink.cep.pattern.conditions.IterativeCondition.{Context => JContext}
import org.apache.flink.cep.scala.conditions.Context

import scala.collection.JavaConverters._

package object pattern {

  private[flink] def wrapPattern(javaPattern: JPattern)
  : Option[Pattern] = javaPattern match {
    case p: JPattern => Some(Pattern(p))
    case _ => None
  }

  private[pattern] class JContextWrapper(private val jContext: JContext)
    extends Context with Serializable {

    override def getEventsForPattern(name: String): Iterable[EventWrapper[_]] =
      jContext.getEventsForPattern(name).asScala
  }
}

