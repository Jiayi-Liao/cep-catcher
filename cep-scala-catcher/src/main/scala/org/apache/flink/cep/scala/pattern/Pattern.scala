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

import org.apache.flink.cep
import org.apache.flink.cep.event.EventWrapper
import org.apache.flink.cep.nfa.NFA
import org.apache.flink.cep.nfa.compiler.NFACompiler
import org.apache.flink.cep.pattern.conditions.IterativeCondition.{Context => JContext}
import org.apache.flink.cep.pattern.conditions.{IterativeCondition, SimpleCondition}
import org.apache.flink.cep.pattern.{MalformedPatternException, Quantifier, Pattern => JPattern}
import org.apache.flink.cep.scala.conditions.Context
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * Base class for a org.apache.flink.scala.cep.pattern definition.
  *
  * A org.apache.flink.scala.cep.pattern definition is used by [[NFACompiler]] to create
  * a [[NFA]].
  *
  * {{{
  * Pattern<T, F> org.apache.flink.scala.cep.pattern = Pattern.<T>begin("start")
  *   .next("middle").subtype(F.class)
  *   .followedBy("end").where(new MyCondition());
  * }}}
  *
  * @param jPattern Underlying Java API Pattern
  * @tparam T Base type of the elements appearing in the org.apache.flink.scala.cep.pattern
  * @tparam F Subtype of T to which the current org.apache.flink.scala.cep.pattern operator is constrained
  */
class Pattern(jPattern: JPattern) {

  private[flink] def wrappedPattern = jPattern

  def setId(id: String): Unit = {
    jPattern.setId(id)
  }

  def setTimeoutMinutes(timeoutMinutes: Long): Unit = {
    jPattern.setTimeoutMinutes(timeoutMinutes)
  }

  /**
    * @return The previous org.apache.flink.scala.cep.pattern
    */
  def getPrevious: Option[Pattern] = {
    wrapPattern(jPattern.getPrevious)
  }

  /**
    *
    * @return Name of the org.apache.flink.scala.cep.pattern operator
    */
  def getName: String = jPattern.getName

  /**
    *
    * @return Window length in which the org.apache.flink.scala.cep.pattern match has to occur
    */
  def getWindowTime: Option[Time] = {
    Option(jPattern.getWindowTime)
  }

  /**
    *
    * @return currently applied quantifier to this org.apache.flink.scala.cep.pattern
    */
  def getQuantifier: Quantifier = jPattern.getQuantifier

  def getCondition: Option[IterativeCondition] = {
    Option(jPattern.getCondition)
  }

  def getUntilCondition: Option[IterativeCondition] = {
    Option(jPattern.getUntilCondition)
  }

  /**
    * Adds a condition that has to be satisfied by an event
    * in order to be considered a match. If another condition has already been
    * set, the new one is going to be combined with the previous with a
    * logical {{{AND}}}. In other case, this is going to be the only
    * condition.
    *
    * @param condition The condition as an [[IterativeCondition]].
    * @return The org.apache.flink.scala.cep.pattern with the new condition is set.
    */
  def where(condition: IterativeCondition): Pattern = {
    jPattern.where(condition)
    this
  }

  /**
    * Adds a condition that has to be satisfied by an event
    * in order to be considered a match. If another condition has already been
    * set, the new one is going to be combined with the previous with a
    * logical {{{AND}}}. In other case, this is going to be the only
    * condition.
    *
    * @param condition The condition to be set.
    * @return The org.apache.flink.scala.cep.pattern with the new condition is set.
    */
  def where(condition: (EventWrapper[_], Context) => Boolean): Pattern = {
    val condFun = new IterativeCondition {
      val cleanCond = cep.scala.cleanClosure(condition)

      override def filter(value: EventWrapper[_], ctx: JContext): Boolean = {
        cleanCond(value, new JContextWrapper(ctx))
      }
    }
    where(condFun)
  }

  /**
    * Adds a condition that has to be satisfied by an event
    * in order to be considered a match. If another condition has already been
    * set, the new one is going to be combined with the previous with a
    * logical {{{AND}}}. In other case, this is going to be the only
    * condition.
    *
    * @param condition The condition to be set.
    * @return The org.apache.flink.scala.cep.pattern with the new condition is set.
    */
  def where(condition: EventWrapper[_] => Boolean): Pattern = {
    val condFun = new IterativeCondition {
      val cleanCond = cep.scala.cleanClosure(condition)

      override def filter(value: EventWrapper[_], ctx: JContext): Boolean = cleanCond(value)
    }
    where(condFun)
  }

  /**
    * Adds a condition that has to be satisfied by an event
    * in order to be considered a match. If another condition has already been
    * set, the new one is going to be combined with the previous with a
    * logical {{{OR}}}. In other case, this is going to be the only
    * condition.
    *
    * @param condition The condition as an [[IterativeCondition]].
    * @return The org.apache.flink.scala.cep.pattern with the new condition is set.
    */
  def or(condition: IterativeCondition): Pattern = {
    jPattern.or(condition)
    this
  }

  /**
    * Adds a condition that has to be satisfied by an event
    * in order to be considered a match. If another condition has already been
    * set, the new one is going to be combined with the previous with a
    * logical {{{OR}}}. In other case, this is going to be the only
    * condition.
    *
    * @param condition The {{{OR}}} condition.
    * @return The org.apache.flink.scala.cep.pattern with the new condition is set.
    */
  def or(condition: EventWrapper[_] => Boolean): Pattern = {
    val condFun = new SimpleCondition {
      val cleanCond = cep.scala.cleanClosure(condition)

      override def filter(value: EventWrapper[_]): Boolean =
        cleanCond(value)
    }
    or(condFun)
  }

  /**
    * Adds a condition that has to be satisfied by an event
    * in order to be considered a match. If another condition has already been
    * set, the new one is going to be combined with the previous with a
    * logical {{{OR}}}. In other case, this is going to be the only
    * condition.
    *
    * @param condition The {{{OR}}} condition.
    * @return The org.apache.flink.scala.cep.pattern with the new condition is set.
    */
  def or(condition: (EventWrapper[_], Context) => Boolean): Pattern = {
    val condFun = new IterativeCondition {
      val cleanCond = cep.scala.cleanClosure(condition)

      override def filter(value: EventWrapper[_], ctx: JContext): Boolean =
        cleanCond(value, new JContextWrapper(ctx))
    }
    or(condFun)
  }

  /**
    * Applies a stop condition for a looping state. It allows cleaning the underlying state.
    *
    * @param untilCondition a condition an event has to satisfy to stop collecting events into
    *                       looping state
    * @return The same org.apache.flink.scala.cep.pattern with applied untilCondition
    */
  def until(untilCondition: IterativeCondition): Pattern = {
    jPattern.until(untilCondition)
    this
  }

  /**
    * Applies a stop condition for a looping state. It allows cleaning the underlying state.
    *
    * @param untilCondition a condition an event has to satisfy to stop collecting events into
    *                       looping state
    * @return The same org.apache.flink.scala.cep.pattern with applied untilCondition
    */
  def until(untilCondition: (EventWrapper[_], Context) => Boolean): Pattern = {
    val condFun = new IterativeCondition {
      val cleanCond = cep.scala.cleanClosure(untilCondition)

      override def filter(value: EventWrapper[_], ctx: JContext): Boolean =
        cleanCond(value, new JContextWrapper(ctx))
    }
    until(condFun)
  }

  /**
    * Applies a stop condition for a looping state. It allows cleaning the underlying state.
    *
    * @param untilCondition a condition an event has to satisfy to stop collecting events into
    *                       looping state
    * @return The same org.apache.flink.scala.cep.pattern with applied untilCondition
    */
  def until(untilCondition: EventWrapper[_] => Boolean): Pattern = {
    val condFun = new IterativeCondition {
      val cleanCond = cep.scala.cleanClosure(untilCondition)

      override def filter(value: EventWrapper[_], ctx: JContext): Boolean = cleanCond(value)
    }
    until(condFun)
  }

  /**
    * Defines the maximum time interval in which a matching org.apache.flink.scala.cep.pattern has to be completed in
    * order to be considered valid. This interval corresponds to the maximum time gap between first
    * and the last event.
    *
    * @param windowTime Time of the matching window
    * @return The same org.apache.flink.scala.cep.pattern operator with the new window length
    */
  def within(windowTime: Time): Pattern = {
    jPattern.within(windowTime)
    this
  }

  /**
    * Appends a new org.apache.flink.scala.cep.pattern to the existing one. The new org.apache.flink.scala.cep.pattern enforces strict
    * temporal contiguity. This means that the whole org.apache.flink.scala.cep.pattern sequence matches only
    * if an event which matches this org.apache.flink.scala.cep.pattern directly follows the preceding matching
    * event. Thus, there cannot be any events in between two matching events.
    *
    * @param name Name of the new org.apache.flink.scala.cep.pattern
    * @return A new org.apache.flink.scala.cep.pattern which is appended to this one
    */
  def next(name: String): Pattern = {
    Pattern(jPattern.next(name))
  }

  /**
    * Appends a new org.apache.flink.scala.cep.pattern to the existing one. The new org.apache.flink.scala.cep.pattern enforces that there is no event
    * matching this org.apache.flink.scala.cep.pattern right after the preceding matched event.
    *
    * @param name Name of the new org.apache.flink.scala.cep.pattern
    * @return A new org.apache.flink.scala.cep.pattern which is appended to this one
    */
  def notNext(name: String): Pattern = {
    Pattern(jPattern.notNext(name))
  }

  /**
    * Appends a new org.apache.flink.scala.cep.pattern to the existing one. The new org.apache.flink.scala.cep.pattern enforces non-strict
    * temporal contiguity. This means that a matching event of this org.apache.flink.scala.cep.pattern and the
    * preceding matching event might be interleaved with other events which are ignored.
    *
    * @param name Name of the new org.apache.flink.scala.cep.pattern
    * @return A new org.apache.flink.scala.cep.pattern which is appended to this one
    */
  def followedBy(name: String): Pattern = {
    Pattern(jPattern.followedBy(name))
  }

  /**
    * Appends a new org.apache.flink.scala.cep.pattern to the existing one. The new org.apache.flink.scala.cep.pattern enforces that there is no event
    * matching this org.apache.flink.scala.cep.pattern between the preceding org.apache.flink.scala.cep.pattern and succeeding this one.
    *
    * NOTE: There has to be other org.apache.flink.scala.cep.pattern after this one.
    *
    * @param name Name of the new org.apache.flink.scala.cep.pattern
    * @return A new org.apache.flink.scala.cep.pattern which is appended to this one
    */
  def notFollowedBy(name : String): Pattern = {
    Pattern(jPattern.notFollowedBy(name))
  }

  /**
    * Appends a new org.apache.flink.scala.cep.pattern to the existing one. The new org.apache.flink.scala.cep.pattern enforces non-strict
    * temporal contiguity. This means that a matching event of this org.apache.flink.scala.cep.pattern and the
    * preceding matching event might be interleaved with other events which are ignored.
    *
    * @param name Name of the new org.apache.flink.scala.cep.pattern
    * @return A new org.apache.flink.scala.cep.pattern which is appended to this one
    */
  def followedByAny(name: String): Pattern = {
    Pattern(jPattern.followedByAny(name))
  }


  /**
    * Specifies that this org.apache.flink.scala.cep.pattern is optional for a final match of the org.apache.flink.scala.cep.pattern
    * sequence to happen.
    *
    * @return The same org.apache.flink.scala.cep.pattern as optional.
    * @throws MalformedPatternException if the quantifier is not applicable to this org.apache.flink.scala.cep.pattern.
    */
  def optional: Pattern = {
    jPattern.optional()
    this
  }

  /**
    * Specifies that this org.apache.flink.scala.cep.pattern can occur {{{one or more}}} times.
    * This means at least one and at most infinite number of events can
    * be matched to this org.apache.flink.scala.cep.pattern.
    *
    * If this quantifier is enabled for a
    * org.apache.flink.scala.cep.pattern {{{A.oneOrMore().followedBy(B)}}} and a sequence of events
    * {{{A1 A2 B}}} appears, this will generate patterns:
    * {{{A1 B}}} and {{{A1 A2 B}}}. See also {{{allowCombinations()}}}.
    *
    * @return The same org.apache.flink.scala.cep.pattern with a [[Quantifier.looping()]] quantifier applied.
    * @throws MalformedPatternException if the quantifier is not applicable to this org.apache.flink.scala.cep.pattern.
    */
  def oneOrMore: Pattern = {
    jPattern.oneOrMore()
    this
  }

  /**
    * Specifies that this org.apache.flink.scala.cep.pattern is greedy.
    * This means as many events as possible will be matched to this org.apache.flink.scala.cep.pattern.
    *
    * @return The same org.apache.flink.scala.cep.pattern with { @link Quantifier#greedy} set to true.
    * @throws MalformedPatternException if the quantifier is not applicable to this org.apache.flink.scala.cep.pattern.
    */
  def greedy: Pattern = {
    jPattern.greedy()
    this
  }

  /**
    * Specifies exact number of times that this org.apache.flink.scala.cep.pattern should be matched.
    *
    * @param times number of times matching event must appear
    * @return The same org.apache.flink.scala.cep.pattern with number of times applied
    * @throws MalformedPatternException if the quantifier is not applicable to this org.apache.flink.scala.cep.pattern.
    */
  def times(times: Int): Pattern = {
    jPattern.times(times)
    this
  }

  /**
    * Specifies that the org.apache.flink.scala.cep.pattern can occur between from and to times.
    *
    * @param from number of times matching event must appear at least
    * @param to   number of times matching event must appear at most
    * @return The same org.apache.flink.scala.cep.pattern with the number of times range applied
    * @throws MalformedPatternException if the quantifier is not applicable to this org.apache.flink.scala.cep.pattern.
    */
  def times(from: Int, to: Int): Pattern = {
    jPattern.times(from, to)
    this
  }

  /**
    * Specifies that this org.apache.flink.scala.cep.pattern can occur the specified times at least.
    * This means at least the specified times and at most infinite number of events can
    * be matched to this org.apache.flink.scala.cep.pattern.
    *
    * @return The same org.apache.flink.scala.cep.pattern with a { @link Quantifier#looping(ConsumingStrategy)} quantifier
    *         applied.
    * @throws MalformedPatternException if the quantifier is not applicable to this org.apache.flink.scala.cep.pattern.
    */
  def timesOrMore(times: Int): Pattern = {
    jPattern.timesOrMore(times)
    this
  }

  /**
    * Applicable only to [[Quantifier.looping()]] and [[Quantifier.times()]] patterns,
    * this option allows more flexibility to the matching events.
    *
    * If {{{allowCombinations()}}} is not applied for a
    * org.apache.flink.scala.cep.pattern {{{A.oneOrMore().followedBy(B)}}} and a sequence of events
    * {{{A1 A2 B}}} appears, this will generate patterns:
    * {{{A1 B}}} and {{{A1 A2 B}}}. If this method is applied, we
    * will have {{{A1 B}}}, {{{A2 B}}} and {{{A1 A2 B}}}.
    *
    * @return The same org.apache.flink.scala.cep.pattern with the updated quantifier.
    * @throws MalformedPatternException if the quantifier is not applicable to this org.apache.flink.scala.cep.pattern.
    */
  def allowCombinations(): Pattern = {
    jPattern.allowCombinations()
    this
  }

  /**
    * Works in conjunction with [[Pattern#oneOrMore()]] or [[Pattern#times(int)]].
    * Specifies that any not matching element breaks the loop.
    *
    * E.g. a org.apache.flink.scala.cep.pattern like:
    * {{{
    * Pattern.begin("start").where(_.getName().equals("c"))
    *        .followedBy("middle").where(_.getName().equals("a")).oneOrMore().consecutive()
    *        .followedBy("end1").where(_.getName().equals("b"));
    * }}}
    *
    * For a sequence: C D A1 A2 A3 D A4 B
    *
    * will generate matches: {C A1 B}, {C A1 A2 B}, {C A1 A2 A3 B}
    *
    * By default a relaxed continuity is applied.
    * @return org.apache.flink.scala.cep.pattern with continuity changed to strict
    */
  def consecutive(): Pattern = {
    jPattern.consecutive()
    this
  }

  /**
    * Appends a new group org.apache.flink.scala.cep.pattern to the existing one. The new org.apache.flink.scala.cep.pattern enforces non-strict
    * temporal contiguity. This means that a matching event of this org.apache.flink.scala.cep.pattern and the
    * preceding matching event might be interleaved with other events which are ignored.
    *
    * @param pattern the org.apache.flink.scala.cep.pattern to append
    * @return A new org.apache.flink.scala.cep.pattern which is appended to this one
    */
  def followedBy(pattern: Pattern): GroupPattern =
    GroupPattern(jPattern.followedBy(pattern.wrappedPattern))

  /**
    * Appends a new group org.apache.flink.scala.cep.pattern to the existing one. The new org.apache.flink.scala.cep.pattern enforces non-strict
    * temporal contiguity. This means that a matching event of this org.apache.flink.scala.cep.pattern and the
    * preceding matching event might be interleaved with other events which are ignored.
    *
    * @param pattern the org.apache.flink.scala.cep.pattern to append
    * @return A new org.apache.flink.scala.cep.pattern which is appended to this one
    */
  def followedByAny(pattern: Pattern): GroupPattern =
    GroupPattern(jPattern.followedByAny(pattern.wrappedPattern))

  /**
    * Appends a new group org.apache.flink.scala.cep.pattern to the existing one. The new org.apache.flink.scala.cep.pattern enforces strict
    * temporal contiguity. This means that the whole org.apache.flink.scala.cep.pattern sequence matches only
    * if an event which matches this org.apache.flink.scala.cep.pattern directly follows the preceding matching
    * event. Thus, there cannot be any events in between two matching events.
    *
    * @param pattern the org.apache.flink.scala.cep.pattern to append
    * @return A new org.apache.flink.scala.cep.pattern which is appended to this one
    */
  def next(pattern: Pattern): GroupPattern =
    GroupPattern(jPattern.next(pattern.wrappedPattern))

}

object Pattern {

  def apply(jPattern: JPattern) = new Pattern(jPattern)

  def begin[X](name: String): Pattern = Pattern(JPattern.begin(name))

  def begin(pattern: Pattern): GroupPattern =
    GroupPattern(JPattern.begin(pattern.wrappedPattern))
}
