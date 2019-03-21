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

package org.apache.flink.cep.pattern;

import org.apache.flink.cep.nfa.compiler.NFACompiler;
import org.apache.flink.cep.pattern.conditions.AndCondition;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.pattern.conditions.OrCondition;
import org.apache.flink.api.java.ClosureCleaner;
import org.apache.flink.cep.nfa.NFA;
import org.apache.flink.cep.pattern.Quantifier.ConsumingStrategy;
import org.apache.flink.cep.pattern.Quantifier.Times;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Preconditions;

/**
 * Base class for a org.apache.flink.scala.cep.pattern definition.
 *
 * <p>A org.apache.flink.scala.cep.pattern definition is used by {@link NFACompiler} to create a {@link NFA}.
 *
 * <p><pre>{@code
 * Pattern<T, F> org.apache.flink.scala.cep.pattern = Pattern.begin("start")
 *   .next("middle").subtype(F.class)
 *   .followedBy("end").where(new MyCondition());
 * }
 * </pre>
 */
public class Pattern {

	private String id = null;

	private long timeoutMinutes = 0L;

	/** Name of the org.apache.flink.scala.cep.pattern. */
	private final String name;

	/** Previous org.apache.flink.scala.cep.pattern. */
	private final Pattern previous;

	/** The condition an event has to satisfy to be considered a matched. */
	private IterativeCondition condition;

	/** Window length in which the org.apache.flink.scala.cep.pattern match has to occur. */
	private Time windowTime;

	/** A quantifier for the org.apache.flink.scala.cep.pattern. By default set to {@link Quantifier#one(ConsumingStrategy)}. */
	private Quantifier quantifier = Quantifier.one(ConsumingStrategy.STRICT);

	/** The condition an event has to satisfy to stop collecting events into looping state. */
	private IterativeCondition untilCondition;

	/**
	 * Applicable to a {@code times} org.apache.flink.scala.cep.pattern, and holds
	 * the number of times it has to appear.
	 */
	private Times times;

	protected Pattern(
		final String name,
		final Pattern previous,
		final ConsumingStrategy consumingStrategy) {
		this.name = name;
		this.previous = previous;
		this.quantifier = Quantifier.one(consumingStrategy);
	}

	public Pattern getPrevious() {
		return previous;
	}

	public Times getTimes() {
		return times;
	}

	public String getName() {
		return name;
	}

	public Time getWindowTime() {
		return windowTime;
	}

	public Quantifier getQuantifier() {
		return quantifier;
	}

	public IterativeCondition getCondition() {
		return condition;
	}

	public IterativeCondition getUntilCondition() {
		return untilCondition;
	}

	public void setId(String id) {
		this.id = id;
	}

	public void setTimeoutMinutes(long timeoutMinutes) {
		this.timeoutMinutes = timeoutMinutes;
	}

	public String getId() {
		return this.id;
	}

	public long getTimeoutMinutes() {
		return this.timeoutMinutes;
	}

	/**
	 * Starts a new org.apache.flink.scala.cep.pattern sequence. The provided name is the one of the initial org.apache.flink.scala.cep.pattern
	 * of the new sequence. Furthermore, the base type of the event sequence is set.
	 *
	 * @param name The name of starting org.apache.flink.scala.cep.pattern of the new org.apache.flink.scala.cep.pattern sequence
	 * @return The first org.apache.flink.scala.cep.pattern of a org.apache.flink.scala.cep.pattern sequence
	 */
	public static Pattern begin(final String name) {
		return new Pattern(name, null, ConsumingStrategy.STRICT);
	}

	/**
	 * Adds a condition that has to be satisfied by an event
	 * in order to be considered a match. If another condition has already been
	 * set, the new one is going to be combined with the previous with a
	 * logical {@code AND}. In other case, this is going to be the only
	 * condition.
	 *
	 * @param condition The condition as an {@link IterativeCondition}.
	 * @return The org.apache.flink.scala.cep.pattern with the new condition is set.
	 */
	public Pattern where(IterativeCondition condition) {
		Preconditions.checkNotNull(condition, "The condition cannot be null.");

		ClosureCleaner.clean(condition, true);
		if (this.condition == null) {
			this.condition = condition;
		} else {
			this.condition = new AndCondition(this.condition, condition);
		}
		return this;
	}

	/**
	 * Adds a condition that has to be satisfied by an event
	 * in order to be considered a match. If another condition has already been
	 * set, the new one is going to be combined with the previous with a
	 * logical {@code OR}. In other case, this is going to be the only
	 * condition.
	 *
	 * @param condition The condition as an {@link IterativeCondition}.
	 * @return The org.apache.flink.scala.cep.pattern with the new condition is set.
	 */
	public Pattern or(IterativeCondition condition) {
		Preconditions.checkNotNull(condition, "The condition cannot be null.");

		ClosureCleaner.clean(condition, true);

		if (this.condition == null) {
			this.condition = condition;
		} else {
			this.condition = new OrCondition(this.condition, condition);
		}
		return this;
	}

	/**
	 * Applies a stop condition for a looping state. It allows cleaning the underlying state.
	 *
	 * @param untilCondition a condition an event has to satisfy to stop collecting events into looping state
	 * @return The same org.apache.flink.scala.cep.pattern with applied untilCondition
	 */
	public Pattern until(IterativeCondition untilCondition) {
		Preconditions.checkNotNull(untilCondition, "The condition cannot be null");

		if (this.untilCondition != null) {
			throw new MalformedPatternException("Only one until condition can be applied.");
		}

		if (!quantifier.hasProperty(Quantifier.QuantifierProperty.LOOPING)) {
			throw new MalformedPatternException("The until condition is only applicable to looping states.");
		}

		ClosureCleaner.clean(untilCondition, true);
		this.untilCondition = untilCondition;

		return this;
	}

	/**
	 * Defines the maximum time interval in which a matching org.apache.flink.scala.cep.pattern has to be completed in
	 * order to be considered valid. This interval corresponds to the maximum time gap between first
	 * and the last event.
	 *
	 * @param windowTime Time of the matching window
	 * @return The same org.apache.flink.scala.cep.pattern operator with the new window length
	 */
	public Pattern within(Time windowTime) {
		if (windowTime != null) {
			this.windowTime = windowTime;
		}

		return this;
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
	public Pattern next(final String name) {
		return new Pattern(name, this, ConsumingStrategy.STRICT);
	}

	/**
	 * Appends a new org.apache.flink.scala.cep.pattern to the existing one. The new org.apache.flink.scala.cep.pattern enforces that there is no event matching this org.apache.flink.scala.cep.pattern
	 * right after the preceding matched event.
	 *
	 * @param name Name of the new org.apache.flink.scala.cep.pattern
	 * @return A new org.apache.flink.scala.cep.pattern which is appended to this one
	 */
	public Pattern notNext(final String name) {
		if (quantifier.hasProperty(Quantifier.QuantifierProperty.OPTIONAL)) {
			throw new UnsupportedOperationException(
					"Specifying a org.apache.flink.scala.cep.pattern with an optional path to NOT condition is not supported yet. " +
					"You can simulate such org.apache.flink.scala.cep.pattern with two independent patterns, one with and the other without " +
					"the optional part.");
		}
		return new Pattern(name, this, ConsumingStrategy.NOT_NEXT);
	}

	/**
	 * Appends a new org.apache.flink.scala.cep.pattern to the existing one. The new org.apache.flink.scala.cep.pattern enforces non-strict
	 * temporal contiguity. This means that a matching event of this org.apache.flink.scala.cep.pattern and the
	 * preceding matching event might be interleaved with other events which are ignored.
	 *
	 * @param name Name of the new org.apache.flink.scala.cep.pattern
	 * @return A new org.apache.flink.scala.cep.pattern which is appended to this one
	 */
	public Pattern followedBy(final String name) {
		return new Pattern(name, this, ConsumingStrategy.SKIP_TILL_NEXT);
	}

	/**
	 * Appends a new org.apache.flink.scala.cep.pattern to the existing one. The new org.apache.flink.scala.cep.pattern enforces that there is no event matching this org.apache.flink.scala.cep.pattern
	 * between the preceding org.apache.flink.scala.cep.pattern and succeeding this one.
	 *
	 * <p><b>NOTE:</b> There has to be other org.apache.flink.scala.cep.pattern after this one.
	 *
	 * @param name Name of the new org.apache.flink.scala.cep.pattern
	 * @return A new org.apache.flink.scala.cep.pattern which is appended to this one
	 */
	public Pattern notFollowedBy(final String name) {
		if (quantifier.hasProperty(Quantifier.QuantifierProperty.OPTIONAL)) {
			throw new UnsupportedOperationException(
					"Specifying a org.apache.flink.scala.cep.pattern with an optional path to NOT condition is not supported yet. " +
					"You can simulate such org.apache.flink.scala.cep.pattern with two independent patterns, one with and the other without " +
					"the optional part.");
		}
		return new Pattern(name, this, ConsumingStrategy.NOT_FOLLOW);
	}

	/**
	 * Appends a new org.apache.flink.scala.cep.pattern to the existing one. The new org.apache.flink.scala.cep.pattern enforces non-strict
	 * temporal contiguity. This means that a matching event of this org.apache.flink.scala.cep.pattern and the
	 * preceding matching event might be interleaved with other events which are ignored.
	 *
	 * @param name Name of the new org.apache.flink.scala.cep.pattern
	 * @return A new org.apache.flink.scala.cep.pattern which is appended to this one
	 */
	public Pattern followedByAny(final String name) {
		return new Pattern(name, this, ConsumingStrategy.SKIP_TILL_ANY);
	}

	/**
	 * Specifies that this org.apache.flink.scala.cep.pattern is optional for a final match of the org.apache.flink.scala.cep.pattern
	 * sequence to happen.
	 *
	 * @return The same org.apache.flink.scala.cep.pattern as optional.
	 * @throws MalformedPatternException if the quantifier is not applicable to this org.apache.flink.scala.cep.pattern.
	 */
	public Pattern optional() {
		checkIfPreviousPatternGreedy();
		quantifier.optional();
		return this;
	}

	/**
	 * Specifies that this org.apache.flink.scala.cep.pattern can occur {@code one or more} times.
	 * This means at least one and at most infinite number of events can
	 * be matched to this org.apache.flink.scala.cep.pattern.
	 *
	 * <p>If this quantifier is enabled for a
	 * org.apache.flink.scala.cep.pattern {@code A.oneOrMore().followedBy(B)} and a sequence of events
	 * {@code A1 A2 B} appears, this will generate patterns:
	 * {@code A1 B} and {@code A1 A2 B}. See also {@link #allowCombinations()}.
	 *
	 * @return The same org.apache.flink.scala.cep.pattern with a {@link Quantifier#looping(ConsumingStrategy)} quantifier applied.
	 * @throws MalformedPatternException if the quantifier is not applicable to this org.apache.flink.scala.cep.pattern.
	 */
	public Pattern oneOrMore() {
		checkIfNoNotPattern();
		checkIfQuantifierApplied();
		this.quantifier = Quantifier.looping(quantifier.getConsumingStrategy());
		this.times = Times.of(1);
		return this;
	}

	/**
	 * Specifies that this org.apache.flink.scala.cep.pattern is greedy.
	 * This means as many events as possible will be matched to this org.apache.flink.scala.cep.pattern.
	 *
	 * @return The same org.apache.flink.scala.cep.pattern with {@link Quantifier#greedy} set to true.
	 * @throws MalformedPatternException if the quantifier is not applicable to this org.apache.flink.scala.cep.pattern.
	 */
	public Pattern greedy() {
		checkIfNoNotPattern();
		checkIfNoGroupPattern();
		this.quantifier.greedy();
		return this;
	}

	/**
	 * Specifies exact number of times that this org.apache.flink.scala.cep.pattern should be matched.
	 *
	 * @param times number of times matching event must appear
	 * @return The same org.apache.flink.scala.cep.pattern with number of times applied
	 *
	 * @throws MalformedPatternException if the quantifier is not applicable to this org.apache.flink.scala.cep.pattern.
	 */
	public Pattern times(int times) {
		checkIfNoNotPattern();
		checkIfQuantifierApplied();
		Preconditions.checkArgument(times > 0, "You should give a positive number greater than 0.");
		this.quantifier = Quantifier.times(quantifier.getConsumingStrategy());
		this.times = Times.of(times);
		return this;
	}

	/**
	 * Specifies that the org.apache.flink.scala.cep.pattern can occur between from and to times.
	 *
	 * @param from number of times matching event must appear at least
	 * @param to number of times matching event must appear at most
	 * @return The same org.apache.flink.scala.cep.pattern with the number of times range applied
	 *
	 * @throws MalformedPatternException if the quantifier is not applicable to this org.apache.flink.scala.cep.pattern.
	 */
	public Pattern times(int from, int to) {
		checkIfNoNotPattern();
		checkIfQuantifierApplied();
		this.quantifier = Quantifier.times(quantifier.getConsumingStrategy());
		if (from == 0) {
			this.quantifier.optional();
			from = 1;
		}
		this.times = Times.of(from, to);
		return this;
	}

	/**
	 * Specifies that this org.apache.flink.scala.cep.pattern can occur the specified times at least.
	 * This means at least the specified times and at most infinite number of events can
	 * be matched to this org.apache.flink.scala.cep.pattern.
	 *
	 * @return The same org.apache.flink.scala.cep.pattern with a {@link Quantifier#looping(ConsumingStrategy)} quantifier applied.
	 * @throws MalformedPatternException if the quantifier is not applicable to this org.apache.flink.scala.cep.pattern.
	 */
	public Pattern timesOrMore(int times) {
		checkIfNoNotPattern();
		checkIfQuantifierApplied();
		this.quantifier = Quantifier.looping(quantifier.getConsumingStrategy());
		this.times = Times.of(times);
		return this;
	}

	/**
	 * Applicable only to {@link Quantifier#looping(ConsumingStrategy)} and
	 * {@link Quantifier#times(ConsumingStrategy)} patterns, this option allows more flexibility to the matching events.
	 *
	 * <p>If {@code allowCombinations()} is not applied for a
	 * org.apache.flink.scala.cep.pattern {@code A.oneOrMore().followedBy(B)} and a sequence of events
	 * {@code A1 A2 B} appears, this will generate patterns:
	 * {@code A1 B} and {@code A1 A2 B}. If this method is applied, we
	 * will have {@code A1 B}, {@code A2 B} and {@code A1 A2 B}.
	 *
	 * @return The same org.apache.flink.scala.cep.pattern with the updated quantifier.	 *
	 * @throws MalformedPatternException if the quantifier is not applicable to this org.apache.flink.scala.cep.pattern.
	 */
	public Pattern allowCombinations() {
		quantifier.combinations();
		return this;
	}

	/**
	 * Works in conjunction with {@link Pattern#oneOrMore()} or {@link Pattern#times(int)}.
	 * Specifies that any not matching element breaks the loop.
	 *
	 * <p>E.g. a org.apache.flink.scala.cep.pattern like:
	 * <pre>{@code
	 * Pattern.<Event>begin("start").where(new SimpleCondition<Event>() {
	 *      @Override
	 *      public boolean filter(Event value) throws Exception {
	 *          return value.getName().equals("c");
	 *      }
	 * })
	 * .followedBy("middle").where(new SimpleCondition<Event>() {
	 *      @Override
	 *      public boolean filter(Event value) throws Exception {
	 *          return value.getName().equals("a");
	 *      }
	 * }).oneOrMore().consecutive()
	 * .followedBy("end1").where(new SimpleCondition<Event>() {
	 *      @Override
	 *      public boolean filter(Event value) throws Exception {
	 *          return value.getName().equals("b");
	 *      }
	 * });
	 * }</pre>
	 *
	 * <p>for a sequence: C D A1 A2 A3 D A4 B
	 *
	 * <p>will generate matches: {C A1 B}, {C A1 A2 B}, {C A1 A2 A3 B}
	 *
	 * <p>By default a relaxed continuity is applied.
	 *
	 * @return org.apache.flink.scala.cep.pattern with continuity changed to strict
	 */
	public Pattern consecutive() {
		quantifier.consecutive();
		return this;
	}

	/**
	 * Starts a new org.apache.flink.scala.cep.pattern sequence. The provided org.apache.flink.scala.cep.pattern is the initial org.apache.flink.scala.cep.pattern
	 * of the new sequence.
	 *
	 * @param group the org.apache.flink.scala.cep.pattern to begin with
	 * @return the first org.apache.flink.scala.cep.pattern of a org.apache.flink.scala.cep.pattern sequence
	 */
	public static  GroupPattern begin(Pattern group) {
		return new GroupPattern(null, group, ConsumingStrategy.STRICT);
	}

	/**
	 * Appends a new group org.apache.flink.scala.cep.pattern to the existing one. The new org.apache.flink.scala.cep.pattern enforces non-strict
	 * temporal contiguity. This means that a matching event of this org.apache.flink.scala.cep.pattern and the
	 * preceding matching event might be interleaved with other events which are ignored.
	 *
	 * @param group the org.apache.flink.scala.cep.pattern to append
	 * @return A new org.apache.flink.scala.cep.pattern which is appended to this one
	 */
	public GroupPattern followedBy(Pattern group) {
		return new GroupPattern(this, group, ConsumingStrategy.SKIP_TILL_NEXT);
	}

	/**
	 * Appends a new group org.apache.flink.scala.cep.pattern to the existing one. The new org.apache.flink.scala.cep.pattern enforces non-strict
	 * temporal contiguity. This means that a matching event of this org.apache.flink.scala.cep.pattern and the
	 * preceding matching event might be interleaved with other events which are ignored.
	 *
	 * @param group the org.apache.flink.scala.cep.pattern to append
	 * @return A new org.apache.flink.scala.cep.pattern which is appended to this one
	 */
	public GroupPattern followedByAny(Pattern group) {
		return new GroupPattern(this, group, ConsumingStrategy.SKIP_TILL_ANY);
	}

	/**
	 * Appends a new group org.apache.flink.scala.cep.pattern to the existing one. The new org.apache.flink.scala.cep.pattern enforces strict
	 * temporal contiguity. This means that the whole org.apache.flink.scala.cep.pattern sequence matches only
	 * if an event which matches this org.apache.flink.scala.cep.pattern directly follows the preceding matching
	 * event. Thus, there cannot be any events in between two matching events.
	 *
	 * @param group the org.apache.flink.scala.cep.pattern to append
	 * @return A new org.apache.flink.scala.cep.pattern which is appended to this one
	 */
	public GroupPattern next(Pattern group) {
		return new GroupPattern(this, group, ConsumingStrategy.STRICT);
	}

	private void checkIfNoNotPattern() {
		if (quantifier.getConsumingStrategy() == ConsumingStrategy.NOT_FOLLOW ||
				quantifier.getConsumingStrategy() == ConsumingStrategy.NOT_NEXT) {
			throw new MalformedPatternException("Option not applicable to NOT org.apache.flink.scala.cep.pattern");
		}
	}

	private void checkIfQuantifierApplied() {
		if (!quantifier.hasProperty(Quantifier.QuantifierProperty.SINGLE)) {
			throw new MalformedPatternException("Already applied quantifier to this Pattern. " +
					"Current quantifier is: " + quantifier);
		}
	}

	private void checkIfNoGroupPattern() {
		if (this instanceof GroupPattern) {
			throw new MalformedPatternException("Option not applicable to group org.apache.flink.scala.cep.pattern");
		}
	}

	private void checkIfPreviousPatternGreedy() {
		if (previous != null && previous.getQuantifier().hasProperty(Quantifier.QuantifierProperty.GREEDY)) {
			throw new MalformedPatternException("Optional org.apache.flink.scala.cep.pattern cannot be preceded by greedy org.apache.flink.scala.cep.pattern");
		}
	}
}
