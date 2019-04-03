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

package org.apache.flink.cep.nfa;

import org.apache.flink.cep.event.EventWrapper;
import org.apache.flink.cep.nfa.compiler.NFACompiler;
import org.apache.flink.cep.nfa.sharedbuffer.SharedBuffer;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cep.operator.AbstractKeyedCEPPatternOperator;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;
import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 * Non-deterministic finite automaton implementation.
 *
 * <p>The {@link AbstractKeyedCEPPatternOperator CEP operator}
 * keeps one NFA per key, for keyed input streams, and a single global NFA for non-keyed ones.
 * When an event gets processed, it updates the NFA's internal state machine.
 *
 * <p>An event that belongs to a partially matched sequence is kept in an internal
 * {@link SharedBuffer buffer}, which is a memory-optimized data-structure exactly for
 * this purpose. Events in the buffer are removed when all the matched sequences that
 * contain them are:
 * <ol>
 *  <li>emitted (success)</li>
 *  <li>discarded (patterns containing NOT)</li>
 *  <li>timed-out (windowed patterns)</li>
 * </ol>
 *
 * <p>The implementation is strongly based on the paper "Efficient Pattern Matching over Event Streams".
 *
 * @see <a href="https://people.cs.umass.edu/~yanlei/publications/sase-sigmod08.pdf">
 * https://people.cs.umass.edu/~yanlei/publications/sase-sigmod08.pdf</a>
 */
public class NFA implements Serializable {

	private Logger logger = LoggerFactory.getLogger(this.getClass());
	/**
	 * A set of all the valid NFA states, as returned by the
	 * {@link NFACompiler NFACompiler}.
	 * These are directly derived from the user-specified org.apache.flink.scala.cep.pattern.
	 */
	private final Map<String, State> states;

	private final String id;

	private final long timeoutMinutes;

	private final boolean notFollowType;

	public NFA(final String id, final Collection<State> validStates) {
		this(id, validStates, 0L);
	}

	public NFA(final String id, final Collection<State> validStates, long timeoutMinutes) {
		this(id, validStates, timeoutMinutes, false);
	}

	public NFA(final String id, final Collection<State> validStates, long timeoutMinutes, boolean notFollowType) {
		this.id = id;
		this.states = loadStates(validStates);
		this.timeoutMinutes = timeoutMinutes;
		this.notFollowType = notFollowType;
	}

	public String getId() {
		return id;
	}

	public long getTimeoutMinutes() {
		return this.timeoutMinutes;
	}

	public boolean isNotFollowType() {
		return notFollowType;
	}

	private Map<String, State> loadStates(final Collection<State> validStates) {
		Map<String, State> tmp = new HashMap<>(4);
		for (State state : validStates) {
			tmp.put(state.getName(), state);
		}
		return Collections.unmodifiableMap(tmp);
	}

	public NFAState createInitialNFAState() {
		LinkedList<ComputationState> startingStates = new LinkedList<>();
		ComputationState lastComputationState = null;
		for (State state : states.values()) {
			if (state.isStart()) {
				startingStates.addFirst(ComputationState.createState(state.getName()));
			} else if (state.isLast()) {
				lastComputationState = ComputationState.createState(state.getName());
				startingStates.add(lastComputationState);
			} else {
				startingStates.add(ComputationState.createState(state.getName()));
			}
		}
		return new NFAState(startingStates, lastComputationState);
	}

	private State getState(ComputationState state) {
		return states.get(state.getCurrentStateName());
	}

	private boolean isStartState(ComputationState state) {
		State stateObject = getState(state);
		if (stateObject == null) {
			throw new FlinkRuntimeException("State " + state.getCurrentStateName() + " does not exist in the NFA. NFA has states "
				+ states.values());
		}

		return stateObject.isStart();
	}

	private boolean isLastState(ComputationState state) {
		State stateObject = getState(state);
		if (stateObject == null) {
			throw new FlinkRuntimeException("State " + state.getCurrentStateName() + " does not exist in the NFA. NFA has states "
					+ states.values());
		}

		return stateObject.isLast();
	}

	private boolean isStopState(ComputationState state) {
		State stateObject = getState(state);
		if (stateObject == null) {
			throw new FlinkRuntimeException("State " + state.getCurrentStateName() + " does not exist in the NFA. NFA has states "
				+ states.values());
		}

		return stateObject.isStop();
	}

	private boolean isFinalState(ComputationState state) {
		State stateObject = getState(state);
		if (stateObject == null) {
			throw new FlinkRuntimeException("State " + state.getCurrentStateName() + " does not exist in the NFA. NFA has states "
				+ states.values());
		}

		return stateObject.isFinal();
	}

	public Tuple2<String, Integer> process(
			final String id,
			final NFAState nfaState,
			final EventWrapper eventWrapper) {
		return doProcess(id, nfaState, eventWrapper);
	}

	private Tuple2<String, Integer> doProcess(
			final String id,
			final NFAState nfaState,
			final EventWrapper event) {
		boolean firstMatch = false;

		int user = event.getUser();
		List<ComputationState> computationStates = nfaState.matchComputationStates(event.getUser());

		if (computationStates.isEmpty()) {
			firstMatch = true;
            computationStates = Collections.singletonList(nfaState.getStartState());
		}

		AtomicBoolean output = new AtomicBoolean(false);

		final List<ComputationState> newComputationStates = new ArrayList<>();
		try {
            for (ComputationState computationState : computationStates) {
                Collection<ComputationState> nextStates = computeNextStates(
                        computationState,
                        event,
                        nfaState);
                newComputationStates.addAll(nextStates);
            }
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

		if (newComputationStates.hashCode() != computationStates.hashCode()) {
		    nfaState.setStateChanged();
        }

		if (newComputationStates.stream().anyMatch(this::isFinalState)) {
			output.set(true);

			logger.info(String.format("Pattern: {}, output user: {}", id, user));
			// delete this user before output
            nfaState.removeUserInAllMatchs(user);
		}

		if (nfaState.isStateChanged() && firstMatch) {
			if (timeoutMinutes > 0) {
				nfaState.addUser(user, event.getTimestamp());
			}
		}

		if (output.get()) {
			return new Tuple2<>(id, user);
		}

		return null;
	}

	private static boolean isEquivalentState(final State s1, final State s2) {
		return s1.getName().equals(s2.getName());
	}

	/**
	 * Class for storing resolved transitions. It counts at insert time the number of
	 * branching transitions both for IGNORE and TAKE actions.
 	 */
	private static class OutgoingEdges {
		private List<StateTransition> edges = new ArrayList<>();

		private final State currentState;

		private int totalTakeBranches = 0;
		private int totalIgnoreBranches = 0;

		OutgoingEdges(final State currentState) {
			this.currentState = currentState;
		}

		void add(StateTransition edge) {

			if (!isSelfIgnore(edge)) {
				if (edge.getAction() == StateTransitionAction.IGNORE) {
					totalIgnoreBranches++;
				} else if (edge.getAction() == StateTransitionAction.TAKE) {
					totalTakeBranches++;
				}
			}

			edges.add(edge);
		}

		int getTotalIgnoreBranches() {
			return totalIgnoreBranches;
		}

		int getTotalTakeBranches() {
			return totalTakeBranches;
		}

		List<StateTransition> getEdges() {
			return edges;
		}

		private boolean isSelfIgnore(final StateTransition edge) {
			return isEquivalentState(edge.getTargetState(), currentState) &&
				edge.getAction() == StateTransitionAction.IGNORE;
		}
	}

	/**
	 * Computes the next computation states based on the given computation state, the current event,
	 * its timestamp and the internal state machine. The algorithm is:
	 *<ol>
	 *     <li>Decide on valid transitions and number of branching paths. See {@link OutgoingEdges}</li>
	 * 	   <li>Perform transitions:
	 * 	   	<ol>
	 *          <li>IGNORE (links in {@link SharedBuffer} will still point to the previous event)</li>
	 *          <ul>
	 *              <li>do not perform for Start State - special case</li>
	 *          	<li>if stays in the same state increase the current stage for future use with number of outgoing edges</li>
	 *          	<li>if after PROCEED increase current stage and add new stage (as we change the state)</li>
	 *          	<li>lock the entry in {@link SharedBuffer} as it is needed in the created branch</li>
	 *      	</ul>
	 *      	<li>TAKE (links in {@link SharedBuffer} will point to the current event)</li>
	 *          <ul>
	 *              <li>add entry to the shared buffer with version of the current computation state</li>
	 *              <li>add stage and then increase with number of takes for the future computation states</li>
	 *              <li>peek to the next state if it has PROCEED path to a Final State, if true create Final
	 *              ComputationState to emit results</li>
	 *          </ul>
	 *      </ol>
	 *     </li>
	 * 	   <li>Handle the Start State, as it always have to remain </li>
	 *     <li>Release the corresponding entries in {@link SharedBuffer}.</li>
	 *</ol>
	 *
	 * @param computationState Current computation state
	 * @param event Current event which is processed
	 * @return Collection of computation states which result from the current one
	 * @throws Exception Thrown if the system cannot access the state.
	 */
	private Collection<ComputationState> computeNextStates(
			final ComputationState computationState,
			final EventWrapper event,
			final NFAState nfaState) throws Exception {

		final ConditionContext context = new ConditionContext(this, null, computationState);

		final OutgoingEdges outgoingEdges = createDecisionGraph(context, computationState, event);

		// Create the computing version based on the previously computed edges
		// We need to defer the creation of computation states until we know how many edges start
		// at this computation state so that we can assign proper version
		final List<StateTransition> edges = outgoingEdges.getEdges();

		boolean needUpdateCurrentState = false;
		final List<ComputationState> resultingComputationStates = new ArrayList<>();
		resultingComputationStates.add(computationState);
		for (StateTransition edge : edges) {
			switch (edge.getAction()) {
				case IGNORE:
					if (!isStartState(computationState)) {
						if (!isEquivalentState(edge.getTargetState(), getState(computationState))) {
							ComputationState targetState = nfaState.getStateByName(edge.getTargetState().getName());
							addUserToState(nfaState, targetState, event.getUser());
							needUpdateCurrentState = true;
							resultingComputationStates.add(targetState);
						}
					}
					break;
				case TAKE:
					final State nextState = edge.getTargetState();

					if (nextState.isStop()) {
						needUpdateCurrentState = true;
						break;
					}

					if (nextState.isFinal()) {
						final ComputationState finalComputationState = nfaState.getStateByName(nextState.getName());
						resultingComputationStates.add(finalComputationState);
						needUpdateCurrentState = true;

						if (logger.isDebugEnabled()) {
							addUserToState(nfaState, finalComputationState, event.getUser());
						}
						break;
					} else {
                        ComputationState targetState = nfaState.getStateByName(nextState.getName());

                        if (!isEquivalentState(edge.getTargetState(), getState(computationState))) {
                            final State finalState = findFinalStateAfterProceed(context, nextState, event);

                            if (finalState == null) {
                                addUserToState(nfaState, targetState, event.getUser());
                                resultingComputationStates.add(targetState);
                            } else {
                                final ComputationState finalComputationState = nfaState.getStateByName(finalState.getName());
                                resultingComputationStates.add(finalComputationState);
                            }
                            needUpdateCurrentState = true;
                        }
                        break;
                    }
			}
		}

		if (needUpdateCurrentState) {
			nfaState.getPartialMatches().get(computationState).remove(event.getUser());
			resultingComputationStates.remove(0);
		}

		return resultingComputationStates;
	}


	private void addUserToState(NFAState nfaState, ComputationState targetState, int user) {
		nfaState.getPartialMatches().compute(targetState, (k, v) -> {
			RoaringBitmap bm = v == null ? new RoaringBitmap() : v;
			bm.add(user);
			return bm;
		});
	}

	private State findFinalStateAfterProceed(
			ConditionContext context,
			State state,
			EventWrapper event) {
		final Stack<State> statesToCheck = new Stack<>();
		statesToCheck.push(state);
		try {
			while (!statesToCheck.isEmpty()) {
				final State currentState = statesToCheck.pop();
				for (StateTransition transition : currentState.getStateTransitions()) {
					if (transition.getAction() == StateTransitionAction.PROCEED &&
							checkFilterCondition(context, transition.getCondition(), event)) {
						if (transition.getTargetState().isFinal()) {
							return transition.getTargetState();
						} else {
							statesToCheck.push(transition.getTargetState());
						}
					}
				}
			}
		} catch (Exception e) {
			throw new FlinkRuntimeException("Failure happened in filter function.", e);
		}

		return null;
	}

	private OutgoingEdges createDecisionGraph(
			ConditionContext context,
			ComputationState computationState,
			EventWrapper event) {
		State state = getState(computationState);
		final OutgoingEdges outgoingEdges = new OutgoingEdges(state);

		final Stack<State> states = new Stack<>();
		states.push(state);

		//First create all outgoing edges, so to be able to reason about the Dewey version
		while (!states.isEmpty()) {
			State currentState = states.pop();
			Collection<StateTransition> stateTransitions = currentState.getStateTransitions();

			// check all state transitions for each state
			for (StateTransition stateTransition : stateTransitions) {
				try {
					if (checkFilterCondition(context, stateTransition.getCondition(), event)) {
						// filter condition is true
						switch (stateTransition.getAction()) {
							case PROCEED:
								// simply advance the computation state, but apply the current event to it
								// PROCEED is equivalent to an epsilon transition
								states.push(stateTransition.getTargetState());
								break;
							case IGNORE:
							case TAKE:
								outgoingEdges.add(stateTransition);
								break;
						}
					}
				} catch (Exception e) {
					throw new FlinkRuntimeException("Failure happened in filter function.", e);
				}
			}
		}
		return outgoingEdges;
	}

	private boolean checkFilterCondition(
			ConditionContext context,
			IterativeCondition condition,
			EventWrapper eventWrapper) throws Exception {
		return condition == null || condition.filter(eventWrapper, context);
	}

	/**
	 * The context used when evaluating this computation state.
	 */
	private static class ConditionContext implements IterativeCondition.Context {

		/** The current computation state. */
		private ComputationState computationState;

		/**
		 * The matched org.apache.flink.scala.cep.pattern so far. A condition will be evaluated over this
		 * org.apache.flink.scala.cep.pattern. This is evaluated <b>only once</b>, as this is an expensive
		 * operation that traverses a path in the {@link SharedBuffer}.
		 */
		private Map<String, List> matchedEvents;

		private NFA nfa;

		private SharedBuffer sharedBuffer;

		ConditionContext(
				final NFA nfa,
				final SharedBuffer sharedBuffer,
				final ComputationState computationState) {
			this.computationState = computationState;
			this.nfa = nfa;
			this.sharedBuffer = sharedBuffer;
		}

		@Override
		public Iterable getEventsForPattern(final String key) throws Exception {
			Preconditions.checkNotNull(key);

			// the (partially) matched org.apache.flink.scala.cep.pattern is computed lazily when this method is called.
			// this is to avoid any overheads when using a simple, non-iterative condition.

			if (matchedEvents == null) {
				this.matchedEvents = new HashMap<>();
			}

			return new Iterable() {
				@Override
				public Iterator iterator() {
					List elements = matchedEvents.get(key);
					return elements == null
						? Collections.EMPTY_LIST.iterator()
						: elements.iterator();
				}
			};
		}
	}
}
