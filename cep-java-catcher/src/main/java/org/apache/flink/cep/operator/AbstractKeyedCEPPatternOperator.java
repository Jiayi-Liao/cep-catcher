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

package org.apache.flink.cep.operator;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cep.event.EventWrapper;
import org.apache.flink.cep.event.PatternWrapper;
import org.apache.flink.cep.nfa.NFA;
import org.apache.flink.cep.nfa.NFAState;
import org.apache.flink.cep.nfa.compiler.NFACompiler;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.internal.InternalMapState;
import org.apache.flink.streaming.api.operators.*;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeCallback;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;
import java.util.*;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Abstract CEP org.apache.flink.scala.cep.pattern operator for a keyed input stream. For each key, the operator creates
 * a {@link NFA} and a priority queue to buffer out of order elements. Both data structures are
 * stored using the managed keyed state. Additionally, the set of all seen keys is kept as part of the
 * operator state. This is necessary to trigger the execution for all keys upon receiving a new
 * watermark.
 *
 * @param <IN> Type of the input elements
 * @param <KEY> Type of the key on which the input stream is keyed
 * @param <OUT> Type of the output elements
 */
public abstract class AbstractKeyedCEPPatternOperator<IN, KEY, OUT, F extends Function>
		extends AbstractUdfStreamOperator<OUT, F>
		implements OneInputStreamOperator<IN, OUT>, Triggerable<KEY, VoidNamespace> {

	private final NFACompiler.NFAFactory nfaFactory;

	private MapStateDescriptor<String, NFAState> patternStateMapDesc;

	private InternalMapState<String, VoidNamespace, String, NFAState> patternState;

	private Map<NFA, NFAState> nfaStateMap;

	private Map<String, NFA> nfaMap;

	private Map<Long, List<EventWrapper>> eventBuffer = new HashMap<>();

	private transient InternalTimerService<VoidNamespace> timerService;

	private long lastWatermark = Long.MIN_VALUE;

	private boolean isProcessingTime;

	private AtomicLong lastTimestamp;

	private long idleDuration = 15 * 60 * 1000;


	/**
	 * {@link OutputTag} to use for late arriving events. Elements with timestamp smaller than
	 * the current watermark will be emitted to this.
	 */
	protected final OutputTag<IN> nfaAbandonOutputTag;

	public AbstractKeyedCEPPatternOperator(
			final TypeSerializer<IN> inputSerializer,
			final NFACompiler.NFAFactory nfaFactory,
			final F function,
			final OutputTag<IN> lateDataOutputTag,
			final boolean isProcessingTime) {
		super(function);

		this.nfaFactory = Preconditions.checkNotNull(nfaFactory);
		this.nfaAbandonOutputTag = lateDataOutputTag;
		this.isProcessingTime = isProcessingTime;
		this.patternStateMapDesc = new MapStateDescriptor<>("cep",
                BasicTypeInfo.STRING_TYPE_INFO, TypeInformation.of(NFAState.class));
		patternStateMapDesc.setQueryable("cep");
	}

	@Override
	public void initializeState(StateInitializationContext context) throws Exception {
		super.initializeState(context);

//		if (context.isRestored()) {
//		    patternState = (InternalMapState<String, VoidNamespace, String, NFAState>) context.getKeyedStateStore().getMapState(patternStateMapDesc);
//        }
	}

	@Override
	public void open() throws Exception {
		super.open();

		this.nfaMap = nfaFactory.createNFA();
		this.nfaStateMap = new HashMap<>();
		timerService = getInternalTimerService(
				"watermark-callbacks",
				VoidNamespaceSerializer.INSTANCE,
				this);
		this.patternState = (InternalMapState<String, VoidNamespace, String, NFAState>) getOrCreateKeyedState(VoidNamespaceSerializer.INSTANCE, patternStateMapDesc);
		patternState.setCurrentNamespace(VoidNamespace.INSTANCE);
	}

	@Override
	public void processElement(StreamRecord<IN> element) {
		if (element.getValue().getClass() == PatternWrapper.class) {
			// Add a org.apache.flink.scala.cep.pattern dynamically(needs a new design)
			PatternWrapper patternWrapper = (PatternWrapper) element.getValue();
			NFACompiler.NFAFactory factory = NFACompiler.compileFactory(Collections.singletonList(patternWrapper.getPattern()));
			Map<String, NFA> updateNFA = factory.createNFA();
			this.nfaMap.compute(patternWrapper.getId(), (k, v) -> {
				if (v != null && !v.equals(updateNFA.get(k))) {
					this.nfaStateMap.put(v, null);
				}
				return updateNFA.get(k);
			});
		} else if (element.getValue().getClass() == EventWrapper.class) {
			EventWrapper eventWrapper = (EventWrapper) element.getValue();
			if (isProcessingTime) {
				processEvent(eventWrapper);
			} else {
				if (eventWrapper.getTimestamp() > lastWatermark) {
					timerService.registerEventTimeTimer(VoidNamespace.INSTANCE, lastWatermark+ 1);
					bufferEvent(eventWrapper);
				}
			}
		}
	}

	@Override
	public void onEventTime(InternalTimer<KEY, VoidNamespace> timer) throws Exception {
		// do not support event time
		long newWatermark = timerService.currentWatermark();
		PriorityQueue<Long> sortedTimestamps = getSortedTimestamps();

		while (!sortedTimestamps.isEmpty() && sortedTimestamps.peek() <= timerService.currentWatermark()) {
			long timestamp = sortedTimestamps.poll();
			List<EventWrapper> eventWrappers = eventBuffer.get(timestamp);
			eventWrappers.forEach(this::processEvent);
            eventBuffer.remove(timestamp);
		}
		lastWatermark = Math.min(newWatermark, lastWatermark);
	}

	@Override
	public void onProcessingTime(InternalTimer<KEY, VoidNamespace> timer) {
		// do not support processing time timer
	}

	private PriorityQueue<Long> getSortedTimestamps() throws Exception {
		PriorityQueue<Long> sortedTimestamps = new PriorityQueue<>();
		for (long timestamp : eventBuffer.keySet()) {
			sortedTimestamps.offer(timestamp);
		}
		return sortedTimestamps;
	}

	private void bufferEvent(EventWrapper eventWrapper) {
		long timestamp = eventWrapper.getTimestamp();
		List<EventWrapper> eventsList = eventBuffer.getOrDefault(timestamp, new ArrayList<>());
		eventsList.add(eventWrapper);
		eventBuffer.put(timestamp, eventsList);
	}

	private void processEvent(EventWrapper eventWrapper) {
		// set key first
		setCurrentKey(eventWrapper.getPattern());
		
		String patternId = eventWrapper.getPattern();
		NFA nfa = nfaMap.getOrDefault(patternId, null);
		if (nfa != null) {
			NFAState nfaState = nfaStateMap.computeIfAbsent(nfa, k -> {
				NFAState x = k.createInitialNFAState();
				try {
					patternState.put(patternId, x);
				} catch (Exception e) {
					throw new RuntimeException("Fail to modify patternState.");
				}
				return x;
			});
			try {
			    long timeoutMinutes = nfa.getTimeoutMinutes();
				if (timeoutMinutes > 0 && lastWatermark != Long.MIN_VALUE) {
					List<Integer> removeUsers = nfaState.removeTimeoutUsers(
							lastWatermark - timeoutMinutes * 60000L, nfa.isNotFollowType());
					if (removeUsers.size() > 0) {
						processMatchedSequences(Tuple2.of(patternId, eventWrapper.getUser()), eventWrapper.getTimestamp());
					}
                }

				processEvent(nfa, nfaState, eventWrapper, eventWrapper.getTimestamp());
				if (!nfaState.isStateChanged()) {
					output.collect(nfaAbandonOutputTag, new StreamRecord<>((IN)eventWrapper));
				}
				updateNFA(nfaState);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
	}

	private void updateNFA(NFAState nfaState) {
		if (nfaState.isStateChanged()) {
			nfaState.resetStateChanged();
		}
	}

	/**
	 * Process the given event by giving it to the NFA and outputting the produced set of matched
	 * event sequences.
	 *
	 * @param nfaState Our NFAState object
	 * @param event The current event to be processed
	 * @param timestamp The timestamp of the event
	 */
	private void processEvent(NFA nfa, NFAState nfaState, EventWrapper event, long timestamp) throws Exception {
		Tuple2<String, Integer> matchingUser = nfa.process(nfa.getId(), nfaState, event);
		processMatchedSequences(matchingUser, timestamp);
	}

	protected abstract void processMatchedSequences(@Nullable Tuple2<String, Integer> matchingUser, long timestamp) throws Exception;
}
