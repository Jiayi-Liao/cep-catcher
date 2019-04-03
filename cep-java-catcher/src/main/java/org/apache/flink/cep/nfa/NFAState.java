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

import org.roaringbitmap.RoaringBitmap;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;

/**
 * State kept for a {@link NFA}.
 */
public class NFAState implements Serializable {

	private Map<ComputationState, RoaringBitmap> partialMatches;

	private Map<String, ComputationState> nameStateMapping;

	private ComputationState startState;

	private ComputationState lastState;

	/* minute level */
	private NavigableMap<Long, RoaringBitmap> timeBitmap;

	/**
	 * Flag indicating whether the matching status of the state machine has changed.
	 */
	private boolean stateChanged;

	public NFAState(Iterable<ComputationState> states, ComputationState lastState) {
		this.partialMatches = new HashMap<>();
		this.nameStateMapping = new HashMap<>();
		boolean isStart = true;
		for (ComputationState startingState : states) {
			if (isStart) {
				this.startState = startingState;
				isStart = false;
			}
			partialMatches.put(startingState, new RoaringBitmap());
			nameStateMapping.put(startingState.getCurrentStateName(), startingState);
		}

		this.lastState = lastState;
		this.timeBitmap = new ConcurrentSkipListMap<>();
	}

	public ComputationState getStartState() {
		return startState;
	}

	public ComputationState getStateByName(String name) {
		return nameStateMapping.computeIfAbsent(name, ComputationState::createState);
	}

	public void addUser(int user, long timestamp) {
		long materializedTimestamp = timestamp - timestamp % 60000;
		if (!timeBitmap.containsKey(materializedTimestamp)) {
			timeBitmap.put(materializedTimestamp, new RoaringBitmap());
		}

		timeBitmap.get(materializedTimestamp).add(user);
	}

	public void removeUser(ComputationState state, int user) {
		partialMatches.get(state).remove(user);
	}

	public void removeUserInAllMatchs(int user) {
		removeUser(user, false);
	}

	public List<Integer> removeTimeoutUsers(long lowerBoundTimestamp) {
		return removeTimeoutUsers(lowerBoundTimestamp, false);
	}

	public List<Integer> removeTimeoutUsers(long lowerBoundTimestamp, boolean isNotFollowType) {
		List<Integer> removeUsers = new ArrayList<>();
		timeBitmap.subMap(0L, lowerBoundTimestamp).forEach((key1, value) -> {
			long key = key1;
			for (Integer user : value) {
				int removeUser = removeUser(user, isNotFollowType);
				if (removeUser > 0) {
					removeUsers.add(removeUser);
				}
			}
			timeBitmap.remove(key);
		});
		return removeUsers;
	}

	private int removeUser(int user, boolean isNotFollowType) {
		if (isNotFollowType) {
			if (partialMatches.get(lastState).contains(user)) {
				partialMatches.values().forEach(bm -> {
					if (bm.contains(user)) {
						bm.remove(user);
					}
				});
				return user;
			}
		} else {
			partialMatches.values().forEach(bm -> {
				if (bm.contains(user)) {
					bm.remove(user);
				}
			});
		}
		return -1;
	}

	/**
	 * Check if the matching status of the NFA has changed so far.
	 *
	 * @return {@code true} if matching status has changed, {@code false} otherwise
	 */
	public boolean isStateChanged() {
		return stateChanged;
	}

	/**
	 * Reset the changed bit checked via {@link #isStateChanged()} to {@code false}.
	 */
	public void resetStateChanged() {
		this.stateChanged = false;
	}

	/**
	 * Set the changed bit checked via {@link #isStateChanged()} to {@code true}.
	 */
	public void setStateChanged() {
		this.stateChanged = true;
	}

	public Map<ComputationState, RoaringBitmap> getPartialMatches() {
		return partialMatches;
	}

	public void setNewPartialMatches(Set<ComputationState> newPartialMatches) {
		newPartialMatches.forEach(state -> this.partialMatches.put(state, new RoaringBitmap()));
	}

	public List<ComputationState> matchComputationStates(int user) {
		return partialMatches.entrySet().stream().filter(entry -> {
			RoaringBitmap bm = entry.getValue();
			return bm.contains(user);
		}).map(Map.Entry::getKey).collect(Collectors.toList());
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		NFAState nfaState = (NFAState) o;
		return Arrays.equals(partialMatches.keySet().toArray(), nfaState.partialMatches.keySet().toArray());
	}

	@Override
	public int hashCode() {
		return Objects.hash(partialMatches);
	}

	@Override
	public String toString() {
		return "NFAState{" +
			"partialMatches=" + partialMatches +
			", stateChanged=" + stateChanged +
			'}';
	}
}
