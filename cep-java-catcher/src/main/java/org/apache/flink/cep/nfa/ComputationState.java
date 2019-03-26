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

import java.io.Serializable;

/**
 * Helper class which encapsulates the currentStateName of the NFA computation. It points to the current currentStateName,
 * the previous entry of the org.apache.flink.scala.cep.pattern, the current version and the starting timestamp
 * of the overall org.apache.flink.scala.cep.pattern.
 */
public class ComputationState extends Object implements Serializable {
	// pointer to the NFA currentStateName of the computation
	private final String currentStateName;

	private final boolean notFollowType;

	private ComputationState(final String currentState) {
		this(currentState, false);
	}

	private ComputationState(final String currentState, final boolean notFollowType) {
		this.currentStateName = currentState;
		this.notFollowType = notFollowType;
	}

	public String getCurrentStateName() {
		return currentStateName;
	}

	@Override
	public String toString() {
		return "ComputationState{" +
			"currentStateName='" + currentStateName + '\'' + '}';
	}

	public static ComputationState createStartState(final String state) {
		return new ComputationState(state);
	}

	public static ComputationState createState(final String currentState) {
		return new ComputationState(currentState);
	}

	public static ComputationState createState(final String currentState, final boolean notFollowType) {
		return new ComputationState(currentState, notFollowType);
	}
}
