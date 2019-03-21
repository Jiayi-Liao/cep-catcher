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


import org.apache.flink.cep.pattern.conditions.IterativeCondition;

/**
 * Base class for a group org.apache.flink.scala.cep.pattern definition.
 *
 */
public class GroupPattern extends Pattern {

	/** Group org.apache.flink.scala.cep.pattern representing the org.apache.flink.scala.cep.pattern definition of this group. */
	private final Pattern groupPattern;

	GroupPattern(
		final Pattern previous,
		final Pattern groupPattern,
		final Quantifier.ConsumingStrategy consumingStrategy) {
		super("GroupPattern", previous, consumingStrategy);
		this.groupPattern = groupPattern;
	}

	@Override
	public Pattern where(IterativeCondition condition) {
		throw new UnsupportedOperationException("GroupPattern does not support where clause.");
	}

	@Override
	public Pattern or(IterativeCondition condition) {
		throw new UnsupportedOperationException("GroupPattern does not support or clause.");
	}

	public Pattern getRawPattern() {
		return groupPattern;
	}
}
