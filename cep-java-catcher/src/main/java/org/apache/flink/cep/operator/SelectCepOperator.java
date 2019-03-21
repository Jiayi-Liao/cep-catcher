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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.nfa.compiler.NFACompiler;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.OutputTag;

import javax.annotation.Nullable;

/**
 * Version of {@link AbstractKeyedCEPPatternOperator} that applies given {@link PatternSelectFunction} to fully matched event patterns.
 *
 * @param <IN> Type of the input elements
 * @param <KEY> Type of the key on which the input stream is keyed
 * @param <OUT> Type of the output elements
 */
public class SelectCepOperator<IN, KEY, OUT>
	extends AbstractKeyedCEPPatternOperator<IN, KEY, OUT, PatternSelectFunction<OUT>> {

	public SelectCepOperator(
		TypeSerializer<IN> inputSerializer,
		NFACompiler.NFAFactory nfaFactory,
		PatternSelectFunction<OUT> function,
		OutputTag<IN> lateDataOutputTag,
		boolean isProcessingTime) {
		super(inputSerializer, nfaFactory, function, lateDataOutputTag, isProcessingTime);
	}

	@Override
	protected void processMatchedSequences(@Nullable Tuple2<String, Integer> matchingUser,
										   long timestamp) throws Exception {
		if (matchingUser != null) {
			output.collect(new StreamRecord<>(getUserFunction().select(matchingUser), timestamp));
		}
	}
}
