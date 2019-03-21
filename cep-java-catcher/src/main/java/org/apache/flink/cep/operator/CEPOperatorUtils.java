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

import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.functions.NullByteKeySelector;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.nfa.compiler.NFACompiler;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.util.OutputTag;

import java.util.List;

/**
 * Utility methods for creating {@link PatternStream}.
 */
public class CEPOperatorUtils {

	/**
	 * Creates a data stream containing results of {@link PatternSelectFunction} to fully matching event patterns.
	 *
	 * @param inputStream stream of input events
	 * @param patterns org.apache.flink.scala.cep.pattern to be search for in the stream
	 * @param selectFunction function to be applied to matching event sequences
	 * @param outTypeInfo output TypeInformation of selectFunction
	 * @param <IN> type of input events
	 * @param <OUT> type of output events
	 * @return Data stream containing fully matched event sequence with applied {@link PatternSelectFunction}
	 */
	public static <IN, OUT> SingleOutputStreamOperator<OUT> createPatternStream(
			final DataStream<IN> inputStream,
			final List<Pattern> patterns,
			final PatternSelectFunction<OUT> selectFunction,
			final TypeInformation<OUT> outTypeInfo,
			final OutputTag<IN> lateDataOutputTag) {
		return createPatternStream(inputStream, patterns, outTypeInfo, new OperatorBuilder<IN, OUT>() {
			@Override
			public OneInputStreamOperator<IN, OUT> build(
				TypeSerializer<IN> inputSerializer,
				NFACompiler.NFAFactory nfaFactory,
				boolean isProcessingTime) {
				return new SelectCepOperator<>(
					inputSerializer,
					nfaFactory,
					selectFunction,
					lateDataOutputTag,
					isProcessingTime
				);
			}

			@Override
			public String getKeyedOperatorName() {
				return "SelectCepOperator";
			}

			@Override
			public String getOperatorName() {
				return "SelectCepOperator";
			}
		});
	}

	private static <IN, OUT, K> SingleOutputStreamOperator<OUT> createPatternStream(
			final DataStream<IN> inputStream,
			final List<Pattern> patterns,
			final TypeInformation<OUT> outTypeInfo,
			final OperatorBuilder<IN, OUT> operatorBuilder) {
		final TypeSerializer<IN> inputSerializer = inputStream.getType().createSerializer(inputStream.getExecutionConfig());

		// compile our org.apache.flink.scala.cep.pattern into a NFAFactory to instantiate NFAs later on
		final NFACompiler.NFAFactory nfaFactory = NFACompiler.compileFactory(patterns);

		final SingleOutputStreamOperator<OUT> patternStream;

		boolean isProcessingTime = inputStream.getExecutionEnvironment()
				.getStreamTimeCharacteristic() == TimeCharacteristic.ProcessingTime;

		if (inputStream instanceof KeyedStream) {
			KeyedStream<IN, K> keyedStream = (KeyedStream<IN, K>) inputStream;

			patternStream = keyedStream.transform(
				operatorBuilder.getKeyedOperatorName(),
				outTypeInfo,
				operatorBuilder.build(inputSerializer, nfaFactory, isProcessingTime));
		} else {
			KeySelector<IN, Byte> keySelector = new NullByteKeySelector<>();

			patternStream = inputStream.keyBy(keySelector).transform(
				operatorBuilder.getOperatorName(),
				outTypeInfo,
				operatorBuilder.build(inputSerializer, nfaFactory, isProcessingTime)).forceNonParallel();
		}

		return patternStream;
	}

	private interface OperatorBuilder<IN, OUT> {
			OneInputStreamOperator<IN, OUT> build(
			TypeSerializer<IN> inputSerializer,
			NFACompiler.NFAFactory nfaFactory,
			boolean isProcessingTime);

		String getKeyedOperatorName();

		String getOperatorName();
	}
}
