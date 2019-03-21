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

package org.apache.flink.cep;

import org.apache.flink.cep.nfa.NFA;
import org.apache.flink.cep.operator.CEPOperatorUtils;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.util.OutputTag;

import java.util.List;

/**
 * Stream abstraction for CEP org.apache.flink.scala.cep.pattern detection. A org.apache.flink.scala.cep.pattern stream is a stream which emits detected
 * org.apache.flink.scala.cep.pattern sequences as a map of events associated with their names. The org.apache.flink.scala.cep.pattern is detected using a
 * {@link NFA}. In order to process the detected sequences, the user
 * has to specify a {@link PatternSelectFunction} or.
 *
 *
 * @param <T> Type of the events
 */
public class PatternStream<T> {

	// underlying data stream
	private final DataStream<T> inputStream;

	private final List<Pattern> patterns;

	/**
	 * Side output {@code OutputTag} for late data. If no tag is set late data will be simply
	 * dropped.
	 */
	private OutputTag<T> lateDataOutputTag;

	PatternStream(final DataStream<T> inputStream, final List<Pattern> patterns) {
		this.inputStream = inputStream;
		this.patterns = patterns;
	}

	public Pattern getPattern() {
		return null;
	}

	public DataStream<T> getInputStream() {
		return inputStream;
	}

	public <R> SingleOutputStreamOperator<R> select(final PatternSelectFunction<R> patternSelectFunction) {
		// we have to extract the output type from the provided org.apache.flink.scala.cep.pattern selection function manually
		// because the TypeExtractor cannot do that if the method is wrapped in a MapFunction

		TypeInformation<R> returnType = TypeExtractor.getUnaryOperatorReturnType(
			patternSelectFunction,
			PatternSelectFunction.class,
			0,
			1,
			TypeExtractor.NO_INDEX,
			inputStream.getType(),
			null,
			false);

		return select(patternSelectFunction, returnType);
	}



	/**
	 * Invokes the {@link org.apache.flink.api.java.ClosureCleaner}
	 * on the given function if closure cleaning is enabled in the {@link ExecutionConfig}.
	 *
	 * @return The cleaned Function
	 */
	private  <F> F clean(F f) {
		return inputStream.getExecutionEnvironment().clean(f);
	}

	/**
	 * Applies a select function to the detected org.apache.flink.scala.cep.pattern sequence. For each org.apache.flink.scala.cep.pattern sequence the
	 * provided {@link PatternSelectFunction} is called. The org.apache.flink.scala.cep.pattern select function can produce
	 * exactly one resulting element.
	 *
	 * @param patternSelectFunction The org.apache.flink.scala.cep.pattern select function which is called for each detected
	 *                              org.apache.flink.scala.cep.pattern sequence.
	 * @param <R> Type of the resulting elements
	 * @param outTypeInfo Explicit specification of output type.
	 * @return {@link DataStream} which contains the resulting elements from the org.apache.flink.scala.cep.pattern select
	 *         function.
	 */
	public <R> SingleOutputStreamOperator<R> select(final PatternSelectFunction<R> patternSelectFunction,
                                                    TypeInformation<R> outTypeInfo) {
		return CEPOperatorUtils.createPatternStream(inputStream, patterns,
                clean(patternSelectFunction), outTypeInfo, lateDataOutputTag);
	}

	public PatternStream<T> sideOutputLateData(OutputTag<T> outputTag) {
		this.lateDataOutputTag = clean(outputTag);
		return this;
	}
}
