package org.apache.flink.cep.scala

import java.util

import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.cep.{CEP => JCEP}
import org.apache.flink.streaming.api.scala.DataStream

object CEP {

  def pattern[T](input: DataStream[T], pattern: List[Pattern]): PatternStream[T] = {
    wrapPatternStream(JCEP.pattern(input.javaStream, util.Arrays.asList(pattern.map(_.wrappedPattern): _*)))
  }
}
