package org.apache.flink.cep.scala.model

import java.util

import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.cep.event

class PatternWrapper(patterId: String, pattern: Pattern, rules: Array[String]) {

  def asJava(): event.PatternWrapper = {
    val jPatternWrapper = new event.PatternWrapper(patterId, pattern.wrappedPattern)
    if (rules.length > 0) {
      jPatternWrapper.setRules(util.Arrays.asList(rules: _*))
    }
    jPatternWrapper
  }

}
