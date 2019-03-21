package org.apache.flink

import org.apache.flink.api.java.tuple
import org.apache.flink.api.scala._
import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.event.EventWrapper
import org.apache.flink.cep.pattern.conditions.IterativeCondition
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.datastream.DataStreamUtils
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.junit.Test

class CEPScalaITCase {

  val simplePattern = "simplePattern"

  @Test
  def testSimplePattern(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val curr = System.currentTimeMillis()

    val pages = Array(
      Page("0a1b4118dd954ec3bcc69da5138bdb96", "/register", 2, curr),
      Page("0a1b4118dd954ec3bcc69da5138bdb96", "/feedback", 1, curr),
      Page("0a1b4118dd954ec3bcc69da5138bdb96", "/login", 2, curr),
      Page("0a1b4118dd954ec3bcc69da5138bdb96", "/register", 3, curr),
      Page("0a1b4118dd954ec3bcc69da5138bdb96", "/exit", 1, curr),
      Page("0a1b4118dd954ec3bcc69da5138bdb96", "/purchase", 2, curr),
      Page("0a1b4118dd954ec3bcc69da5138bdb96", "/purchase", 3, curr)
    )

    val input = env.fromCollection(pages)
      .map(page => new EventWrapper[Page](page, page.timestamp, page.user, "simplePattern"))

    val resultList= new java.util.ArrayList[String]

    val patterns = buildPatterns()
    val result = CEP.pattern(input, patterns)
      .select(new PatternSelectFunction[String] {
        override def select(matchingUser: tuple.Tuple2[String, Integer]): String = {
          matchingUser.f0 + "," + matchingUser.f1
        }
      })

    result.print()

    env.execute()

  }


  private def buildPatterns(): scala.List[Pattern] = {
    val testPattern = Pattern.begin("register").where(new IterativeCondition {
      override def filter(eventWrapper: EventWrapper[_], context: IterativeCondition.Context): Boolean = {
        val page = eventWrapper.getEvent.asInstanceOf[Page]
        page.url == "/register"
      }
    }).followedBy("login").where(new IterativeCondition {
      override def filter(eventWrapper: EventWrapper[_], context: IterativeCondition.Context): Boolean = {
        val page = eventWrapper.getEvent.asInstanceOf[Page]
        page.url == "/login"
      }
    }).followedBy("purchase").where(new IterativeCondition {
      override def filter(eventWrapper: EventWrapper[_], context: IterativeCondition.Context): Boolean = {
        val page = eventWrapper.getEvent.asInstanceOf[Page]
        page.url == "/purchase"
      }
    })
    testPattern.setId(simplePattern)
    Array(testPattern).toList
  }
}
