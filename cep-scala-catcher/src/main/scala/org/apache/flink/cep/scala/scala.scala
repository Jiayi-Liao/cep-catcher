package org.apache.flink.cep

import org.apache.flink.api.scala.ClosureCleaner

import java.util.{List => JList, Map => JMap}
import org.apache.flink.cep.{PatternStream => JPatternStream}

package object scala {


  import collection.JavaConverters._
  import collection.Map
  /**
    * Utility method to wrap [[org.apache.flink.cep.PatternStream]] for usage with the Scala API.
    *
    * @param javaPatternStream The underlying pattern stream from the Java API
    * @tparam T Type of the events
    * @return A pattern stream from the Scala API which wraps a pattern stream from the Java API
    */
  private[flink] def wrapPatternStream[T](javaPatternStream: JPatternStream[T])
  : scala.PatternStream[T] = {
    Option(javaPatternStream) match {
      case Some(p) => scala.PatternStream[T](p)
      case None =>
        throw new IllegalArgumentException("PatternStream from Java API must not be null.")
    }
  }

  private[flink] def cleanClosure[F <: AnyRef](f: F, checkSerializable: Boolean = true): F = {
    ClosureCleaner.clean(f, checkSerializable)
    f
  }

  private[flink] def mapToScala[T](map: JMap[String, JList[T]]): Map[String, Iterable[T]] = {
    map.asScala.mapValues(_.asScala.toIterable)
  }
}
