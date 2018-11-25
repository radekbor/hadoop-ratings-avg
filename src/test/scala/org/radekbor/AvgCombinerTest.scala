package org.radekbor

import org.apache.hadoop.io.Text
import org.scalamock.scalatest.MockFactory
import org.scalatest.FunSuite

import scala.collection.JavaConverters._

class AvgCombinerTest extends FunSuite with MockFactory {

  val combiner = new AvgCombiner()

  test("should combiner and calculate average") {
    val key = new Text("2")
    val value1 = new MyPair(4, 2)
    val value2 = new MyPair(5, 2)
    val values = Iterable.newBuilder.+=(value1).+=(value2).result()

    val context = mock[combiner.Context]
    (context.write _).expects(new Text("2"), new MyPair(9.0/4, 4)).once()

    combiner.reduce(key, values.asJava, context)

  }

}
