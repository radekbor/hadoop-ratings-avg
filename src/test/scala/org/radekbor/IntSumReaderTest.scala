package org.radekbor

import org.apache.hadoop.io.Text
import org.scalamock.scalatest.MockFactory
import org.scalatest.FunSuite
import scala.collection.JavaConverters._

class IntSumReaderTest extends FunSuite with MockFactory {

  val reducer = new IntSumReader()

  test("should sum values and sum marks") {
    val key = new Text("2")
    val value1 = new MyPair(3, 1)
    val value2 = new MyPair(2, 1)
    val values = Iterable.newBuilder.+=(value1).+=(value2).result()

    val context = mock[reducer.Context]
    (context.write _).expects(new Text("2"), new MyPair(5, 2)).once()

    reducer.reduce(key, values.asJava, context)

  }

}
