package org.radekbor

import org.apache.hadoop.io.Text
import org.scalamock.scalatest.MockFactory
import org.scalatest.FunSuite


class ExtractMarksTest extends FunSuite with MockFactory {

  val extractor = new ExtractMarks()


  test("should extract marks from line") {
    val key = mock[Object]
    val value = new Text("1::2::3::4")

    val context = mock[extractor.Context]
    (context.write _).expects(new Text("2"), new MyPair(3, 1)).once()

    extractor.map(key, value, context)

  }
}
