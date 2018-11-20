package org.radekbor

import java.io.{BufferedReader, InputStreamReader}

import org.apache.hadoop.conf.Configuration
import org.scalatest.Matchers

class AvgTest extends org.scalatest.FunSuite with Matchers {

  test("Should pass and return avg") {
    import org.apache.hadoop.fs.FileSystem
    val conf = new Configuration()
    conf.set("fs.default.name", "file:///")
    conf.set("mapred.job.tracker", "local")
    import org.apache.hadoop.fs.Path
    val input = new Path("./src/test/resources/ratings.data")
    val output = new Path("./target/output")
    val fs = FileSystem.getLocal(conf)
    fs.delete(output, true) // delete old output

    val driver = new Avg
    driver.setConf(conf)
    val exitCode = driver.run(Array[String](input.toString, output.toString))
    exitCode should be(0)


    val in = fs.open(new Path("target/output/part-r-00000"))
    val br = new BufferedReader(new InputStreamReader(in))
    lineToResult(br.readLine()) should be(Array(10, 4.0, 2.0))
    lineToResult(br.readLine()) should be(Array(11, 4.0, 3.0))
    in.close()
  }

  private def lineToResult(arg: String): Array[Double] = {
    arg.split("\\s+").map(_.toDouble)
  }

}
