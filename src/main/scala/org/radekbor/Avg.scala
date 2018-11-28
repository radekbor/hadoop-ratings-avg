package org.radekbor

import java.io.{DataInput, DataOutput}
import java.lang.Iterable

import org.apache.hadoop.conf.{Configuration, Configured}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}
import org.apache.hadoop.util.{Tool, ToolRunner}

import scala.collection.JavaConverters._

class ExtractMarks extends Mapper[Object, Text, Text, MyPair] {

  override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, MyPair]#Context): Unit = {
    val Array(i, movieId, rate, _) = value.toString.split("\\::")
    val word = new Text()
    word.set(movieId)
    if (rate.toInt < 1) {
      context.setStatus("Detected possibly corrupted record: see logs.")
      context.getCounter(Counters.RATE_LOWER_THAN_1).increment(1)
    }

    val writable = new MyPair(rate.toInt, 1)
    context.write(word, writable)
  }
}

class IntSumReader extends Reducer[Text, MyPair, Text, MyPair] {
  override def reduce(key: Text, values: Iterable[MyPair],
                      context: Reducer[Text, MyPair, Text, MyPair]#Context): Unit = {
    val valuesAsScala = values.asScala
    val tuples = valuesAsScala.map(t => {
      (t.getA(), t.getB())
    })
    val (sum, votes) = tuples.foldLeft((0.0, 0.0))((current, x) => (current._1 + x._1, current._2 + x._2))
    context.write(key, new MyPair(sum, votes))
  }
}

class AvgCombiner extends Reducer[Text, MyPair, Text, MyPair] {

  override def reduce(key: Text, values: Iterable[MyPair],
                      context: Reducer[Text, MyPair, Text, MyPair]#Context): Unit = {
    val valuesAsScala = values.asScala
    val tuples = valuesAsScala.map(t => {
      (t.getA(), t.getB())
    })
    val (sum, votes) = tuples.foldLeft((0.0, 0.0))((current, x) => (current._1 + x._1, current._2 + x._2))
    context.write(key, new MyPair(sum / votes, votes))
  }

}

class Avg extends Configured with Tool {
  override def run(args: Array[String]): Int = {
    val configuration = new Configuration
    configuration.set("mapreduce.job.end-notification.url","http://localhost:8000/end/job/$jobId/status/$jobStatus")

    val job = Job.getInstance(configuration, "marks count")

    job.setJarByClass(this.getClass)
    job.setMapperClass(classOf[ExtractMarks])
    job.setReducerClass(classOf[IntSumReader])
    job.setCombinerClass(classOf[AvgCombiner])

    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[MyPair])

    FileInputFormat.addInputPath(job, new Path(args(0)))
    FileOutputFormat.setOutputPath(job, new Path(args(1)))
    if (job.waitForCompletion(true)) 0 else 1
  }
}

object Avg {
  def main(args: Array[String]): Unit = {
    val result = ToolRunner.run(new Avg(), args)
    System.exit(result)
  }

}
