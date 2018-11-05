package org.radekbor

import java.io.{DataInput, DataOutput}
import java.lang.Iterable

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text, Writable}
import org.apache.hadoop.mapred.join.TupleWritable
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}

import scala.collection.JavaConverters._

class MyPair(initA: Int, initB: Int) extends Writable {

  def this() = this(0, 0)

  private[this] val a = new IntWritable(initA)
  private[this] val b = new IntWritable(initB)

  def getA() = a.get()
  def getB() = b.get()

  override def write(out: DataOutput): Unit = {
    a.write(out)
    b.write(out)
  }

  override def readFields(in: DataInput): Unit = {
    a.readFields(in)
    b.readFields(in)
  }

  override def toString: String = a.get() + " " + b.get()
}

class ExtractMarks extends Mapper[Object, Text, Text, MyPair] {

  override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, MyPair]#Context): Unit = {
    val Array(i, movieId, rate, _) = value.toString.split("\\::")
    val word = new Text()
    word.set(movieId)
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
    val tuple = tuples.foldLeft((0, 0))((current, x) => (current._1 + x._1, current._2 + x._2))

    context.write(key, new MyPair(tuple._1, tuple._2))
  }
}


object Avg {
  def main(args: Array[String]): Unit = {
    val configuration = new Configuration
    val job = Job.getInstance(configuration, "marks count")

    job.setJarByClass(this.getClass)
    job.setMapperClass(classOf[ExtractMarks])
    job.setCombinerClass(classOf[IntSumReader])
    job.setReducerClass(classOf[IntSumReader])

    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[MyPair])

    FileInputFormat.addInputPath(job, new Path(args(0)))
    FileOutputFormat.setOutputPath(job, new Path(args(1)))
    System.exit(if (job.waitForCompletion(true)) 0 else 1)
  }

}
