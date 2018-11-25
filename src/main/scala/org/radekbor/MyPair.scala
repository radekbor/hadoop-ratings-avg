package org.radekbor

import java.io.{DataInput, DataOutput}

import org.apache.hadoop.io.{DoubleWritable, Writable}

class MyPair(initA: Double, initB: Double) extends Writable {

  def this() = this(0, 0)

  private[this] val a = new DoubleWritable(initA)
  private[this] val b = new DoubleWritable(initB)

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

  override def equals(obj: scala.Any): Boolean = {
    obj match {
      case pair: MyPair =>
        pair.getA().equals(this.getA()) && pair.getB().equals(this.getB())
      case _ =>
        false
    }
  }

  override def toString: String = a.get() + " " + b.get()
}
