package utils

import org.apache.spark.util.AccumulatorV2

class PopulationAccumulator(var _value: BitSet, var size: Int) extends AccumulatorV2[Int, BitSet]{

  def this() = {
    this(new BitSet(0), 0)
  }

 def this(size: Int) = {
    this(new BitSet(size), size)

 }
  override def isZero: Boolean = value.cardinality() == 0

  override def copy(): AccumulatorV2[Int, BitSet] = new PopulationAccumulator(value, value.cardinality())

  override def reset(): Unit = value.clearUntil(value.capacity)

  override def add(v: Int): Unit = value.set(v)

  override def merge(other: AccumulatorV2[Int, BitSet]): Unit = new PopulationAccumulator(value | other.value, (value | other.value).capacity )

  override def value: BitSet = _value
}
