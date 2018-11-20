package fuzzy

import scala.collection.mutable.ArrayBuffer

class DecreasingLineFuzzySet(val value: ArrayBuffer[Double], val y2: Double) extends Fuzzy(value, y2) {

  override def toString: String = "DecreasingLine(" + sixDecimals.format(getValue(0)) + ", " + sixDecimals.format(getValue(1)) + ")"

  /**
    * It returns the belonging degree of the value of {@code x} with respect to this fuzzy set.
    *
    * @param x
    * @return
    */
  override def getBelongingDegree(x: Double): Double = {
    val x0 = getValue(0)
    val x1 = getValue(1)
    if (x <= x0) return 1.0
    if (x > x0 && x < x1) return (x1 - x) * (getY / (x1 - x0))
    if (x >= x1) return 0.0
    getY
  }
}
