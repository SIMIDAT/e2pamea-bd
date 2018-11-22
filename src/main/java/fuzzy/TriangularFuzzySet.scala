package fuzzy

import scala.collection.mutable.ArrayBuffer


/**
  * Class for the definition of a triangular fuzzy set.
  *
  * It has three values, the minimum, the medium and the maximum
  */
class TriangularFuzzySet(val value: ArrayBuffer[Double], val y2: Double) extends Fuzzy(value, y2) {

  override def getBelongingDegree(x: Double): Double = {
    val x0 = getValue(0)
    val x1 = getValue(1)
    val x2 = getValue(2)
    if (x <= x0 || x >= x2) return 0.0
    if (x < x1) return (x - x0) * (getY / (x1 - x0))
    if (x > x1) return (x2 - x) * (getY / (x2 - x1))
    getY
  }

  override def toString: String = "Triangular(" + sixDecimals.format(getValue(0)) + ", " + sixDecimals.format(getValue(1)) + ", " +
                  sixDecimals.format(getValue(2)) + ")"
}
