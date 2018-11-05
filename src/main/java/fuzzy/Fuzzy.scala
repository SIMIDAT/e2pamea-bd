package fuzzy

import scala.collection.mutable.ArrayBuffer


abstract class Fuzzy(val values: ArrayBuffer[Double],
                     var y: Double) {

  def getY: Double = y

  def setY(y: Double): Unit = {
    this.y = y
  }

  def getValue(index: Int): Double = values(index)

  def setValue(index: Int, value: Double): Unit = {
    values(index) = value
  }

  /**
    * It returns the belonging degree of the value of {@code x} with respect to this fuzzy set.
    *
    * @param x
    * @return
    */
  def getBelongingDegree(x: Double): Double

  def toString: String
}
