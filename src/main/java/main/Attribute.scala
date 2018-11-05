package main

import scala.collection.immutable.List


/**
  * Class for the definition of an attribute. It could be nominal or numeric attributes
  */
class Attribute(val name: String, val nominal: Boolean, val min: Double, val max: Double, val nominalValue: List[String]){

  /**
    * Nominal attribute
    */
  def isNominal: Boolean = nominal

  /**
    * Numeric attribute
    */
  def isNumeric: Boolean = !nominal

  /**
    * Max and min for the numeric attribute
    */
  def getMin: Double = min

  def getMax: Double = max

  def numValues: Int = nominalValue.size

  def valueName(pos: Int): String = nominalValue(pos)

  def getName: String = name

}
