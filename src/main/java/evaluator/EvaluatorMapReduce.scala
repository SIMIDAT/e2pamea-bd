package evaluator

import java.util

import attributes.Clase
import fuzzy.Fuzzy
import main.BigDataEPMProblem
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.util.SizeEstimator
import org.json4s.jsonwritable
import org.uma.jmetal.problem.Problem
import org.uma.jmetal.solution.{BinarySolution, Solution}
import qualitymeasures.ContingencyTable
import utils.BitSet
import weka.core.Instances

import scala.collection.mutable.ArrayBuffer

/**
  * Class for the use of an improved DNF evaluator with MapReduce
  */
class EvaluatorMapReduce extends Evaluator[BinarySolution] {


  /**
    * The bitSets for the variables. They are distributed across the cluster.
    * Each row in the RDD represent a variable with different BitSets for each possible value
    */
  var bitSets: RDD[(ArrayBuffer[BitSet], Long)] = null


  /**
    * The bitsets for the variables in a non distributed environment
    */
  var sets: ArrayBuffer[ArrayBuffer[BitSet]] = null

  /**
    * The bitsets for the determination of the belonging of the examples for each class
    */
  var classes: ArrayBuffer[BitSet] = null

  /**
    * It determines if the evaluation must be performed using the RDD (distributed) or the ArrayBuffer (local)
    */
  var bigDataProcessing = true

  /**
    * It initialises the bitset structure employed for the improved evaluation.
    *
    * @param fuzzySet      the fuzzy sets definitions
    * @param dataset       The data
    * @param sc            The spark context in order to create the RDD
    * @param numPartitions The number of partitions to create
    */
  def initialise(problem: Problem[BinarySolution]): Unit = {
    if (problem.isInstanceOf[BigDataEPMProblem]) {
      println("Initialising the precalculated structure...")

      val problema = problem.asInstanceOf[BigDataEPMProblem]
      val attrs = problema.getAttributes
      val length = problema.getDataset.count()
      classes = new ArrayBuffer[BitSet]()

      val t_ini = System.currentTimeMillis()
      for (i <- 0 until attrs.last.numValues) {
        classes += new BitSet(length.toInt)
      }
      // Calculate the bitsets for the variables
      sets = problema.getDataset.rdd.zipWithIndex().mapPartitions(x => {

        // Instatiate the whole bitsets structure for each partition with length equal to the length of data
        val partialSet = new ArrayBuffer[ArrayBuffer[BitSet]]()
        for (i <- 0 until (attrs.length - 1)) {
          partialSet += new ArrayBuffer[BitSet]()
          for (j <- 0 until attrs(i).numValues) {
            partialSet(i) += new BitSet(length.toInt)
          }
        }

        x.foreach(y => {
          val row = y._1
          val index = y._2.toInt
          for (i <- attrs.indices.dropRight(1)) {
            // For each attribute
            for (j <- 0 until attrs(i).numValues) {
              // for each label/value
              if (attrs(i).isNumeric) {
                // Numeric variable, fuzzy computation
                if (problema.calculateBelongingDegree(i, j, row.getDouble(i)) >= getMaxBelongingDegree(problema, i, row.getDouble(i))) {
                  partialSet(i)(j).set(index)
                } else {
                  partialSet(i)(j).unset(index)
                }
              } else {
                // Discrete variable
                if (attrs(i).valueName(j).equals(row.getString(i))) {
                  partialSet(i)(j).set(index)
                } else {
                  partialSet(i)(j).unset(index)
                }
              }
            }
          }
        })
        val aux = new Array[ArrayBuffer[ArrayBuffer[BitSet]]](1)
        aux(0) = partialSet
        aux.iterator
      }).reduce((x, y) => {
        for (i <- x.indices) {
          for (j <- x(i).indices) {
            x(i)(j) = x(i)(j) | y(i)(j)
          }
        }
        x
      })

      // Calculate the bitsets for the classes (Buscar forma de hacerlo distribuido, sin el collect() )
      val columna = problema.getDataset.select(attrs.last.getName).collect()
      for (i <- columna.indices) {
        val name = columna(i).getString(0)
        classes(attrs.last.nominalValue.indexOf(name)).set(i)
      }

      //println("total sample: " + length + "  class 0: " + classes(0).cardinality() + "  class 1: " + classes(1).cardinality())
      super.setProblem(problema)
      if(bigDataProcessing){
        bitSets = problema.spark.sparkContext.parallelize(sets, problema.getNumPartitions()).zipWithIndex()
        bitSets.cache()
        sets = null
        println("Pre-calculation time: " + (System.currentTimeMillis() - t_ini) + " ms. Size of Structure: " + (SizeEstimator.estimate(bitSets) / Math.pow(1000, 2)) + " MB.")
      } else {
        println("Pre-calculation time: " + (System.currentTimeMillis() - t_ini) + " ms. Size of Structure: " + (SizeEstimator.estimate(sets) / Math.pow(1000, 2)) + " MB.")

      }


    }
  }

  override def evaluate(solutionList: util.List[BinarySolution], problem: Problem[BinarySolution]): util.List[BinarySolution] = {
    // In the map phase, it is returned an array of bitsets for each variable of the problem for all the individuals
    val t_ini = System.currentTimeMillis()

    val coverages = if (bigDataProcessing) {
      calculateCoveragesBigData(solutionList, problem)
    } else {
      calculateCoveragesNonBigData(solutionList, problem)
    }

    // Calculate the contingency table for each individual
    for (i <- coverages.indices) {
      val ind = solutionList.get(i)
      if (!isEmpty(ind)) {
        val clase = ind.getAttribute(classOf[Clase[BinarySolution]]).asInstanceOf[Int]

        // tp = covered AND belong to the class
        val tp = coverages(i) & classes(clase)

        // tn = NOT covered AND DO NOT belong to the class
        val tn = (~coverages(i)) & (~classes(clase))

        // fp = covered AND DO NOT belong to the class
        val fp = coverages(i) & (~classes(clase))

        // fn = NOT covered AND belong to the class
        val fn = (~coverages(i)) & classes(clase)

        val table = new ContingencyTable(tp.cardinality(), fp.cardinality(), tn.cardinality(), fn.cardinality())

        val objectives = super.calculateMeasures(table)
        for (j <- 0 until objectives.size()) {
          ind.setObjective(j, objectives.get(j).getValue)
        }
      } else {
        // If the rule is empty, set the fitness at minimum possible value for all the objectives.
        for (j <- 0 until ind.getNumberOfObjectives) {
          ind.setObjective(j, Double.NegativeInfinity)
        }
      }
    }

    println("Time spent on the evaluation of 100 individuals after the precalculation: " + (System.currentTimeMillis() - t_ini) + " ms.")

    // Once we've got the coverage of the rules, we can calculate the contingecy tables.
    return solutionList
  }

  override def shutdown(): Unit = ???

  /**
    * It return whether the individual represents the empty pattern or not.
    *
    * @param individual
    * @return
    */
  override def isEmpty(individual: Solution[BinarySolution]): Boolean = {
    for (i <- 0 until individual.getNumberOfVariables) {
      if (participates(individual, i)) {
        return false
      }
    }
    return true

  }

  def isEmpty(individual: BinarySolution): Boolean = {
    for (i <- 0 until individual.getNumberOfVariables) {
      if (participates(individual, i)) {
        return false
      }
    }
    return true

  }

  /**
    * It returns whether a given variable of the individual participates in the pattern or not.
    *
    * @param individual
    * @param var
    * @return
    */
  override def participates(individual: Solution[BinarySolution], `var`: Int): Boolean = {
    val ind = individual.asInstanceOf[BinarySolution]

    if (ind.getVariableValue(`var`).cardinality() > 0 && ind.getVariableValue(`var`).cardinality() < ind.getNumberOfBits(`var`)) {
      true
    } else {
      false
    }
  }

  def participates(individual: BinarySolution, `var`: Int): Boolean = {

    if (individual.getVariableValue(`var`).cardinality() > 0 && individual.getVariableValue(`var`).cardinality() < individual.getNumberOfBits(`var`)) {
      true
    } else {
      false
    }
  }


  /**
    * It calculates the coverages bit sets for each individual in the population by means of performing the operations in
    * a local structure stored in {@code sets}.
    *
    * This function is much more faster than Big Data one when the number of variables is LOW.
    *
    * @param solutionList
    * @param problem
    * @return
    */
  def calculateCoveragesNonBigData(solutionList: util.List[BinarySolution], problem: Problem[BinarySolution]): Array[BitSet] = {
    val coverages = new Array[BitSet](solutionList.size())
    for (i <- coverages.indices) coverages(i) = new BitSet(sets(0)(0).capacity)

    for (i <- 0 until solutionList.size()) {
      // for each individual
      val ind = solutionList.get(i)
      var first = true
      for (j <- 0 until ind.getNumberOfVariables) {
        // for each variable
        if (participates(ind, j)) {
          // Perform OR operations between active elements in the DNF rule.
          var coverageForVariable = new BitSet(sets(0)(0).capacity)
          for (k <- 0 until ind.getNumberOfBits(j)) {
            if (ind.getVariableValue(j).get(k)) {
              coverageForVariable = coverageForVariable | sets(j)(k)
            }
          }
          // after that, perform the AND operation
          if (first) {
            coverages(i) = coverageForVariable
            first = false
          } else {
            coverages(i) = coverages(i) & coverageForVariable
          }
        }
      }
    }

    coverages
  }


  /**
    * It calculates the coverages bit sets for each individual in the population by means of performing the operations in
    * a distributed RDD structure stored in {@code bitSets}.
    *
    * This function is much more faster than local one when the number of variables is VERY HIGH.
    *
    * @param solutionList
    * @param problem
    * @return
    */
  def calculateCoveragesBigData(solutionList: util.List[BinarySolution], problem: Problem[BinarySolution]): Array[BitSet] = {
    val coverages = bitSets.mapPartitions(x => {
      val bits = x.map(y => {
        val index = y._2.toInt
        val sets = y._1
        val ors = new Array[BitSet](solutionList.size())
        for (i <- ors.indices) {
          ors(i) = new BitSet(sets(0).capacity)
        }

        for (i <- 0 until solutionList.size()) { // For all the individuals
          val ind = solutionList.get(i)
          if (participates(ind, index)) {
            // Perform OR operations between active elements in the DNF rule.
            for (j <- 0 until ind.getNumberOfBits(index)) {
              if (ind.getVariableValue(index).get(j)) {
                ors(i) = ors(i) | sets(j)
              }
            }
          } else { // If the variable does not participe, fill the bitset with 1 so the and operations are not affected.
            ors(i).setUntil(ors(i).capacity)
          }
        }
        ors
      })
      bits
    }).reduce((x, y) => {
      // The reduce phase just performs an AND operation between all the elements to get the final coverage of the individuals
      for (i <- x.indices) {
        x(i) = x(i) & y(i)
      }
      x
    })

    coverages
  }



  def setBigDataProcessing(processing : Boolean): Unit ={
    bigDataProcessing = processing
  }


  /**
    * If returns the maximum belonging degree of the LLs defined for a variable
    * @param problem
    * @param variable
    * @param x
    */
  def getMaxBelongingDegree(problem: Problem[BinarySolution], variable: Int,  x: Double): Double ={
    val problema = problem.asInstanceOf[BigDataEPMProblem]
    val tope = 10E-13

    var max = Double.NegativeInfinity

    for(i <- 0 until problema.getAttributes(variable).numValues){
      val a = problema.calculateBelongingDegree(variable, i, x)
      if(a > max){
        max = a
      }
    }

    max - tope
  }
}
