package evaluator
import java.util

import fuzzy.Fuzzy
import main.{BigDataEPMProblem, Clase}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.util.SizeEstimator
import org.apache.spark.util.collection.BitSet
import org.uma.jmetal.problem.Problem
import org.uma.jmetal.solution.{BinarySolution, Solution}
import weka.core.Instances

import scala.collection.mutable.ArrayBuffer

/**
  * Class for the use of an improved DNF evaluator with MapReduce
  */
class EvaluatorMapReduce extends Evaluator[BinarySolution]{


  /**
    * The bitSets for the variables. They are distributed across the cluster.
    * Each row in the RDD represent a variable with different BitSets for each possible value
    */
  var bitSets: RDD[(ArrayBuffer[BitSet],Long)] = null

  /**
    * The bitsets for the determination of the belonging of the examples for each class
    */
  var classes: ArrayBuffer[BitSet] = null

  /**
    * It initialises the bitset structure employed for the improved evaluation.
    *
    * @param fuzzySet the fuzzy sets definitions
    * @param dataset The data
    * @param sc The spark context in order to create the RDD
    * @param numPartitions The number of partitions to create
    */
  def initialise(problem: Problem[BinarySolution]): Unit = {
    if(problem.isInstanceOf[BigDataEPMProblem]){
      println("Initialising the precalculated structure...")

      val problema = problem.asInstanceOf[BigDataEPMProblem]
      val attrs = problema.getAttributes
      val length = problema.getDataset.count()
      classes = new ArrayBuffer[BitSet]()

      val t_ini = System.currentTimeMillis()
      for(i <- 0 until attrs.last.numValues){
        classes += new BitSet(length.toInt)
      }
      // Calculate the bitsets for the variables
      val sets = problema.getDataset.rdd.zipWithIndex().mapPartitions(x => {

        // Instatiate the whole bitsets structure for each partition with length equal to the length of data
        val partialSet = new ArrayBuffer[ArrayBuffer[BitSet]]()
        for(i <- 0 until (attrs.length - 1)){
          partialSet += new ArrayBuffer[BitSet]()
          for(j <- 0 until attrs(i).numValues){
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
                if (problema.calculateBelongingDegree(i, j, row.getDouble(i)) > 0){ // Cambiar por grado pertenecia máximo respecto al resto de labels
                  partialSet(i)(j).set(index)
                } else {
                  partialSet(i)(j).unset(index)
                }
              } else {
                // Discrete variable
                if(attrs(i).valueName(j).equals(row.getString(i))){
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
      }).reduce((x,y) => {
        for(i <- x.indices){
          for(j <- x(i).indices){
            x(i)(j) = x(i)(j) | y(i)(j)
          }
        }
        x
      })

      // Calculate the bitsets for the classes (Buscar forma de hacerlo distribuido)
      val columna = problema.getDataset.select(attrs.last.getName).collect()
      for(i <- columna.indices){
        val name = columna(i).getString(0)
        classes(attrs.last.nominalValue.indexOf(name)).set(i)
      }

      println("Pre-calculation time: " + (System.currentTimeMillis() - t_ini) / 1000.0 + " seconds. Size: " + (SizeEstimator.estimate(sets) / 1024 /1024) + " MB.")
      println("total sample: " + length + "  class 0: " + classes(0).cardinality() + "  class 1: " + classes(1).cardinality())
      super.setProblem(problema)
      bitSets = problema.spark.sparkContext.parallelize(sets, problema.getNumPartitions()).zipWithIndex()
      bitSets.cache()
    }
  }

  override def evaluate(solutionList: util.List[BinarySolution], problem: Problem[BinarySolution]): util.List[BinarySolution] = {

    // In the map phase, it is returned an array of bitsets for each variable of the problem for all the individuals
    val t_ini = System.currentTimeMillis()
    val coverages = bitSets.mapPartitions(x => {
      val bits = x.map( y => {
        val index = y._2.toInt
        val sets = y._1
        val ors = new Array[BitSet](solutionList.size())
        for(i <- ors.indices){
          ors(i) = new BitSet(sets(0).capacity)
        }

        for(i <- 0 until solutionList.size()){ // For all the individuals
          val ind = solutionList.get(i)
          if(participates(ind, index)){
            // Perform OR operations between active elements in the DNF rule.
            for(j <- 0 until ind.getNumberOfBits(index)){
              if(ind.getVariableValue(index).get(j)){
                ors(i) = ors(i) | sets(j)
              }
            }
          }
        }
        ors
      })
      bits
    }).reduce((x,y) => {
     // The reduce phase just performs an AND operation between all the elements to get the final coverage of the individuals
      for(i <- x.indices){
        x(i) = x(i) & y(i)
      }
      x
    })


    for(i <- coverages.indices){
      val ind = solutionList.get(i)
      val clase =  ind.getAttribute(Clase[BinarySolution].getClass)
     
    }
    println("Time spent on the evaluation of 100 individuals after the precalculation: " + (System.currentTimeMillis() - t_ini) + " ms.")

    // Once we've got the coverage of the rules, we can calculate the contingecy tables.

    null
  }

  override def shutdown(): Unit = ???

  /**
    * It return whether the individual represents the empty pattern or not.
    *
    * @param individual
    * @return
    */
  override def isEmpty(individual: Solution[BinarySolution]): Boolean = {
    for(i <- 0 until individual.getNumberOfVariables){
      if(participates(individual,i)){
        return false
      }
    }
    return true

  }

  def isEmpty(individual: BinarySolution): Boolean = {
    for(i <- 0 until individual.getNumberOfVariables){
      if(participates(individual,i)){
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

    if(ind.getVariableValue(`var`).cardinality() > 0 && ind.getVariableValue(`var`).cardinality() < ind.getNumberOfBits(`var`)){
      true
    } else {
      false
    }
}

  def participates(individual: BinarySolution, `var`: Int): Boolean = {

    if(individual.getVariableValue(`var`).cardinality() > 0 && individual.getVariableValue(`var`).cardinality() < individual.getNumberOfBits(`var`)){
      true
    } else {
      false
    }
  }
}
