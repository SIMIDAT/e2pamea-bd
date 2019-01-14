package evaluator

import java.util
import java.util.logging.Logger

import attributes.{Clase, Coverage, DiversityMeasure, TestMeasures}
import exceptions.InvalidRangeInMeasureException
import main.BigDataEPMProblem
import org.apache.spark.rdd.RDD
import org.apache.spark.util.SizeEstimator
import org.uma.jmetal.problem.Problem
import org.uma.jmetal.solution.{BinarySolution, Solution}
import org.uma.jmetal.util.solutionattribute.impl.DominanceRanking
import qualitymeasures.{ContingencyTable, WRAccNorm}
import utils.{Attribute, BitSet}

import scala.collection.mutable.ArrayBuffer

/**
  * Class for the use of an improved DNF evaluator with MapReduce
  */
class EvaluatorMapReduce extends Evaluator[BinarySolution] {




  /**
    * The bitSets for the variables. They are distributed across the cluster.
    * Each row in the RDD represent a variable with different BitSets for each possible value
    */
  var bitSets: RDD[ArrayBuffer[Array[(Int, Int, BitSet)]]] = null


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
      val length = problema.getNumExamples
      classes = new ArrayBuffer[BitSet]()

      val t_ini = System.currentTimeMillis()
      for (i <- 0 until attrs.last.numValues) {
        classes += new BitSet(length.toInt)
      }


      if(this.bigDataProcessing){
        bitSets = initaliseBigData(problema, attrs)
      } else {
        sets = initialiseNonBigData(problema, attrs)
      }


      // Calculate the bitsets for the classes
      val numclasses = attrs.last.numValues
      classes = problema.getDataset.select("index", attrs.last.getName).rdd.mapPartitions(x => {
        val clase = new ArrayBuffer[BitSet]()
        for(i <- 0 until numclasses){
          clase += new BitSet(length)
        }
        x.foreach(y => {
          val index = y.getLong(0).toInt
          val name = y.getString(1)
          clase(attrs.last.nominalValue.indexOf(name)).set(index)
        })
        val aux = new Array[ArrayBuffer[BitSet]](1)
        aux(0) = clase
        aux.iterator
      }).treeReduce((x,y) => {
        for(i <- x.indices){
          x(i) = x(i) | y(i)
        }
        x
      })

      /*val columna = problema.getDataset.select(attrs.last.getName).collect()
      for (i <- columna.indices) {
        val name = columna(i).getString(0)
        classes(attrs.last.nominalValue.indexOf(name)).set(i)
      }*/

      //println("total sample: " + length + "  class 0: " + classes(0).cardinality() + "  class 1: " + classes(1).cardinality())
      super.setProblem(problema)
      if(bigDataProcessing){
        println("Pre-calculation time: " + (System.currentTimeMillis() - t_ini) + " ms. Size of Structure: " + (SizeEstimator.estimate(bitSets) / Math.pow(1000, 2)) + " MB.")
      } else {
        println("Pre-calculation time: " + (System.currentTimeMillis() - t_ini) + " ms. Size of Structure: " + (SizeEstimator.estimate(sets) / Math.pow(1000, 2)) + " MB.")

      }
      problema.getDataset.unpersist()

    }
  }






  override def evaluate(solutionList: util.List[BinarySolution], problem: Problem[BinarySolution]): util.List[BinarySolution] = {
    // In the map phase, it is returned an array of bitsets for each variable of the problem for all the individuals
    val t_ini = System.currentTimeMillis()


    val coverages = if (bigDataProcessing) {
      /*val tables =*/ calculateBigData(solutionList, problem)

      /*val orig = tables(0).getCoverage.load("bitset.ser")
      println(orig equals tables(0).getCoverage)


      for(i <- tables.indices){
        val cove = new Coverage[BinarySolution]()
        val ind = solutionList.get(i)
        val diversity = new DiversityMeasure[BinarySolution]()

        if(!isEmpty(ind)){
          val div = new WRAccNorm()
          div.calculateValue(tables(i))
          diversity.setAttribute(ind, div)
          tables(i).setAttribute(ind, tables(i))

          val objectives = super.calculateMeasures(tables(i))
          for (j <- 0 until objectives.size()) {
            ind.setObjective(j, objectives.get(j).getValue)
          }
          cove.setAttribute(ind, tables(i).getCoverage)
        } else {

          // If the rule is empty, set the fitness at minimum possible value for all the objectives.
          for (j <- 0 until ind.getNumberOfObjectives) {
            ind.setObjective(j, Double.NegativeInfinity)
          }
          val div = new WRAccNorm()
          val rank = new DominanceRanking[BinarySolution]()
          div.setValue(Double.NegativeInfinity)
          rank.setAttribute(ind, Integer.MAX_VALUE)
          cove.setAttribute(ind, new BitSet(problem.asInstanceOf[BigDataEPMProblem].getNumExamples))
          diversity.setAttribute(ind, div)
        }
      }*/

    } else {
      /*val coverages =*/ calculateCoveragesNonBigData(solutionList, problem)
    }

      /*println("Saving file...")
      coverages(0).saveToDisk("bitset.ser")
      println("Saved file to disk")
      System.exit(-1)*/

    val orig = new BitSet(0).load("bitset.ser")
    println(orig equals coverages(0))

      // Calculate the contingency table for each individual
      for (i <- coverages.indices) {
        val ind = solutionList.get(i)
        val cove = new Coverage[BinarySolution]()
        val diversity = new DiversityMeasure[BinarySolution]()

        if (!isEmpty(ind)) {
          cove.setAttribute(ind, coverages(i))
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
          val div = new WRAccNorm()
          div.calculateValue(table)
          diversity.setAttribute(ind, div)
          table.setAttribute(ind, table)

          val objectives = super.calculateMeasures(table)
          for (j <- 0 until objectives.size()) {
            ind.setObjective(j, objectives.get(j).getValue)
          }
        } else {
          // If the rule is empty, set the fitness at minimum possible value for all the objectives.
          for (j <- 0 until ind.getNumberOfObjectives) {
            ind.setObjective(j, Double.NegativeInfinity)
          }
          val div = new WRAccNorm()
          val rank = new DominanceRanking[BinarySolution]()
          div.setValue(Double.NegativeInfinity)
          rank.setAttribute(ind, Integer.MAX_VALUE)
          cove.setAttribute(ind, new BitSet(coverages(i).capacity))
          diversity.setAttribute(ind, div)
        }
      }


   // println("Time spent on the evaluation of 100 individuals after the precalculation: " + (System.currentTimeMillis() - t_ini) + " ms.")

    // Once we've got the coverage of the rules, we can calculate the contingecy tables.
    return solutionList
  }




  /**
    * It evaluates against the test data.
    * @param solutionList
    * @param problem
    * @return
    */
   def evaluateTest(solutionList: util.List[BinarySolution], problem: Problem[BinarySolution]): util.List[BinarySolution] = {
    // In the map phase, it is returned an array of bitsets for each variable of the problem for all the individuals
    val t_ini = System.currentTimeMillis()

    val coverages = if (bigDataProcessing) {
      calculateBigData(solutionList, problem)
    } else {
      calculateCoveragesNonBigData(solutionList, problem)
    }

      val table = new Array[ContingencyTable](coverages.length)
      for (i <- coverages.indices) {
        val ind = solutionList.get(i)
        val cove = new Coverage[BinarySolution]()
        val diversity = new DiversityMeasure[BinarySolution]()

        if (!isEmpty(ind)) {
          cove.setAttribute(ind, coverages(i))
          val clase = ind.getAttribute(classOf[Clase[BinarySolution]]).asInstanceOf[Int]

          // tp = covered AND belong to the class
          val tp = coverages(i) & classes(clase)

          // tn = NOT covered AND DO NOT belong to the class
          val tn = (~coverages(i)) & (~classes(clase))

          // fp = covered AND DO NOT belong to the class
          val fp = coverages(i) & (~classes(clase))

          // fn = NOT covered AND belong to the class
          val fn = (~coverages(i)) & classes(clase)

          table(i) = new ContingencyTable(tp.cardinality(), fp.cardinality(), tn.cardinality(), fn.cardinality(), coverages(i))
        } else {
          table(i) = new ContingencyTable(0, 0, 0, 0)
          cove.setAttribute(ind, new BitSet(coverages(i).capacity))
        }
      }




     for(i <- table.indices){
       val ind = solutionList.get(i)
       val cove = new Coverage[BinarySolution]()
       val div  = new WRAccNorm()
       div.calculateValue(table(i))
       val diversity = new DiversityMeasure[BinarySolution]()

       if(!isEmpty(ind)){
         val measures = utils.ClassLoader.getClasses

         for(q <- 0 until measures.size()){
           try{
             measures.get(q).calculateValue(table(i))
             measures.get(q).validate()

           } catch {
             case ex: InvalidRangeInMeasureException =>
               System.err.println("Error while evaluating Individuals in test: ")
               ex.showAndExit(this)
           }
         }
         val test = new TestMeasures[BinarySolution]()
         test.setAttribute(ind, measures)
         table(i).setAttribute(ind, table(i))
         diversity.setAttribute(ind, div)

       } else {
         // If the rule is empty, set the fitness at minimum possible value for all the objectives.
         for (j <- 0 until ind.getNumberOfObjectives) {
           ind.setObjective(j, Double.NegativeInfinity)
         }
         val div = new WRAccNorm()
         val rank = new DominanceRanking[BinarySolution]()
         div.setValue(Double.NegativeInfinity)
         rank.setAttribute(ind, Integer.MAX_VALUE)
         //cove.setAttribute(ind, new BitSet(coverages(i).capacity))
         diversity.setAttribute(ind, div)
       }
     }

    // println("Time spent on the evaluation of 100 individuals after the precalculation: " + (System.currentTimeMillis() - t_ini) + " ms.")

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
    * This function is much faster than Big Data one when the number of variables is LOW.
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

      if(!isEmpty(ind)) {
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
              coverages(i) = coverageForVariable | coverages(i)
              first = false
            } else {
              coverages(i) = coverages(i) & coverageForVariable
            }
          }
        }
      } else {
        coverages(i) = new BitSet(sets(0)(0).capacity)
      }
    }

    coverages
  }


  /**
    * It calculates the coverages bit sets for each individual in the population by means of performing the operations in
    * a distributed RDD structure stored in {@code bitSets}.
    *
    * This function is much faster than local one when the number of variables is VERY HIGH.
    *
    * @param solutionList
    * @param problem
    * @return
    */
  def calculateBigData(solutionList: util.List[BinarySolution], problem: Problem[BinarySolution]): Array[BitSet] = {

    val numExamples = problem.asInstanceOf[BigDataEPMProblem].getNumExamples
    bitSets.mapPartitions(x => {
      val bits = x.map(y => {
        //val index = y._2.toInt
        //val sets = y._1
        val min = y(0)(0)._1
        val max = y(0)(0)._2
        var popCoverage = new BitSet(max - min)

        val coverages = new Array[BitSet](solutionList.size())
        for (i <- coverages.indices) {
          coverages(i) = new BitSet(max - min)//new BitSet(problem.asInstanceOf[BigDataEPMProblem].numExamples)
        }
        val tables = new Array[BitSet](solutionList.size())

        for (i <- 0 until solutionList.size()) {
          // for each individual
          val ind = solutionList.get(i)
          val clase = ind.getAttribute(classOf[Clase[BinarySolution]]).asInstanceOf[Int]
          if(!isEmpty(ind)) {
            var first = true
            for (j <- 0 until ind.getNumberOfVariables) {
              // for each variable
              if (participates(ind, j)) {
                // Perform OR operations between active elements in the DNF rule.
                var coverageForVariable = new BitSet(max - min)
                for (k <- 0 until ind.getNumberOfBits(j)) {
                  if (ind.getVariableValue(j).get(k)) {
                    coverageForVariable = coverageForVariable | y(j)(k)._3
                  }
                }
                // after that, perform the AND operation
                if (first) {
                  coverages(i) = coverageForVariable | coverages(i)
                  first = false
                } else {
                  coverages(i) = coverages(i) & coverageForVariable
                }
              }
            }
          } else {
            coverages(i) = new BitSet(y(0)(0)._3.capacity)
          }
          // tp = covered AND belong to the class
          //val tp2 = coverages(i) & classes(clase).get(min, max)

          // Add to the bitSet zeros before min and max in order to have a full-length BitSet.
          // This allows us to perform OR operations on the reduce for the final coverage of the individual
          if(min > 0)
            coverages(i) = new BitSet(min).concatenate(min , coverages(i), max - min).get(0,max)//.concatenate(max +1  , new BitSet(numExamples - (max + 1)), numExamples - (max + 1))


          //tables(i) = new ContingencyTable(0, 0, 0, 0, coverages(i))
          tables(i) = coverages(i)

          //popCoverage = popCoverage | coverages(i)
        }

        // Hay que enviar si o si las coberturas en vez de las matrice de confusión para que se añadan a los inds. y puedan ser usadas en a reinicializacion
        // DEBES PROBAR A USAR UN ACCUMULADOR en un bitset que use represente la cobertura total de la población.
        // Luego, puedes usar token competition también en paralelo.

        tables

      })
      bits
    }).treeReduce((x, y) => {
      val tables = new Array[BitSet](x.length)

      for(i <- x.indices){
        //val clase = solutionList.get(i).getAttribute(classOf[Clase[BinarySolution]]).asInstanceOf[Int]
        /*val tp = x(i).getTp + y(i).getTp
        val fp = x(i).getFp + y(i).getFp
        val tn = x(i).getTn + y(i).getTn
        val fn = x(i).getFn + y(i).getFn*/
        val cove = x(i) | y(i)

        //val tp = (cove & classes(clase)).cardinality()

        // tn = NOT covered AND DO NOT belong to the class
        //val tn = ((~cove) & (~classes(clase))).cardinality()

        // fp = covered AND DO NOT belong to the class
        //val fp = (cove & (~classes(clase))).cardinality()

        // fn = NOT covered AND belong to the class
        //val fn = ((~cove) & classes(clase)).cardinality()

        //tables(i) = new ContingencyTable(0,0,0,0, cove)
        tables(i) = cove
      }
      tables
    }, Evaluator.TREE_REDUCE_DEPTH)

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
    val tope = 10E-12

    var max = Double.NegativeInfinity

    for(i <- 0 until problema.getAttributes(variable).numValues){
      val a = problema.calculateBelongingDegree(variable, i, x)
      if(a > max){
        max = a
      }
    }

    max - tope
  }


  /**
    * It initialises the BitSets structure for the {@code sets}. It store the coverage of each possible feature (var-value pair)
    * in a local structure, i.e., in the driver.
    *
    *
    * @param problema
    * @param attrs
    * @return
    */
  def initialiseNonBigData(problema: BigDataEPMProblem, attrs: Array[Attribute]): ArrayBuffer[ArrayBuffer[BitSet]] = {
    // Calculate the bitsets for the variables
    problema.getDataset.rdd.mapPartitions(x => {
      var min = Int.MaxValue
      var max = -1
      // Instatiate the whole bitsets structure for each partition with length equal to the length of data
      val partialSet = new ArrayBuffer[ArrayBuffer[BitSet]]()
      for (i <- 0 until (attrs.length - 1)) {
        partialSet += new ArrayBuffer[BitSet]()
        for (j <- 0 until attrs(i).numValues) {
          partialSet(i) += new BitSet(0)
        }
      }

      var counter = 0
      x.foreach(f = y => {
        val index = y.getLong(0).toInt
        if(index < min) min = index
        if(index > max) max = index

        for (i <- attrs.indices.dropRight(1)) {
          val ind = i + 1
          // For each attribute
          for (j <- 0 until attrs(i).numValues) {
            // for each label/value
            if (attrs(i).isNumeric) {
              // Numeric variable, fuzzy computation
              if (y.isNullAt(ind)) {
                partialSet(i)(j).set(counter)
              } else {
                if (problema.calculateBelongingDegree(i, j, y.getDouble(ind)) >= getMaxBelongingDegree(problema, i, y.getDouble(ind))) {
                  partialSet(i)(j).set(counter)
                } else {
                  partialSet(i)(j).unset(counter)
                }
              }
            } else {
              // Discrete variable
              if (y.isNullAt(ind)) {
                partialSet(i)(j).set(counter)
              } else {
                if (attrs(i).valueName(j).equals(y.getString(ind))) {
                  partialSet(i)(j).set(counter)
                } else {
                  partialSet(i)(j).unset(counter)
                }
              }
            }
          }
        }
        counter += 1
      })
      val aux = new Array[(Int, Int, ArrayBuffer[ArrayBuffer[BitSet]]) ](1)
      aux(0) = (min, max, partialSet)
      aux.iterator
    }).treeReduce((x, y) => {
      val min = Array(x._1, y._1).min
      val max = Array(x._2, y._2).max


      for (i <- x._3.indices) {
        for (j <- x._3(i).indices) {
          var pos = -1
          val Bs = new BitSet(max - min)
          while (x._3(i)(j).nextSetBit(pos + 1) >= 0){
            pos = x._3(i)(j).nextSetBit(pos + 1)
            Bs.set(pos + x._1 - min)
          }
          pos = -1
          while (y._3(i)(j).nextSetBit(pos + 1) >= 0){
            pos = y._3(i)(j).nextSetBit(pos + 1)
            Bs.set(pos + y._1 - min)
          }
          x._3(i)(j) = Bs
        }
      }

      (min, max, x._3)
    }, 6)._3

  }


  def initaliseBigData(problema: BigDataEPMProblem, attrs: Array[Attribute]): RDD[ArrayBuffer[Array[(Int, Int, BitSet)]]] = {

    problema.getDataset.rdd.mapPartitions(x => {
      var min = Int.MaxValue
      var max = -1
      // Instatiate the whole bitsets structure for each partition with length equal to the length of data
      val partialSet = new ArrayBuffer[Array[(Int, Int, BitSet)]]()
      for (i <- 0 until (attrs.length - 1)) {
        partialSet += new Array[(Int, Int, BitSet)](attrs(i).numValues)
        for (j <- 0 until attrs(i).numValues) {
          partialSet(i)(j) = (min,max,new BitSet(0))
        }
      }

      var counter = 0
      x.foreach(f = y => {
        val index = y.getLong(0).toInt
        if(index < min) min = index
        if(index > max) max = index

        for (i <- attrs.indices.dropRight(1)) {
          val ind = i + 1
          // For each attribute
          for (j <- 0 until attrs(i).numValues) {
            // for each label/value
            if (attrs(i).isNumeric) {
              // Numeric variable, fuzzy computation
              if (y.isNullAt(ind)) {
                partialSet(i)(j)._3.set(counter)
              } else {
                if (problema.calculateBelongingDegree(i, j, y.getDouble(ind)) >= getMaxBelongingDegree(problema, i, y.getDouble(ind))) {
                  partialSet(i)(j)._3.set(counter)
                } else {
                  partialSet(i)(j)._3.unset(counter)
                }
              }
            } else {
              // Discrete variable
              if (y.isNullAt(ind)) {
                partialSet(i)(j)._3.set(counter)
              } else {
                if (attrs(i).valueName(j).equals(y.getString(ind))) {
                  partialSet(i)(j)._3.set(counter)
                } else {
                  partialSet(i)(j)._3.unset(counter)
                }
              }
            }
          }
        }
        counter += 1
      })
     for(i <- partialSet.indices){
       for(j <- partialSet(i).indices){
         partialSet(i)(j) = partialSet(i)(j).copy(_1 = min, _2 = max)
       }
     }
      val aux = new Array[ArrayBuffer[Array[(Int, Int, BitSet)]]](1)
      aux(0) = partialSet
      aux.iterator
    }, true).cache()

  }


}
