package main

import java.util
import java.util.ArrayList

import fuzzy.{Fuzzy, TriangularFuzzySet}
import org.apache.spark.sql.functions.{max, min}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.util.collection.BitSet
import org.uma.jmetal.problem.BinaryProblem
import org.uma.jmetal.solution.BinarySolution
import org.uma.jmetal.solution.impl.DefaultBinarySolution
import org.uma.jmetal.util.pseudorandom.JMetalRandom
import org.uma.jmetal.util.solutionattribute.impl.{NumberOfViolatedConstraints, OverallConstraintViolation}
import weka.core.Instances

import scala.collection.mutable.ArrayBuffer

/**
  * Class that define an Emerging pattern mining problem in JMetal
  */
class EPMProblem extends BinaryProblem{

  val RANDOM_INITIALISATION: Int = 0
  val ORIENTED_INITIALISATION: Int = 1

  /**
    * The dataset. It contains all the information about the instances
    */
  private var dataset: DataFrame = null

  private var attributes: Array[Attribute] = null

  /**
    * The class of the problem for the extraction of rules.
    */
  private var clase: Clase[BinarySolution] = null
  private var clas: Int = 0


  /**
    * The number of objectives to be used in a MOEA algorithm.
    */
  private var numObjectives: Int = 2


  /**
    * The number of linguistic labels to be used in numeric variables
    */
  private var numLabels: Int = 3


  /**
    * The initialisation method used
    */
  private var initialisationMethod: Int = 0

  /**
    * The seed of the random number generator
    */
  private var seed: Int = 0

  /**
    * The fuzzy sets stored for each numeric variable
    */
  private var fuzzySets: ArrayBuffer[ArrayBuffer[Fuzzy]] = null


  /**
    * The number of violated constraints in the problem
    */
  var numberOfViolatedConstraints: NumberOfViolatedConstraints[BinarySolution] = null

  /**
    * The overall constraint violation
    */
  var overallConstraintViolation: OverallConstraintViolation[BinarySolution] = null

  /**
    * The random number generator
    */
  val rand: JMetalRandom = JMetalRandom.getInstance()


  override def getNumberOfBits(index: Int): Int = {
    if(attributes(index).isNumeric){
      numLabels
    } else {
      attributes(index).numValues
    }
  }

  override def getTotalNumberOfBits: Int = {
    var suma = 0
    for(i <- 0 until attributes.length){
      suma += getNumberOfBits(i)
    }
    suma
  }

  override def getNumberOfVariables: Int = attributes.length - 1

  override def getNumberOfObjectives: Int = numObjectives

  override def getNumberOfConstraints: Int = 0

  override def getName: String = "Emerging Pattern Mining extraction for Big Data"

  override def evaluate(solution: BinarySolution): Unit = ???

  override def createSolution(): BinarySolution = {
    // Create a random individual
    val sol = if (initialisationMethod == RANDOM_INITIALISATION) { // By default, individuals are initialised at random
      new DefaultBinarySolution(this)
    }
    else if (initialisationMethod == ORIENTED_INITIALISATION) { // Oriented initialisation
      OrientedInitialisation(rand, 0.25)
    }

    clase.setAttribute(sol, clas)
    return sol
  }


  /**
    * It reads a dataset and it stores it in the dataset field
    * @param path
    * @param numPartitions
    * @param spark
    * @return
    */
  def readDataset(path: String, numPartitions: Int, spark: SparkSession): Unit = {
    val listValues = spark.sparkContext.textFile(path)
      .filter(x => x.startsWith("@"))
      .filter(x => !x.startsWith("@relation"))
      .filter(x => !x.startsWith("@data")).map(x => {
      val values = x.split(" ")
      if (values(2).equalsIgnoreCase("numeric")) {
        StructField(values(1), DoubleType, true)
      } else {
        StructField(values(1), StringType, true)
      }
    }).collect()

    val schema = StructType(listValues)

    dataset = spark.read.option("header", "false").option("comment", "@").schema(schema).csv(path)
    dataset.cache()
  }



  /**
    * It returns an array with the attribute definitions
    *
    * @param data
    * @return
    */
  def getAttributes(spark: SparkSession): Unit = {
    import spark.implicits._

    attributes = dataset.columns.map(x => {
      val nominal = dataset.schema(x).dataType match {
        case StringType => true
        case DoubleType => false
      }

      val attr = if (nominal) {
        val values = dataset.select(x).distinct().map(x => x.getString(0)).collect()
        new Attribute(x, nominal,0,0,values.toList)
      } else {
        val a = dataset.select(min(x), max(x)).head()
        new Attribute(x, nominal, a.getDouble(0), a.getDouble(1), null)
      }
      attr
    })

  }


  /**
    * It generates the fuzzy sets definitions for numeric variables
    *
    */
  def generateFuzzySets(): Unit ={
    if(dataset != null && attributes != null){
      fuzzySets = new ArrayBuffer[ArrayBuffer[Fuzzy]]()
      for(attr <- attributes){
        if(attr.isNominal){
          fuzzySets += null
        } else {
          fuzzySets += generateLinguistcLabels(attr.getMin, attr.getMax)
        }
      }

    }
  }


  /**
    * It generates the triangular linguistic fuzzy labels for a numeric variable
    *
    * @param min
    * @param max
    * @return
    */
  private def generateLinguistcLabels(min: Double, max: Double) = {
    val marca = (max - min) / (numLabels - 1).toDouble
    var cutPoint = min + marca / 2
    val sets = new ArrayBuffer[Fuzzy]

    for(label <- 0 until numLabels){
      val definitions = new ArrayBuffer[Double]
      var value = min + marca * (label - 1)
      // Creation of x0 point
      if (label == 0)
        definitions += -1 * Double.MaxValue
      else
        definitions += Round(value, max)
      // Creation of x1 point
      value = min + marca * label
      definitions += Round(value, max)
      // Creation of x2 point
      value = min + marca * (label + 1)
      if (label == numLabels - 1)
        definitions += Double.MaxValue
      else
        definitions += Round(value, max)
      // Create de triangular fuzzy set
      val set = new TriangularFuzzySet(definitions, 1.0)
      sets += set
      cutPoint += marca
      }

    sets
  }


  /**
    * <p>
    * Rounds the generated value for the semantics when necesary
    * </p>
    *
    * @param val The value to round
    * @param tope
    * @return
    */
  def Round(`val`: Double, tope: Double): Double = {
    if (`val` > -0.0001 && `val` < 0.0001) return 0
    if (`val` > tope - 0.0001 && `val` < tope + 0.0001) return tope
    `val`
  }



  /**
    * It generates a random indivual initialising a percentage of its variables at random.
    * @param rand
    * @return
    */
  def OrientedInitialisation(pctVariables: Double): DefaultBinarySolution = {
    val sol = new DefaultBinarySolution(this)
    val maxVariablesToInitialise = Math.round(pctVariables * (attributes.length - 1))
    val varsToInit = rand.nextInt(1, maxVariablesToInitialise.toInt + 1)

    val initialised = new BitSet(attributes.length -1)
    var varInitialised = 0

    while(varInitialised != varsToInit){
      val value = rand.nextInt(0 , attributes.length - 1)
      if (!initialised.get(var)){
        BinarySet value = new BinarySet(sol.getNumberOfBits(var));
        for(int i = 0; i < sol.getNumberOfBits(var);i++){
          if(rand.nextBoolean())
            value.set(i);
          else
            value.clear(i);
        }
        // check if the generated variable is empty and fix it if necessary
        if(value.cardinality() == 0){
          value.set(rand.nextInt(sol.getNumberOfBits(var)));
        } else  if(value.cardinality() == sol.getNumberOfBits(var)){
          value.clear(rand.nextInt(sol.getNumberOfBits(var)));
        }
        sol.setVariableValue(var,value);
        varInitialised++;
        initialised.set(var);
      }
    }

    sol.getVariableValue(dataset.classIndex()).clear();
    sol.getVariableValue(dataset.classIndex()).set(rand.nextInt(dataset.numClasses()));

    // clear the non-initialised variables
    for(int i = 0; i < sol.getNumberOfVariables(); i++){
      if(!initialised.get(i)){
        sol.getVariableValue(i).clear();
      }
    }

    return sol;
  }

}
