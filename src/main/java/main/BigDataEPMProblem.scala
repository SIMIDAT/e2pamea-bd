package main

import attributes.Clase
import fuzzy.{DecreasingLineFuzzySet, Fuzzy, IncreasingLineFuzzySet, TriangularFuzzySet}
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.util.collection.BitSet
import org.uma.jmetal.problem.BinaryProblem
import org.uma.jmetal.solution.BinarySolution
import org.uma.jmetal.solution.impl.DefaultBinarySolution
import org.uma.jmetal.util.binarySet.BinarySet
import org.uma.jmetal.util.pseudorandom.JMetalRandom
import org.uma.jmetal.util.pseudorandom.impl.JavaRandomGenerator
import org.uma.jmetal.util.solutionattribute.impl.{NumberOfViolatedConstraints, OverallConstraintViolation}
import utils.Attribute

import scala.collection.mutable.ArrayBuffer

/**
  * Class that define a Big Data Emerging pattern mining problem in JMetal framework
  *
  */
class BigDataEPMProblem extends BinaryProblem{


  val RANDOM_INITIALISATION: Int = 0
  val ORIENTED_INITIALISATION: Int = 1
  val COVERAGE_INITIALISATION: Int = 2

  /**
    * The dataset. It contains all the information about the instances
    */
  private var dataset: DataFrame = null

  /**
    * The attributes information
    */
  private var attributes: Array[Attribute] = null

  /**
    * The class of the problem for the extraction of rules.
    */
  private var clase: Clase[BinarySolution] = new Clase[BinarySolution]()
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
  private var initialisationMethod: Int = 1

  /**
    * The seed of the random number generator
    */
  private var seed: Int = 1

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
  var rand: JMetalRandom = null
  var rnd = new JavaRandomGenerator(seed)

  /**
    * The Spark Session employed in this problem
    */
  var spark: SparkSession = null

  /**
    * The number of examples in the dataset
    */
  var numExamples: Int = 0

  /**
    * The number of partitions in the dataset
    */
  private var numPartitions: Int = 0

  /**
    * The default comment char for the ARFF reading as CSV
    */
  private var commentChar = "@"

  /**
    * The null value string
    */
  private var nullValue = "?"



  def setRandomGenerator(generator : JMetalRandom) = {
    rand = generator
  }

  def setNullValue(value: String): Unit = {nullValue = value}

  /**
    * Get the corresponding fuzzy set j for the variable i
    * @param i
    * @param j
    * @return
    */
  def getFuzzySet(i: Int, j: Int): Fuzzy = {
    fuzzySets(i)(j)
  }

  /**
    * It returns the belonging degree of the value x to the fuzzy set j of the variable i
    * @param i
    * @param j
    * @param x
    */
  def calculateBelongingDegree(i: Int, j: Int, x: Double): Double ={
    fuzzySets(i)(j).getBelongingDegree(x)
  }

  /**
    * Sets the given fuzzy set for in the position j for the variable i.
    * @param i
    * @param j
    * @param set
    */
  def setFuzzySet(i: Int, j: Int, set: Fuzzy): Unit ={
    fuzzySets(i)(j) = set
  }

  def getDataset: DataFrame = dataset

  def getAttributes: Array[Attribute] = attributes

  def getNumPartitions(): Int = numPartitions

  def getNumLabels(): Int = numLabels

  def setNumLabels(labels: Int) = {numLabels = labels}

  def getInitialisationMethod(): Int = initialisationMethod

  def getNumExamples: Int  = numExamples

  def setInitialisationMethod(method: Int) = initialisationMethod = method

  def setSparkSession(sp: SparkSession) = {
    this.spark = sp
  }

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
    val sol: BinarySolution = if (initialisationMethod == RANDOM_INITIALISATION) { // By default, individuals are initialised at random
      new DefaultBinarySolution(this)
    } else if (initialisationMethod == ORIENTED_INITIALISATION) { // Oriented initialisation
      OrientedInitialisation(0.25)
    } else if (initialisationMethod == COVERAGE_INITIALISATION){
      // Get the pairs that cover that example.
      // Get the pairs that cover that example.
      /*val pairs : ArrayBuffer[(Int, Int)] = new ArrayBuffer[(Int, Int)]()
      for(i <- eval.sets.indices){
        // for each variable
        for(j <- eval.sets(i).indices){
          // for each value
          if(eval.sets(i)(j).get(example)){
            val add = (i,j)
            pairs += add
          }
        }
      }*/
      new DefaultBinarySolution(this)
    } else {
      new DefaultBinarySolution(this)
    }

    clase.setAttribute(sol, clas)

    // The next individual belongs to a different class (last attribute), this is for ensuring we have individuals for all classes.
    clas = (clas + 1) % attributes.last.numValues

    return sol
  }

  def createSolution(sets: ArrayBuffer[(Int, Int)], pctVariables: Double): BinarySolution = {

    val sol = new DefaultBinarySolution(this)
    val maxVariablesToInitialise = Math.ceil(pctVariables * sets.length)
    val varsToInit = rand.nextInt(1, maxVariablesToInitialise.toInt + 1)

    val initialised = new BitSet(sets.length)
    var varInitialised = 0

    while(varInitialised != varsToInit){
      val value = rand.nextInt(0 , sets.length - 1) // la variable sets corresponde a los pares var,value que cubren al ejemplo.
      if (!initialised.get(value)){
        val set = new BinarySet(sol.getNumberOfBits(sets(value)._1))
        set.set(sets(value)._2)

        // check if the generated variable is empty and fix it if necessary
        if(set.cardinality() == 0){
          set.set(rand.nextInt(0, sol.getNumberOfBits(sets(value)._1 ) - 1))
        } else  if(set.cardinality() == sol.getNumberOfBits(sets(value)._1)){
          set.clear(rand.nextInt(0, sol.getNumberOfBits(sets(value)._1) - 1))
        }

        sol.setVariableValue(sets(value)._1,set)
        varInitialised += 1
        initialised.set(value)
      }
    }

    // clear the non-initialised variables
    for(i <- 0 until getNumberOfVariables){
      if(!initialised.get(i)){
        sol.getVariableValue(i).clear()
      }
    }

    return sol
  }


  def getNumberOfClasses: Int = {attributes.last.numValues}

  /**
    * It reads a dataset and it stores it in the dataset field
    * @param path
    * @param numPartitions
    * @param spark
    * @return
    */
  def readDataset(path: String, numPartitions: Int, spark: SparkSession): Unit = {
    this.spark = spark
    this.numPartitions = numPartitions
    val listValues = spark.sparkContext.textFile(path, this.numPartitions)
      .filter(x => x.startsWith("@"))
      .filter(x => !x.toLowerCase.startsWith("@relation"))
      .filter(x => !x.toLowerCase.startsWith("@data"))
      .filter(x => !x.toLowerCase.startsWith("@inputs"))
      .filter(x => !x.toLowerCase.startsWith("@outputs"))
      .map(x => {
      val values = x.split("(\\s*)(\\{)(\\s*)|(\\s*)(\\})(\\s*)|(\\s*)(\\[)(\\s*)|(\\s*)(\\])(\\s*)|(\\s*)(,)(\\s*)|\\s+")
      if (values(2).equalsIgnoreCase("numeric") | values(2).equalsIgnoreCase("real") | values(2).equalsIgnoreCase("integer")) {
        StructField(values(1), DoubleType, true)
      } else {
        StructField(values(1), StringType, true)
      }
    }).coalesce(this.numPartitions).collect()

    val schema = StructType(listValues)


      dataset = spark.read.option("header", "false")
        .option("comment", "@")
        .option("nullValue", nullValue)
        .option("mode", "FAILFAST") // This mode throws an error on any malformed line is encountered
        .schema(schema).csv(path).coalesce(this.numPartitions)


    dataset = zipWithIndex(dataset,0)

    numExamples = dataset.count().toInt

  }



  /**
    * It returns an array with the attribute definitions
    *
    * @param data
    * @return
    */
  def getAttributes(spark: SparkSession): Unit = {
    import spark.implicits._

    attributes = dataset.columns.drop(1).map(x => {
      val nominal = dataset.schema(x).dataType match {
        case StringType => true
        case DoubleType => false
      }

      val attr = if (nominal) {
        val values = dataset.select(x).distinct().map(x => x.getString(0)).collect()
        new Attribute(x, nominal,0,0,0,values.toList)
      } else {
        val a = dataset.select(min(x), max(x)).head()
        new Attribute(x, nominal, a.getDouble(0), a.getDouble(1), numLabels, null)
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
        definitions += min
      else
        definitions += Round(value, max)
      // Creation of x1 point
      value = min + marca * label
      definitions += Round(value, max)
      // Creation of x2 point
      value = min + marca * (label + 1)
      if (label == numLabels - 1)
        definitions += max
      else
        definitions += Round(value, max)
      // Create de triangular fuzzy set
      val set = if(label == 0){
        definitions.remove(0)
        new DecreasingLineFuzzySet(definitions, 1.0)
      } else if(label == numLabels - 1){
        definitions.remove(definitions.size -1)
        new IncreasingLineFuzzySet(definitions, 1.0)
      } else {
        new TriangularFuzzySet(definitions, 1.0)
      }

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
    val maxVariablesToInitialise = Math.round(pctVariables * getNumberOfVariables)
    val varsToInit = rand.nextInt(1, maxVariablesToInitialise.toInt)
    val initialised = new BitSet(getNumberOfVariables)
    var varInitialised = 0

    while(varInitialised != varsToInit){
      val value = rand.nextInt(0 , getNumberOfVariables - 1)
      if (!initialised.get(value)){
        val set = new BinarySet(sol.getNumberOfBits(value))
        for(i <- 0 until sol.getNumberOfBits(value)){
          if(rand.nextDouble(0.0, 1.0) <= 0.5)
            set.set(i)
          else
            set.clear(i)
        }
        // check if the generated variable is empty and fix it if necessary
        if(set.cardinality() == 0){
          set.set(rand.nextInt(0, sol.getNumberOfBits(value) - 1))
        } else  if(set.cardinality() == sol.getNumberOfBits(value)){
          set.clear(rand.nextInt(0, sol.getNumberOfBits(value) - 1))
        }

        sol.setVariableValue(value,set)
        varInitialised += 1
        initialised.set(value)
      }
    }

    // clear the non-initialised variables
    for(i <- 0 until getNumberOfVariables){
      if(!initialised.get(i)){
        sol.getVariableValue(i).clear()
      }
    }

    return sol
  }


  /**
    * zipWithIndex: it adds a column in the data frame that corresponds the index of that element
    * @param df
    * @param offset
    * @param indexName
    * @return
    */
  def zipWithIndex(df: DataFrame, offset: Long = 1, indexName: String = "index") = {
    val columnNames = Array(indexName) ++ df.columns
    //df.repartition(this.numPartitions)
    val dfWithPartitionId = df.withColumn("partition_id", spark_partition_id()).withColumn("inc_id", monotonically_increasing_id())

    val partitionOffsets = dfWithPartitionId
      .groupBy("partition_id")
      .agg(count(lit(1)) as "cnt", first("inc_id") as "inc_id")
      .orderBy("partition_id")
      .select(sum("cnt").over(Window.orderBy("partition_id")) - col("cnt") - col("inc_id") + lit(offset) as "cnt" )
      .collect()
      .map(_.getLong(0))
      .toArray

    dfWithPartitionId
      .withColumn("partition_offset", udf((partitionId: Int) => partitionOffsets(partitionId), LongType)(col("partition_id")))
      .withColumn(indexName, col("partition_offset") + col("inc_id"))
      .drop("partition_id", "partition_offset", "inc_id")
      .select(columnNames.head, columnNames.tail: _*).cache()
  }



  def showDataset() = {
    this.dataset.show()
  }
}
