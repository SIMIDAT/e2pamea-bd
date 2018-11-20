import java.util
import java.util.concurrent.Callable

import evaluator.{EvaluatorIndDNF, EvaluatorMapReduce}
import main._
import operators.crossover.NPointCrossover
import operators.mutation.BiasedMutationDNF
import org.apache.avro.generic.GenericData
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.uma.jmetal.algorithm.multiobjective.nsgaii.NSGAIIBuilder
import org.uma.jmetal.operator.CrossoverOperator
import org.uma.jmetal.operator.impl.crossover.{HUXCrossover, SBXCrossover, TwoPointCrossover}
import org.uma.jmetal.operator.impl.mutation.{BitFlipMutation, PolynomialMutation}
import org.uma.jmetal.operator.impl.selection.BinaryTournamentSelection
import org.uma.jmetal.problem.BinaryProblem
import org.uma.jmetal.solution.{BinarySolution, DoubleSolution}
import org.uma.jmetal.util.comparator.{DominanceComparator, RankingAndCrowdingDistanceComparator}
import org.uma.jmetal.util.evaluator.impl.SequentialSolutionListEvaluator
import org.uma.jmetal.util.{AlgorithmRunner, ProblemUtils}
import org.apache.spark.sql.functions.{max, min}
import org.uma.jmetal.util.pseudorandom.JMetalRandom
import picocli.CommandLine
import picocli.CommandLine.{Command, Option, Parameters}
import qualitymeasures.{QualityMeasure, SuppDiff, WRAccNorm}
import utils.{ParametersParser, ResultWriter}

@Command(name = "spark-submit --master <URL> <jarfile>", version = Array("v1.0"),
  description = Array("@|bold \nFast Big Data MOEA\n|@"))
class Main extends Runnable{

  @Parameters(index = "0", paramLabel = "trainingFile", description = Array("The training file in ARFF format."))
  var trainingFile: String = null

  @Parameters(index = "1", paramLabel = "testFile", description = Array("The test file in ARFF format."))
  var testFile: String = null

  @Option(names = Array("-h", "--help"),  usageHelp = true, description = Array("Show this help message and exit."))
  var help = false

  @Option(names = Array("-V", "--version"),  versionHelp = true, description = Array("Print version information and exit."))
  var version = false

  @Option(names = Array("-s", "--seed"), paramLabel = "SEED", description = Array("The seed for the random number generator."))
  var seed : Int = 1

  @Option(names = Array("-p", "--partitions"), paramLabel = "NUMBER", description = Array("The number of partitions used within the MapReduce procedure"))
  var numPartitions = 4

  @Option(names = Array("-i", "--individuals"), paramLabel = "NUMBER", description = Array("The number of individuals in the population"))
  var popSize = 100

  @Option(names = Array("-e", "--evaluations"), paramLabel = "NUMBER", description = Array("The maximum number of evaluations until the end of the evolutionary process."))
  var maxEvals = 25000

  @Option(names = Array("-t", "--training"), paramLabel = "PATH", description = Array("The path for storing the training results file."))
  var resultTraining: String = null

  @Option(names = Array("-T", "--test"), paramLabel = "PATH", description = Array("The path for storing the test results file."))
  var resultTest: String = null

  @Option(names = Array("-r", "--rules"), paramLabel = "PATH", description = Array("The path for storing the rules file."))
  var resultRules: String = null

  @Option(names = Array("-o", "--objectives"), split = ",", paramLabel = "OBJ",
    description = Array("A comma-separated list of names for the quality measures to be used as optimisation objectives." ))
  var objectives : Array[String] = null

  @Option(names = Array("--list"), help=true, description = Array("List the quality measures available to be used as objectives."))
  var showMeasures = false

  @Option(names = Array("-l", "--labels"), paramLabel = "NUMBER", description = Array("The number of fuzzy linguistic labels for each variable."))
  var numLabels = 3

  override def run(): Unit = {
    if(help){
      new CommandLine(this).usage(System.err)
      return
    } else if(version){
      new CommandLine(this).printVersionHelp(System.err)
      return
    } else if(showMeasures){
      for( s <- utils.ClassLoader.getAvailableClasses)
      System.err.println(s)
      return
    }

    if(resultTraining == null) resultTraining = trainingFile + "_tra.txt"
    if(resultTest == null) resultTest = trainingFile + "_tst.txt"
    if(resultRules == null) resultRules = trainingFile + "_rules.txt"
    val objs =  if(objectives != null){
      val arr = new util.ArrayList[QualityMeasure]()
      for(s <- objectives){
       arr.add( Class.forName(classOf[QualityMeasure].getPackage.getName + "." + s).newInstance().asInstanceOf[QualityMeasure])
      }
      arr
    } else {
      val arr = new util.ArrayList[QualityMeasure]()
      arr.add(new WRAccNorm)
      arr.add(new SuppDiff)
      arr
    }

    // Se define el entorno de Spark
    val spark = SparkSession.builder.appName("Nuevo-MOEA").master("local[*]").config("spark.executor.memory", "3g").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    // Se elige el problema, esto se debe de ver como se le puede pasar los ficheros de keel o arff
    val problem = ProblemUtils.loadProblem[BinaryProblem]("main.BigDataEPMProblem").asInstanceOf[BigDataEPMProblem]

    println("Reading data...")
    problem.setNumLabels(numLabels)
    problem.readDataset(trainingFile, numPartitions, spark)
    problem.getAttributes(spark)
    problem.generateFuzzySets()

    problem.rand.setSeed(seed)


    // Se elige el evaluador
    val evaluador = new EvaluatorMapReduce()
    evaluador.setObjectives(objs)
    evaluador.setBigDataProcessing(false)


    // Se elige el crossover y sus parametros, en este caso, el crossover sbx
    val crossoverProbability: Double = 1.0
    val crossoverDistributionIndex: Double = 20.0
    val crossover = new NPointCrossover[BinarySolution](crossoverProbability, 2, problem.rand) //new HUXCrossover(crossoverProbability)

    // Operador de mutacion
    val mutationProbability: Double = 1.0
    val mutationDistributionIndex: Double = 20.0
    val mutation = new BiasedMutationDNF(mutationProbability, problem.rand) //new PolynomialMutation(mutationProbability, mutationDistributionIndex)

    // Operador de seleccion
    val selection = new BinaryTournamentSelection[BinarySolution](new RankingAndCrowdingDistanceComparator[BinarySolution])

    // Por defecto, el comparador de dominancia MINIMIZA los objetivos, hay que revertirlo para poder maximizar.
    val dominanceComparator = new DominanceComparator[BinarySolution]().reversed()

    // Se construye el algoritmo genetico con los datos que se han introducido.
    /*val algorithm = new NSGAIIBuilder[BinarySolution](problem, crossover.asInstanceOf[CrossoverOperator[BinarySolution]], mutation)
      .setSelectionOperator(selection)
      .setMaxEvaluations(25000)
      .setPopulationSize(100).setDominanceComparator(dominanceComparator)
      .setSolutionListEvaluator(evaluador)
      .build*/
    val algorithm = new NSGAIIModifiableBuilder[BinarySolution]
      .setProblem(problem)
      .setCrossoverOperator(crossover.asInstanceOf[CrossoverOperator[BinarySolution]])
      .setMutationOperator(mutation)
      .setSelectionOperator(selection)
      .setMaxEvaluations(maxEvals)
      .setPopulationSize(popSize)
      .setDominanceComparator(dominanceComparator)
      .setEvaluator(evaluador)
      .addOperator(new HUXCrossover(1, () => problem.rand.nextDouble()))
      .build()

    //val algorithm = new [BinarySolution](problem,25000,100,crossover,mutation, selection,new SequentialSolutionListEvaluator[BinarySolution]())

    // Ahora, se ejecuta el algoritmo genetico previamente creado.
    val algorithmRunner: AlgorithmRunner = new AlgorithmRunner.Executor(algorithm).execute

    // Una vez se ejecuta, se extraen los resultados.

    val population = algorithm.getResult
    val computingTime = algorithmRunner.getComputingTime

    println("Total training time: " + computingTime + " ms.\n.\n.\n.")

    //algorithm.getPopulation.forEach(println)


    /**
      * TEST PART
      */
    println("Initialising test. Reading data...")
    problem.readDataset(testFile, numPartitions, spark)
    evaluador.initialise(problem)
    evaluador.evaluateTest(population, problem)

    val writer = new ResultWriter(resultTraining,resultTest,"",resultRules, population, problem, objs, true)
    writer.writeRules()
    writer.writeTrainingMeasures()
    writer.writeTestFullResults()
  }
}

object Main{


  def main(args: Array[String]): Unit = {
    CommandLine.run(new Main(), System.err, args: _*)
  }


}
