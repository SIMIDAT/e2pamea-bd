import java.util

import evaluator.EvaluatorMapReduce
import main._
import operators.crossover.NPointCrossover
import operators.mutation.BiasedMutationDNF
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.uma.jmetal.operator.CrossoverOperator
import org.uma.jmetal.operator.impl.crossover.HUXCrossover
import org.uma.jmetal.operator.impl.selection.BinaryTournamentSelection
import org.uma.jmetal.problem.BinaryProblem
import org.uma.jmetal.solution.BinarySolution
import org.uma.jmetal.util.comparator.{DominanceComparator, RankingAndCrowdingDistanceComparator}
import org.uma.jmetal.util.{AlgorithmRunner, ProblemUtils}
import picocli.CommandLine
import picocli.CommandLine.{Command, Option, Parameters}
import qualitymeasures.{QualityMeasure, SuppDiff, WRAccNorm}
import utils.{Attribute, ResultWriter}

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

  @Option(names = Array("-v"), description = Array("Show Spark INFO messages."))
  var verbose = false

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
    //val spark = SparkSession.builder.appName("Nuevo-MOEA").master("local[*]").config("spark.executor.memory", "3g").getOrCreate()
    val conf = getConfig
    val spark = SparkSession.builder.config(conf).config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      // use this if you need to increment Kryo buffer size. Default 64k
      .config("spark.kryoserializer.buffer", "1024k")
      // use this if you need to increment Kryo buffer max size. Default 64m
      .config("spark.kryoserializer.buffer.max", "1024m").appName("Nuevo-MOEA").getOrCreate()


    if(!verbose) spark.sparkContext.setLogLevel("ERROR")

    // Se elige el problema, esto se debe de ver como se le puede pasar los ficheros de keel o arff
    val problem = ProblemUtils.loadProblem[BinaryProblem]("main.BigDataEPMProblem").asInstanceOf[BigDataEPMProblem]

    val t_ini = System.currentTimeMillis()
    println("Reading data...")
    problem.setNumLabels(numLabels)
    problem.readDataset(trainingFile, numPartitions, spark)
    problem.getAttributes(spark)
    problem.generateFuzzySets()

    problem.rand.setSeed(seed)
    var features: Int = 0
    val attrs: Array[Attribute] = problem.getAttributes
    for (i <- 0 until attrs.length){
      features += attrs(i).numValues
    }
    println("Number of features: " + features)


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
    val t_ini_test = System.currentTimeMillis()
    evaluador.initialise(problem)
    evaluador.evaluateTest(population, problem)
    println("Total test time: " + (System.currentTimeMillis() - t_ini_test) + " ms.")

    val writer = new ResultWriter(resultTraining,resultTest,"",resultRules, population, problem, objs, true)
    writer.writeRules()
    writer.writeTrainingMeasures()
    writer.writeTestFullResults()

    println("Total execution time: " + (System.currentTimeMillis() - t_ini) + " ms.")
  }

  private def getConfig = {
    val conf = new SparkConf()
    conf.registerKryoClasses(
      Array(
        classOf[scala.collection.mutable.WrappedArray.ofRef[_]],
        classOf[org.apache.spark.sql.types.StructType],
        classOf[Array[org.apache.spark.sql.types.StructType]],
        classOf[org.apache.spark.sql.types.StructField],
        classOf[Array[org.apache.spark.sql.types.StructField]],
        Class.forName("org.apache.spark.sql.types.StringType$"),
        Class.forName("org.apache.spark.sql.types.LongType$"),
        Class.forName("org.apache.spark.sql.types.BooleanType$"),
        Class.forName("org.apache.spark.sql.types.DoubleType$"),
        classOf[org.apache.spark.sql.types.Metadata],
        classOf[org.apache.spark.sql.types.ArrayType],
        Class.forName("org.apache.spark.sql.execution.joins.UnsafeHashedRelation"),
        classOf[org.apache.spark.sql.catalyst.InternalRow],
        classOf[Array[org.apache.spark.sql.catalyst.InternalRow]],
        classOf[org.apache.spark.sql.catalyst.expressions.UnsafeRow],
        Class.forName("org.apache.spark.sql.execution.joins.LongHashedRelation"),
        Class.forName("org.apache.spark.sql.execution.joins.LongToUnsafeRowMap"),
        classOf[utils.BitSet],
        classOf[org.apache.spark.sql.types.DataType],
        classOf[Array[org.apache.spark.sql.types.DataType]],
        Class.forName("org.apache.spark.sql.types.NullType$"),
        Class.forName("org.apache.spark.sql.types.IntegerType$"),
        Class.forName("org.apache.spark.sql.types.TimestampType$"),
        Class.forName("org.apache.spark.internal.io.FileCommitProtocol$TaskCommitMessage"),
        Class.forName("scala.collection.immutable.Set$EmptySet$"),
        Class.forName("java.lang.Class")
      )
    )
  }


}

object Main{


  def main(args: Array[String]): Unit = {
    CommandLine.run(new Main(), System.err, args: _*)
  }


}
