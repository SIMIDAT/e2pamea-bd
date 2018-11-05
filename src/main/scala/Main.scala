import evaluator.EvaluatorIndDNF
import main._
import operators.crossover.NPointCrossover
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


object Main {

  def main(args: Array[String]): Unit = {

    // Se define el entorno de Spark
    val spark = SparkSession.builder.appName("Nuevo-MOEA").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    // Se elige el problema, esto se debe de ver como se le puede pasar los ficheros de keel o arff
    val problem = ProblemUtils.loadProblem[BinaryProblem]("main.EPMProblem").asInstanceOf[EPMProblem]

    problem.readDataset("Air.arff", 4, spark)
    problem.getAttributes(spark)
    problem.generateFuzzySets()


    // Se elige el evaluador
    val evaluador = new EvaluatorIndDNF(problem)


    // Se elige el crossover y sus parametros, en este caso, el crossover sbx
    val crossoverProbability: Double = 0.9
    val crossoverDistributionIndex: Double = 20.0
    val crossover = new NPointCrossover[BinarySolution](crossoverProbability, 2) //new HUXCrossover(crossoverProbability)

    // Operador de mutacion
    val mutationProbability: Double = 1.0 / problem.getNumberOfVariables
    val mutationDistributionIndex: Double = 20.0
    val mutation = new BiasedMutationDNF(mutationProbability) //new PolynomialMutation(mutationProbability, mutationDistributionIndex)

    // Operador de seleccion
    val selection = new BinaryTournamentSelection[BinarySolution](new RankingAndCrowdingDistanceComparator[BinarySolution])

    // Por defecto, el comparador de dominancia MINIMIZA los objetivos, hay que revertirlo para poder maximizar.
    val dominanceComparator = new DominanceComparator[BinarySolution]().reversed()

    // Se construye el algoritmo genetico con los datos que se han introducido.
    val algorithm = new NSGAIIBuilder[BinarySolution](problem, crossover.asInstanceOf[CrossoverOperator[BinarySolution]], mutation)
      .setSelectionOperator(selection)
      .setMaxEvaluations(25000)
      .setPopulationSize(100).setDominanceComparator(dominanceComparator)
      .setSolutionListEvaluator(evaluador)
      .build

    //val algorithm = new [BinarySolution](problem,25000,100,crossover,mutation, selection,new SequentialSolutionListEvaluator[BinarySolution]())

    // Ahora, se ejecuta el algoritmo genetico previamente creado.
    val algorithmRunner: AlgorithmRunner = new AlgorithmRunner.Executor(algorithm).execute

    // Una vez se ejecuta, se extraen los resultados.

    val population = algorithm.getResult
    val computingTime = algorithmRunner.getComputingTime

    println("Total execution time: " + computingTime + "ms")

    //algorithm.getPopulation.forEach(println)

  }




}
