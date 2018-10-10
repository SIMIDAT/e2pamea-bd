import main.{IndDNF, NSGAIIModifiable, Problema}
import org.uma.jmetal.algorithm.multiobjective.nsgaii.NSGAIIBuilder
import org.uma.jmetal.operator.impl.crossover.{HUXCrossover, SBXCrossover}
import org.uma.jmetal.operator.impl.mutation.{BitFlipMutation, PolynomialMutation}
import org.uma.jmetal.operator.impl.selection.BinaryTournamentSelection
import org.uma.jmetal.problem.BinaryProblem
import org.uma.jmetal.solution.{BinarySolution, DoubleSolution}
import org.uma.jmetal.util.comparator.{DominanceComparator, RankingAndCrowdingDistanceComparator}
import org.uma.jmetal.util.evaluator.impl.SequentialSolutionListEvaluator
import org.uma.jmetal.util.{AlgorithmRunner, ProblemUtils}

object Main {

  def main(args: Array[String]): Unit = {

    // Se elige el problema, esto se debe de ver como se le puede pasar los ficheros de keel o arff
    val problem = ProblemUtils.loadProblem[BinaryProblem]("main.Problema").asInstanceOf[Problema]
    problem.readDataset("iris.arff")

    // Se elige el crossover y sus parametros, en este caso, el crossover sbx
    val crossoverProbability: Double = 0.9
    val crossoverDistributionIndex: Double = 20.0
    val crossover = new HUXCrossover(crossoverProbability)

    // Operador de mutacion
    val mutationProbability: Double = 1.0 / problem.getNumberOfVariables
    val mutationDistributionIndex: Double = 20.0
    val mutation =  new BiasedMutationDNF(mutationProbability)   //new PolynomialMutation(mutationProbability, mutationDistributionIndex)

    // Operador de seleccion
    val selection = new BinaryTournamentSelection[BinarySolution](new RankingAndCrowdingDistanceComparator[BinarySolution])

    // Por defecto, el comparador de dominancia MINIMIZA los objetivos, hay que revertirlo para poder maximizar.
    val dominanceComparator = new DominanceComparator[BinarySolution]().reversed()

    // Se construye el algoritmo genetico con los datos que se han introducido.
    val algorithm = new NSGAIIBuilder[BinarySolution](problem, crossover, mutation)
      .setSelectionOperator(selection)
      .setMaxEvaluations(25000)
      .setPopulationSize(100).setDominanceComparator(dominanceComparator)
      .build

    //val algorithm = new [BinarySolution](problem,25000,100,crossover,mutation, selection,new SequentialSolutionListEvaluator[BinarySolution]())


    // Ahora, se ejecuta el algoritmo genetico previamente creado.
    val algorithmRunner: AlgorithmRunner = new AlgorithmRunner.Executor(algorithm).execute

    // Una vez se ejecuta, se extraen los resultados.

    val population = algorithm.getResult
    val computingTime = algorithmRunner.getComputingTime

    println("Total execution time: " + computingTime + "ms")

    algorithm.getPopulation.forEach(println)

  }

}
