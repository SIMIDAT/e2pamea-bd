import org.uma.jmetal.algorithm.multiobjective.nsgaii.NSGAIIBuilder
import org.uma.jmetal.operator.impl.crossover.SBXCrossover
import org.uma.jmetal.operator.impl.mutation.PolynomialMutation
import org.uma.jmetal.operator.impl.selection.BinaryTournamentSelection
import org.uma.jmetal.solution.DoubleSolution
import org.uma.jmetal.util.comparator.RankingAndCrowdingDistanceComparator
import org.uma.jmetal.util.{AlgorithmRunner, ProblemUtils}

object Main {

  def main(args: Array[String]): Unit = {

    // Se elige el problema, esto se debe de ver como se le puede pasar los ficheros de keel o arff
    val problem = ProblemUtils.loadProblem[DoubleSolution]("org.uma.jmetal.problem.multiobjective.zdt.ZDT1")


    // Se elige el crossover y sus parametros, en este caso, el crossover sbx
    val crossoverProbability: Double = 0.9
    val crossoverDistributionIndex: Double = 20.0
    val crossover = new SBXCrossover(crossoverProbability,crossoverDistributionIndex)

    // Operador de mutacion
    val mutationProbability: Double = 1.0 / problem.getNumberOfVariables
    val mutationDistributionIndex: Double = 20.0
    val mutation = new PolynomialMutation(mutationProbability, mutationDistributionIndex)

    // Operador de seleccion
    val selection = new BinaryTournamentSelection[DoubleSolution](new RankingAndCrowdingDistanceComparator[DoubleSolution])


    // Se construye el algoritmo genetico con los datos que se han introducido.
    val alg = new
    val algorithm = new NSGAIIBuilder[DoubleSolution](problem, crossover, mutation)
      .setSelectionOperator(selection)
      .setMaxEvaluations(25000)
      .setPopulationSize(100)
      .build


    // Ahora, se ejecuta el algoritmo genetico previamente creado.
    val algorithmRunner: AlgorithmRunner = new AlgorithmRunner.Executor(algorithm).execute

    // Una vez se ejecuta, se extraen los resultados.

    val population = algorithm.getResult
    val computingTime = algorithmRunner.getComputingTime

    println("Total execution time: " + computingTime + "ms")

    algorithm.getResult.forEach(x => println(x.getObjective(0)))

  }

}
