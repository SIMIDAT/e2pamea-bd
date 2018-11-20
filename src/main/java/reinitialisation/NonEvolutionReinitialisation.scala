package reinitialisation

import java.util
import java.util.stream.Collectors

import attributes.{Clase, Coverage}
import evaluator.EvaluatorMapReduce
import filters.TokenCompetitionFilter
import javax.management.JMException
import main.{BigDataEPMProblem, NSGAIIModifiable}
import utils.BitSet
import org.uma.jmetal.problem.Problem
import org.uma.jmetal.solution.{BinarySolution, Solution}
import org.uma.jmetal.util.JMetalException
import org.uma.jmetal.util.solutionattribute.impl.DominanceRanking

import scala.collection.mutable.ArrayBuffer

class NonEvolutionReinitialisation[S <: Solution[_]](threshold: Int, numClasses: Int, numExamples: Int) extends ReinitialisationCriteria[S]{

  /**
    * Generation where the last change in the population occurred
    */
    private var lastChange: Array[Int] = new Array[Int](numClasses)

    private var previousCoverage: Array[BitSet] = new Array[BitSet](numClasses)
    for(i <- previousCoverage.indices){
      previousCoverage(i) = new BitSet(numExamples)
    }

  /**
    * It checks whether the reinitialisation criteria must be applied or not
    *
    * @param solutionList
    * @return
    */
  override def checkReinitialisation(solutionList: util.List[S], problem: Problem[S], evaluationsNumber: Int, classNumber: Int): Boolean = {
    var coverageTotal = new BitSet(previousCoverage(classNumber).capacity)
    val clase = classNumber
    val pop = solutionList.stream().filter(x => x.getAttribute(classOf[Clase[S]]).asInstanceOf[Int] == clase).collect(Collectors.toList[S])

    for(i <- 0 until pop.size()){
      val rank = pop.get(i).getAttribute(classOf[DominanceRanking[S]]).asInstanceOf[Int]
      if(rank == 0){
        val coverage = pop.get(i).getAttribute(classOf[Coverage[S]]).asInstanceOf[BitSet]
        coverageTotal = coverageTotal | coverage
      }
    }

    val newIndsCovered = (previousCoverage(classNumber) ^ coverageTotal) & (~previousCoverage(classNumber))
    previousCoverage(classNumber) = coverageTotal

    if(newIndsCovered.cardinality() > 0 ){
      lastChange(classNumber) = evaluationsNumber
      return false
    }

    return evaluationsNumber - lastChange(classNumber) >= threshold
  }

  /**
    * It applies the reinitialisation over the actual solution
    *
    * @param solutionList
    * @return
    */
  override def doReinitialisation(solutionList: util.List[S], problem: Problem[S], generationNumber: Int, classNumber: Int): util.List[S] = {
    return null
  }

  def doReinitialisation(solutionList: util.List[S], problem: Problem[S], generationNumber: Int, classNumber: Int, algorithm: NSGAIIModifiable[S]): util.List[S] = {
    val filter = new TokenCompetitionFilter[S]
    val popToAdd = new ArrayBuffer[S]()
    val originalSize = solutionList.size()
    val problema = problem.asInstanceOf[BigDataEPMProblem]

    // Adds the individuals that belongs to the class we are processing and they are in the Pareto front (ranking = 0)
    for(i <- 0 until solutionList.size()){
      val ind = solutionList.get(i)
      val ranking = ind.getAttribute(classOf[DominanceRanking[S]]).asInstanceOf[Int]
      val clas = ind.getAttribute(classOf[Clase[S]]).asInstanceOf[Int]

      if( ranking == 0 && clas == classNumber){
        popToAdd += ind
      }
    }

    // Adds the individual of the current class of the elite as well
    for(i <- 0 until algorithm.getElitePopulation.size()){
      val ind = algorithm.getElitePopulation.get(i)
      val clas = ind.getAttribute(classOf[Clase[S]]).asInstanceOf[Int]

      if(clas == classNumber){
        popToAdd += ind
      }
    }

    // Then, perform the token competition against the individuals of the joint population
    val newPop = filter.doFilter(popToAdd, classNumber, algorithm.getEvaluator.asInstanceOf[EvaluatorMapReduce])

    // After that, removes in the elite population the indivuals in the elite that belongs to the given class and add all of the newPop
    algorithm.getElitePopulation.removeIf(ind => ind.getAttribute(classOf[Clase[S]]).asInstanceOf[Int] == classNumber)
    algorithm.getElitePopulation.addAll(newPop)

    // Now, create the new population in order to continue the evolutionary process
    var coverageTotal = new BitSet(problema.getNumExamples)
    for(i <- 0 until newPop.size()){
      val cov = newPop.get(i).getAttribute(classOf[Coverage[S]]).asInstanceOf[BitSet]
      coverageTotal = coverageTotal | cov
    }
    val eval = algorithm.getEvaluator.asInstanceOf[EvaluatorMapReduce]
    val examplesClassNotCovered = ~(coverageTotal & eval.classes(classNumber)) & eval.classes(classNumber)

    if(examplesClassNotCovered.cardinality() > 0){
      // Coverage-based re-initialisation. Get one example not covered and try to cover it.
      var example = examplesClassNotCovered.nextSetBit(0)

      // Try to generate individuals that covers that example
      if(eval.bigDataProcessing){
        throw new JMetalException(this.getClass.getName + ": Method for processing the RDD is not supported yet." )
      } else {
        // Get the pairs that cover that example.
        val pairs : ArrayBuffer[(Int, Int)] = new ArrayBuffer[(Int, Int)]()
        for(i <- eval.sets.indices){
          // for each variable
          for(j <- eval.sets(i).indices){
            // for each value
            if(eval.sets(i)(j).get(example)){
              val add = (i,j)
              pairs += add
            }
          }
        }

        // now we have the pairs that covers that example. Create the new population
        solutionList.removeIf(ind => ind.getAttribute(classOf[Clase[S]]).asInstanceOf[Int] == classNumber)
        for (i <- 0 until (originalSize - solutionList.size())) {
          val newInd: S = problema.createSolution(pairs, 0.5).asInstanceOf[S]
          val clas = new Clase[S]
          clas.setAttribute(newInd, classNumber)
          solutionList.add(newInd)
        }

      }
    } else {

      // As all examples of the class are covered by the elite population, create random individuals.
      // Now, the new population is generated by means of a coverage-based initialisation.
      // By now, it is generated by the oriented initialisation
      solutionList.removeIf(ind => ind.getAttribute(classOf[Clase[S]]).asInstanceOf[Int] == classNumber)
      for (i <- 0 until (originalSize - solutionList.size())) {
        val newInd = problem.createSolution()
        val clas = new Clase[S]
        clas.setAttribute(newInd, classNumber)
        solutionList.add(newInd)
      }
    }
    lastChange(classNumber) = generationNumber

    solutionList
  }



  def coverageBasedInitialisation(elitePop: util.List[S], problem: BigDataEPMProblem): Unit ={

  }
}
