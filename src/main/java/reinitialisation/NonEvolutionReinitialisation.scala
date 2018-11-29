package reinitialisation

import java.util

import attributes.{Clase, Coverage}
import evaluator.EvaluatorMapReduce
import filters.TokenCompetitionFilter
import main.{BigDataEPMProblem, NSGAIIModifiable}
import operators.selection.RankingAndCrowdingSelection
import org.uma.jmetal.problem.Problem
import org.uma.jmetal.solution.Solution
import org.uma.jmetal.util.JMetalException
import org.uma.jmetal.util.solutionattribute.impl.DominanceRanking
import utils.BitSet

import scala.collection.JavaConverters._
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
    val pop = solutionList.asScala.filter( (x:S) => x.getAttribute(classOf[Clase[S]]).asInstanceOf[Int] == clase)

    for(i <- pop.indices){
      val rank = pop(i).getAttribute(classOf[DominanceRanking[S]]).asInstanceOf[Int]
      if(rank == 0){
        val coverage = pop(i).getAttribute(classOf[Coverage[S]]).asInstanceOf[BitSet]
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
    var newPop = filter.doFilter(popToAdd, classNumber, algorithm.getEvaluator.asInstanceOf[EvaluatorMapReduce])

    // Aplicar test de dominancia en la elite y quedarse con el frente de pareto
    val rankingAndCrowdingSelection = new RankingAndCrowdingSelection[S](newPop.size, algorithm.getDominanceComparator)
    newPop = rankingAndCrowdingSelection.execute(newPop)
      .asScala
      .filter(x => x.getAttribute(classOf[DominanceRanking[S]]).asInstanceOf[Int] == 0)
      .asJava

    // After that, removes in the elite population the indivuals in the elite that belongs to the given class and add all of the newPop
    val elite = algorithm.getElitePopulation.asScala.filter((ind: S) => ind.getAttribute(classOf[Clase[S]]).asInstanceOf[Int] != classNumber)
    algorithm.setElitePopulation(elite.asJava)
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
      lastChange(classNumber) = generationNumber

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
        val newSol = solutionList.asScala.filter(ind => ind.getAttribute(classOf[Clase[S]]).asInstanceOf[Int] != classNumber).asJava
        // add the non-repeated individuals of the elite population
        newSol.addAll(removeRepeated(newPop))

        // Then, remaining indivuals are generated by means of the coverage-based initialisation
        for (i <- 0 until (originalSize - newSol.size())) {
          val newInd: S = problema.createSolution(pairs, 0.5).asInstanceOf[S]
          val clas = new Clase[S]
          clas.setAttribute(newInd, classNumber)
          newSol.add(newInd)
        }
        return newSol
      }
    } else {

      // As all examples of the class are covered by the elite population, create random individuals.
      // Adds the non-repeated individuals of the Pareto front in the population
      // By now, it is generated by the oriented initialisation
      val newSol = solutionList.asScala.filter(ind => ind.getAttribute(classOf[Clase[S]]).asInstanceOf[Int] != classNumber).asJava
      newSol.addAll(removeRepeated(newPop))
      for (i <- 0 until (originalSize - newSol.size())) {
        val newInd = problem.createSolution()
        val clas = new Clase[S]
        clas.setAttribute(newInd, classNumber)
        newSol.add(newInd)
      }
      return newSol
    }

  }



  def coverageBasedInitialisation(elitePop: util.List[S], problem: BigDataEPMProblem): Unit ={

  }

  def getHashCode(ind: S): Int = {
    var result = util.Arrays.hashCode(ind.getObjectives)
    for(i <- 0 until ind.getNumberOfVariables){
      result = 31 * result + ind.getVariableValue(i).hashCode()
    }
    result
  }


  def removeRepeated(solutionList: util.List[S]): util.List[S] = {
    val marks = new util.BitSet(solutionList.size())

    for(i <- 0 until solutionList.size()){
      val hash = getHashCode(solutionList.get(i))
      for(j <- i until solutionList.size()){
        if(i!=j && getHashCode(solutionList.get(j)) == hash){
          marks.set(j)
        }
      }
    }

    val toReturn = new ArrayBuffer[S]()
    for(i <- 0 until solutionList.size()){
      if(!marks.get(i)){
        toReturn += solutionList.get(i)
      }
    }

    toReturn.asJava

  }
}
