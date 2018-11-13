package reinitialisation

import java.util
import java.util.stream.Collectors

import attributes.{Clase, Coverage}
import main.NSGAIIModifiable
import utils.BitSet
import org.uma.jmetal.problem.Problem
import org.uma.jmetal.solution.{BinarySolution, Solution}
import org.uma.jmetal.util.solutionattribute.impl.DominanceRanking

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
  override def checkReinitialisation(solutionList: util.List[S], problem: Problem[S], generationNumber: Int, classNumber: Int): Boolean = {
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
      lastChange(classNumber) = generationNumber
      return false
    }

    return generationNumber - lastChange(classNumber) >= threshold
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

  def doReinitialisation(solutionList: util.List[S], problem: Problem[S], generationNumber: Int, algorithm: NSGAIIModifiable[S]): util.List[S] = {
    
  }
}
