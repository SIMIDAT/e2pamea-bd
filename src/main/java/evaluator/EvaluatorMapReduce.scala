package evaluator

import java.util

import org.uma.jmetal.problem.Problem
import org.uma.jmetal.solution.{BinarySolution, Solution}


/**
  * Scala class for implementing the improved evaluation based in MapReduce for Individuals in DNF form
  *
  */
class EvaluatorMapReduce extends Evaluator[BinarySolution] {



  /**
    * It return whether the individual represents the empty pattern or not.
    *
    * @param individual
    * @return
    */
  override def isEmpty(individual: Solution[_]): Boolean = ???

  /**
    * It returns whether a given variable of the individual participates in the pattern or not.
    *
    * @param individual
    * @param var
    * @return
    */
  override def participates(individual: Solution[_], `var`: Int): Boolean = ???

  override def evaluate(solutionList: util.List[BinarySolution], problem: Problem[BinarySolution]): util.List[BinarySolution] = ???

  override def shutdown(): Unit = ???
}
