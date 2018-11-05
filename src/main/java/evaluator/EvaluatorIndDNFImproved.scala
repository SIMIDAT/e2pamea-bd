package evaluator
import java.util

import fuzzy.Fuzzy
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.uma.jmetal.problem.Problem
import org.uma.jmetal.solution.{BinarySolution, Solution}
import weka.core.Instances

/**
  * Class for the use of an improved DNF evaluator with MapReduce
  */
class EvaluatorIndDNFImproved extends Evaluator[BinarySolution]{


  var bitSets: RDD[util.BitSet] = null

  /**
    * It initialises the bitset structure employed for the improved evaluation.
    *
    * @param fuzzySet the fuzzy sets definitions
    * @param dataset The data
    * @param sc The spark context in order to create the RDD
    * @param numPartitions The number of partitions to create
    */
  def createInitialisation(fuzzySet: util.ArrayList[util.ArrayList[Fuzzy]], dataset: Instances, sc: SparkContext, numPartitions: Int): Unit = {
    val numAtts = dataset.numAttributes()
    dataset.enumerateAttributes()
  }

  override def evaluate(solutionList: util.List[BinarySolution], problem: Problem[BinarySolution]): util.List[BinarySolution] = ???

  override def shutdown(): Unit = ???


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
}
