package filters

import java.util

import evaluator.EvaluatorMapReduce
import org.uma.jmetal.solution.Solution
import qualitymeasures.{ContingencyTable, QualityMeasure}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

class MeasureFilter[S <: Solution[_]](filter: QualityMeasure, threshold: Double) {


  def doFilter(solutionList: util.List[S], clase: Int, evaluator: EvaluatorMapReduce): util.List[S] = {
    val pop = solutionList.asScala
    val filterPop = pop.filter(x => {
      filter.calculateValue(x.getAttribute(classOf[ContingencyTable]).asInstanceOf[ContingencyTable])
      filter.getValue >= threshold
    })

    val a = new ArrayBuffer[S]()

    if(filterPop.isEmpty){
      pop.sortBy( x => {
        filter.calculateValue(x.getAttribute(classOf[ContingencyTable]).asInstanceOf[ContingencyTable])
        filter
      })
      a += pop(0)
    } else {
      a ++= filterPop
    }

    a.asJava

  }

}
