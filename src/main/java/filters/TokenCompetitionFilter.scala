package filters

import java.util
import java.util.Comparator
import java.util.stream.Collectors

import attributes.Clase
import evaluator.EvaluatorMapReduce
import org.uma.jmetal.solution.{BinarySolution, Solution}
import qualitymeasures.WRAccNorm
import utils.BitSet

class TokenCompetitionFilter[S <: Solution[_]]{


  def doFilter(solutionList: util.List[S], clase: Int, evaluator: EvaluatorMapReduce): util.List[S] ={
    val result = new util.ArrayList[S]()
    val pop = solutionList.stream.filter((x: S) => x.getAttribute(classOf[Clase[_ <: Solution[_]]]).asInstanceOf[Int] == clase).collect(Collectors.toList)
    pop.sort(Comparator.comparing((ind: S) => ind.getAttribute(classOf[WRAccNorm]).asInstanceOf[WRAccNorm].getValue).reversed())

    var tokens = new BitSet(pop.get(0).getAttribute(classOf[BitSet]).asInstanceOf[BitSet].capacity)

    var counter = 0
    var allCovered = false

    do{

      val coverage = pop.get(counter).getAttribute(classOf[BitSet]).asInstanceOf[BitSet]
      val clase = pop.get(counter).getAttribute(classOf[Clase[BinarySolution]]).asInstanceOf[Int]

      // covered examples that belongs to the class
      val correct = coverage & evaluator.classes(clase)

      val newCov = (tokens ^ correct) & (~tokens)
      if(newCov.cardinality() > 0){
        result.add(pop.get(counter))
      }
      tokens = tokens | correct

      if(tokens.cardinality() == tokens.capacity){
        allCovered = true
      }
      counter += 1

    } while(counter < pop.size() && !allCovered)

    if(result.isEmpty){
      result.add(pop.get(0))
    }

    result
  }

}
