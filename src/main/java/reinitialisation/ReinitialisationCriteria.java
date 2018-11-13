package reinitialisation;

import org.uma.jmetal.problem.Problem;
import org.uma.jmetal.solution.Solution;

import java.io.Serializable;
import java.util.List;

public interface ReinitialisationCriteria<S extends Solution<?>> extends Serializable {

    /**
     * It checks whether the reinitialisation criteria must be applied or not
     * @param solutionList
     * @return
     */
    public abstract boolean checkReinitialisation(List<S> solutionList, Problem<S> problem, int generationNumber, int classNumber);

    /**
     * It applies the reinitialisation over the actual solution
     * @param solutionList
     * @return
     */
    public abstract List<S> doReinitialisation(List<S> solutionList, Problem<S> problem, int generationNumber, int classNumber);
}
