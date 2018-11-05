package evaluator;

import exceptions.InvalidRangeInMeasureException;
import fuzzy.Fuzzy;
import org.uma.jmetal.problem.Problem;
import org.uma.jmetal.solution.BinarySolution;
import org.uma.jmetal.solution.Solution;
import org.uma.jmetal.util.evaluator.SolutionListEvaluator;
import qualitymeasures.ContingencyTable;
import qualitymeasures.QualityMeasure;
import weka.core.Instances;

import java.util.ArrayList;
import java.util.List;

/**
 * An evaluator of individuals to be used in an evolutionary algorithm
 *
 * @param <S> The type of the solution to be handled
 */
public abstract class Evaluator<S extends Solution> implements SolutionListEvaluator<S> {

    /**
     * The objectives to be used in the evaluator.
     * These are the objectives employed for guiding the search process and they are used only for its identification.
     */
    private ArrayList<QualityMeasure>  objectives;

    private Problem<S> problem;


    /**
     * GETTERS AND SETTERS
     */

    public ArrayList<QualityMeasure> getObjectives() {
        return objectives;
    }

    public void setObjectives(ArrayList<QualityMeasure> objectives) {
        this.objectives = objectives;
    }


    public Problem<S> getProblem() {
        return problem;
    }

    public void setProblem(Problem<S> problem) {
        this.problem = problem;
    }


    /**
     * It calculates the quality measures given a contingency table
     *
     * @param confMatrix
     * @param objs
     * @param isTrain
     */
    public ArrayList<QualityMeasure> calculateMeasures(ContingencyTable confMatrix) {

        ArrayList<QualityMeasure> objs = (ArrayList) objectives.clone();

        // Calculates the value of each measure
        objs.forEach(q -> {
            try {
                q.calculateValue(confMatrix);
                q.validate();
            } catch (InvalidRangeInMeasureException ex) {
                System.err.println("Error while evaluating Individuals: ");
                ex.showAndExit(this);
            }
        });

        return objs;
    }


    /**
     * It performs the evaluation of the individuals using the labels and the dataset (if necessary)
     * @param individual
     * @param fuzzySet
     * @param dataset
     */
//    public abstract void doEvaluation(Solution individual, ArrayList<ArrayList<Fuzzy>> fuzzySet, Instances dataset);


    /**
     * It return whether the individual represents the empty pattern or not.
     *
     * @param individual
     * @return
     */
    public abstract boolean isEmpty(Solution individual);

    /**
     * It returns whether a given variable of the individual participates in the pattern or not.
     *
     * @param individual
     * @param var
     * @return
     */
    public abstract boolean participates(Solution individual, int var);


}
