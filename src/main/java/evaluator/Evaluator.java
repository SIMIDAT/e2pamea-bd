package evaluator;

import exceptions.InvalidRangeInMeasureException;
import fuzzy.Fuzzy;
import org.uma.jmetal.solution.Solution;
import qualitymeasures.ContingencyTable;
import qualitymeasures.QualityMeasure;
import weka.core.Instances;

import java.util.ArrayList;

public abstract class Evaluator {

    /**
     * The objectives to be used in the evaluator.
     * These are the objectives employed for guiding the search process and they are used only for its identification.
     */
    private ArrayList<QualityMeasure>  objectives;


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
    public abstract void doEvaluation(Solution individual, ArrayList<ArrayList<Fuzzy>> fuzzySet, Instances dataset);

}
