package evaluator;

import fuzzy.Fuzzy;
import main.IndDNF;
import org.uma.jmetal.solution.Solution;
import qualitymeasures.ContingencyTable;
import weka.core.Instances;

import java.util.ArrayList;


public class EvaluatorIndDNF extends Evaluator {

    @Override
    public void doEvaluation(Solution individual, ArrayList<ArrayList<Fuzzy>> fuzzySet, Instances dataset) {
        if(individual instanceof IndDNF){
            IndDNF ind = (IndDNF) individual;
            int tp = 0;
            int fp = 0;
            int tn = 0;
            int fn = 0;

            // Now, for each instance in the dataset, calculate the coverage or not of the example
        }
    }
}
