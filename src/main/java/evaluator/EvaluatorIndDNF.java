package evaluator;

import fuzzy.Fuzzy;
import main.Clase;
import main.IndDNF;
import main.Problema;
import org.uma.jmetal.problem.Problem;
import org.uma.jmetal.solution.BinarySolution;
import org.uma.jmetal.solution.Solution;
//import qualitymeasures.ContingencyTable;
import org.uma.jmetal.solution.impl.DefaultBinarySolution;
import qualitymeasures.ContingencyTable;
import qualitymeasures.QualityMeasure;
import scala.util.hashing.Hashing;
import weka.core.Instances;

import java.util.ArrayList;
import java.util.List;


public class EvaluatorIndDNF extends Evaluator<BinarySolution> {

    public EvaluatorIndDNF(Problem<BinarySolution> problem){
        super();
        super.setProblem(problem);
    }

    public void doEvaluation(Solution individual, ArrayList<ArrayList<Fuzzy>> fuzzySet, Instances dataset) {
        if(individual instanceof DefaultBinarySolution) {
            DefaultBinarySolution ind = (DefaultBinarySolution) individual;
            int tp = 0;
            int fp = 0;
            int tn = 0;
            int fn = 0;

            // Now, for each instance in the dataset, calculate the coverage or not of the example

            if (! isEmpty(ind) && ind.getVariableValue(dataset.classIndex()).cardinality() == 1 ) { // The pattern is empty or it is not valid (as the class variable contains more than one class.)
                for (int i = 0; i < dataset.numInstances(); i++) {
                    double fuzzyTrigger = 1.0;
                    int index = 0;
                    for (int var = 0; var < dataset.numAttributes() && fuzzyTrigger > 0.0; var++) {
                        if (var != dataset.classIndex()) {
                            if (participates(ind, var)) {
                                // The variable participates in the rule (all values are different from zero or one)
                                if (dataset.attribute(var).isNominal()) {
                                    // Variable nominal
                                    Double value = dataset.instance(i).value(var);
                                    if (!ind.getVariableValue(var).get(value.intValue()) && !dataset.instance(i).isMissing(var)) {
                                        // Variable (and the whole rule) does not cover the example
                                        fuzzyTrigger = 0.0;
                                    }
                                } else if (dataset.attribute(var).isNumeric()) {
                                    // Numeric variable, fuzzy computation.
                                    if (!dataset.instance(i).isMissing(var)) {
                                        double belonging = 0.0;
                                        double aux;
                                        for (int k = 0; k < ind.getNumberOfBits(var); k++) {
                                            if (ind.getVariableValue(var).get(k)) {
                                                Double value = dataset.instance(i).value(var);
                                                aux = fuzzySet.get(var).get(k).getBelongingDegree(value);
                                            } else {
                                                aux = 0.0;
                                            }
                                            belonging = Math.max(belonging, aux);
                                        }
                                        fuzzyTrigger = Math.min(belonging, fuzzyTrigger);
                                    }
                                }
                            }
                        }
                    }

                    // Get the class of the examples
                    Double classAttr = dataset.get(i).classValue();

                    // Get the attribute class of the individual
                    int clas = (int) ind.getAttribute(new Clase<DefaultBinarySolution>().getAttributeIdentifier());

                    // Fuzzy belonging degree is now calculated for the given instance. Calculate the measures
                    if (fuzzyTrigger > 0) {
                        if (clas == classAttr.intValue()) {
                            tp++;
                        } else {
                            fp++;
                        }
                    } else {
                        if (clas == classAttr.intValue()) {
                            fn++;
                        } else {
                            tn++;
                        }
                    }

                }

                // now, all individuals are evaluated so the contingency table can be created for calculating the objectives.

                ContingencyTable table = new ContingencyTable(tp, fp, tn, fn);
                ArrayList<QualityMeasure> measures = super.calculateMeasures(table);
                for (int i = 0; i < measures.size(); i++) {
                    ind.setObjective(i, measures.get(i).getValue());
                }
            } else {
                for (int i = 0; i < ind.getNumberOfObjectives(); i++) {
                    ind.setObjective(i, Double.NEGATIVE_INFINITY);
                }
            }
        }
    }

    @Override
    public boolean isEmpty(Solution individual) {
        if(individual instanceof DefaultBinarySolution){
            DefaultBinarySolution ind = (DefaultBinarySolution) individual;
            for(int i = 0; i < ind.getNumberOfVariables(); i++){
                if(ind.getVariableValue(i).cardinality() > 0 && ind.getVariableValue(i).cardinality() < ind.getNumberOfBits(i)){
                    // variable participates in the rules, is not empty
                    return false;
                }
            }

            return true;
        } else {
            return false;
        }
    }

    @Override
    public boolean participates(Solution individual, int var) {
        if(individual instanceof DefaultBinarySolution) {
            DefaultBinarySolution ind = (DefaultBinarySolution) individual;
            // a variable does not participate in the rule if all its values are 0 or 1.
            return ind.getVariableValue(var).cardinality() > 0 && ind.getVariableValue(var).cardinality() < ind.getNumberOfBits(var);
        } else {
            return false;
        }
    }


    @Override
    public List<BinarySolution> evaluate(List<BinarySolution> solutionList, Problem<BinarySolution> problem) {
        // se evalua lo población entera
        Problema pr = (Problema) problem;
        solutionList.forEach( s -> doEvaluation(s, pr.getFuzzySets(),((Problema) problem).getDataset()));
    }

    @Override
    public void shutdown() {

    }
}
