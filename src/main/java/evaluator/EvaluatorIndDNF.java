package evaluator;

import fuzzy.Fuzzy;
import main.IndDNF;
import org.uma.jmetal.solution.Solution;
//import qualitymeasures.ContingencyTable;
import qualitymeasures.ContingencyTable;
import qualitymeasures.QualityMeasure;
import weka.core.Instances;

import java.util.ArrayList;


public class EvaluatorIndDNF extends Evaluator {

    @Override
    public void doEvaluation(Solution individual, ArrayList<ArrayList<Fuzzy>> fuzzySet, Instances dataset) {
        if(individual instanceof IndDNF) {
            IndDNF ind = (IndDNF) individual;
            int tp = 0;
            int fp = 0;
            int tn = 0;
            int fn = 0;

            // Now, for each instance in the dataset, calculate the coverage or not of the example
            if (!ind.isEmpty()) {
                for (int i = 0; i < dataset.numInstances(); i++) {
                    double fuzzyTrigger = 1.0;
                    int index = 0;
                    for (int var = 0; var < dataset.numAttributes() && fuzzyTrigger > 0.0; var++) {
                        if (var != dataset.classIndex()) {
                            if (ind.participates(var)) {
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

                    Double classAttr = dataset.get(i).classValue();
                    // Fuzzy belonging degree is now calculated for the given instance. Calculate the measures
                    if (fuzzyTrigger > 0) {
                        if (ind.getClas() == classAttr.intValue()) {
                            tp++;
                        } else {
                            fp++;
                        }
                    } else {
                        if (ind.getClas() == classAttr.intValue()) {
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
                    ind.setObjective(i, 0.0);
                }
            }
        }
    }
}
