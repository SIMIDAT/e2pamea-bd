/* 
 * The MIT License
 *
 * Copyright 2018 Ángel Miguel García Vico.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package utils;


import attributes.Clase;
import attributes.TestMeasures;
import main.BigDataEPMProblem;
import org.uma.jmetal.solution.BinarySolution;
import qualitymeasures.ContingencyTable;
import qualitymeasures.QualityMeasure;

import java.io.File;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;

/**
 * Class to store the results and rules of a given population of individuals
 *
 * @author Ángel Miguel García Vico (agvico@ujaen.es)
 * @since JDK 8
 * @version 1.0
 */
public final class ResultWriter {

    /**
     * The path where the objectives values for each individual is stored
     */
    private final String pathTra;

    /**
     * The path where the test quality measures are stored (detailed file)
     */
    private final String pathTst;

    /**
     * The path where the test quality measures are stored (summary file)
     */
    private final String pathTstSummary;

    /**
     * The path where the rules are stored
     */
    private final String pathRules;

    /**
     * The population to get the results
     */
    private List<BinarySolution> population;

    /**
     * The instance where the variables are obtained
     */
    private BigDataEPMProblem problem;

    /**
     * The formatter of the numbers
     */
    private final DecimalFormat sixDecimals;

    /**
     * The symbols to use in the formatter
     */
    private final DecimalFormatSymbols symbols;

    /**
     * It determines if it is the first time to write the header or not
     */
    private boolean firstTime;

    /**
     * The objectives used in the algorithm
     */
    private ArrayList<QualityMeasure> objectives;

    /**
     * Default constructor, it sets the path where the files are stored.
     *
     *
     * @param tra The path for training QMs files
     * @param tst The path fot test QMs files
     * @param tstSummary The path for the summary of the test QMs file
     * @param rules The path for the rules file
     * @param population The population of individuals
     * @param overwrite If previous files exists, overwrite it?
     */
    public ResultWriter(String tra, String tst, String tstSummary, String rules, List<BinarySolution> population, BigDataEPMProblem problem, ArrayList<QualityMeasure> objectives, boolean overwrite) {
        this.pathRules = rules;
        this.pathTra = tra;
        this.pathTst = tst;
        this.pathTstSummary = tstSummary;
        this.population = population;
        this.problem = problem;
        symbols = new DecimalFormatSymbols(Locale.GERMANY);
        symbols.setDecimalSeparator('.');
        symbols.setNaN("NaN");
        symbols.setInfinity("INFINITY");
        sixDecimals = new DecimalFormat("0.000000", symbols);
        if (this.population != null) {
            this.population.sort(Comparator.comparingInt(x -> (int) x.getAttribute(Clase.class)));
        }
        firstTime = true;

        if (overwrite) {
            File[] a = {new File(tra), new File(tst), new File(tstSummary), new File(rules)};
            for (File f : a) {
                if (f.exists()) {
                    f.delete();
                }
            }
        }
        this.objectives = objectives;
    }

    /**
     * It only writes the results of the rules
     */
    public void writeRules() {
        String content = "";
        for (int i = 0; i < population.size(); i++) {
            content += "Rule " + i + "\n";
            content += toString(population.get(i)) + "\n";
        }
        Files.addToFile(pathRules, content);
    }

    /**
     * It only writes the results of the objectives
     */
    public void writeTrainingMeasures() {
        String content = "";
            // Write the header (the consequent first, and next, the objective quality measures, finaly, the diversity measure)
            content += "Rule\tID\tConsequent";
            for (QualityMeasure q : (ArrayList<QualityMeasure>) objectives) {
                content += "\t" + q.getShortName();
            }
            content += "\n";
        Attribute[] attrs = problem.getAttributes();
        // Now, for each individual, writes the training measures
        for (int i = 0; i < population.size(); i++) {
            content +=  i + "\t" + population.get(i).hashCode() + "\t" + attrs[attrs.length - 1].valueName((int) population.get(i).getAttribute(Clase.class)) + "\t";
            for (double d: population.get(i).getObjectives()) {
                content += sixDecimals.format(d) + "\t";
            }
            content += "\n";
        }
        Files.addToFile(pathTra, content);
    }

    /**
     * It writes the full version of the results test quality measures, i.e.,
     * the whole set of measures for each individual on each timestamp, in
     * addition to the summary
     */
    public void writeTestFullResults() {
        // this array stores the sum of the quality measures for the average
        ArrayList<Double> averages = new ArrayList<>();
        double numVars = 0.0;
        
        for (QualityMeasure q : (ArrayList<QualityMeasure>) population.get(0).getAttribute(TestMeasures.class)) {
            averages.add(0.0);
        }

        // First, write the headers
        String content = "Rule\tClass\tID\tNumRules\tNumVars\tTP\tFP\tTN\tFN";

            // now, append each test quality measure
            for (QualityMeasure q : (ArrayList<QualityMeasure>) population.get(0).getAttribute(TestMeasures.class)) {
                content += "\t" + q.getShortName();
            }
            content += "\n";

        Attribute[] attrs = problem.getAttributes();
        // now write the test results for each individual
        for (int i = 0; i < population.size(); i++) {
            int vars = getNumVars(population.get(i));
            content += i + "\t"
                    + attrs[attrs.length - 1].valueName((int) population.get(i).getAttribute(Clase.class)) + "\t" + population.get(i).hashCode() + "\t"
                    + "------\t"
                    + sixDecimals.format(vars) + "\t";
            numVars += vars;
            ContingencyTable table = (ContingencyTable) population.get(i).getAttribute(ContingencyTable.class);
            content += table.getTp() + "\t";
            content += table.getFp() + "\t";
            content += table.getTn() + "\t";
            content += table.getFn() + "\t";

            ArrayList<QualityMeasure> objs = (ArrayList<QualityMeasure>) population.get(i).getAttribute(TestMeasures.class);
            for(int j = 0; j < objs.size(); j++){
                content += sixDecimals.format(objs.get(j).getValue()) + "\t";
                averages.set(j, averages.get(j) + objs.get(j).getValue());
            }


            content += "\n";
        }

        numVars /= (double) population.size();
        // finally, write the average results
        content += "------\t------\t------\t" + sixDecimals.format(population.size()) + "\t" + sixDecimals.format(numVars) + "\t-----\t------\t------\t------\t";
        for (Double d : averages) {
            content += sixDecimals.format(d / (double) population.size()) + "\t";
        }
        content += "\n";
        Files.addToFile(pathTst, content);
    }

    /**
     * It writes the summary results of the test quality measures, i.e., it only
     * writes the line with the average results.
     *
     * @param time_ms The execution time in milliseconds.
     */
    /*public void writeTestSummaryResults(long time_ms) {
        // this array stores the sum of the quality measures for the average
        ArrayList<Double> averages = new ArrayList<>();
        double numVars = 0.0;
        for (QualityMeasure q : (ArrayList<QualityMeasure>) population.get(0).getMedidas()) {
            averages.add(0.0);
        }

        // First, write the headers
        String content = "";
        if (firstTime) {
            content = "Timestamp\tRule\tClass\tNumRules\tNumVars";

            // now, append each test quality measure
            for (int j = 0; j < population.get(0).getMedidas().size(); j++) {
                QualityMeasure q = (QualityMeasure) population.get(0).getMedidas().get(j);
                content += "\t" + q.getShortName();
            }
            content += "\tExecTime_ms\n";
        }

        // Now, average the results of the test measures
        for (int i = 0; i < population.size(); i++) {
            numVars += population.get(i).getNumVars();
            for (int j = 0; j < population.get(i).getMedidas().size(); j++) {
                QualityMeasure q = (QualityMeasure) population.get(i).getMedidas().get(j);
                averages.set(j, averages.get(j) + q.getValue());
            }
        }

        numVars /= (double) population.size();
        // finally, write the average results
        content += sixDecimals.format(StreamMOEAEFEP.getTimestamp()) + "\t------\t------\t" + sixDecimals.format(population.size()) + "\t" + sixDecimals.format(numVars) + "\t";
        for (Double d : averages) {
            content += sixDecimals.format(d / (double) population.size()) + "\t";
        }
        content += sixDecimals.format(time_ms) + "\n";
        Files.addToFile(pathTstSummary, content);
    }*/

    /**
     * It writes the results of the individuals in the files
     */
    public void writeResults(long time_ms) {
        writeRules();
        writeTrainingMeasures();
        writeTestFullResults();
        firstTime = false;
    }

    /**
     * @return the population
     */
    public List<BinarySolution> getPopulation() {
        return population;
    }


    /**
     * It returns the String representation of the given rule.
     * @param sol
     * @return
     */
    public String toString(BinarySolution sol){
        String result = "";
        Attribute[] attrs = problem.getAttributes();
        for(int i = 0; i < sol.getNumberOfVariables(); i++){
            if(sol.getVariableValue(i).cardinality() > 0 && sol.getVariableValue(i).cardinality() < sol.getNumberOfBits(i)){
                // Variable participates in the rule
                result += "\tVariable " + attrs[i].getName() + " = ";
                for(int j = 0; j < sol.getNumberOfBits(i); j++){
                    if(sol.getVariableValue(i).get(j)){
                        if(attrs[i].isNominal()){
                            result += attrs[i].valueName(j) + "  ";
                        } else {
                            result += "Label " + j + ": " + problem.getFuzzySet(i,j).toString() + "  ";
                        }

                    }
                }
                result += "\n";
            }
        }

        result += "Consequent: " + attrs[attrs.length -1].valueName((int)sol.getAttribute(Clase.class)) + "\n\n";
        return result;
    }

    public int getNumVars(BinarySolution sol ){
        int vars = 0;
        for(int i = 0; i < sol.getNumberOfVariables(); i++) {
            if (sol.getVariableValue(i).cardinality() > 0 && sol.getVariableValue(i).cardinality() < sol.getNumberOfBits(i)) {
                vars++;
            }
        }
        return vars;
    }

}
