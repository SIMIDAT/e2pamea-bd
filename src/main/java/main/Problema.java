package main;

import evaluator.Evaluator;
import evaluator.EvaluatorIndDNF;
import fuzzy.Fuzzy;
import fuzzy.TriangularFuzzySet;
import org.uma.jmetal.problem.BinaryProblem;
import org.uma.jmetal.problem.ConstrainedProblem;
import org.uma.jmetal.problem.impl.AbstractBinaryProblem;
import org.uma.jmetal.solution.BinarySolution;
import org.uma.jmetal.solution.impl.DefaultBinarySolution;
import org.uma.jmetal.util.binarySet.BinarySet;
import org.uma.jmetal.util.pseudorandom.JMetalRandom;
import org.uma.jmetal.util.solutionattribute.impl.NumberOfViolatedConstraints;
import org.uma.jmetal.util.solutionattribute.impl.OverallConstraintViolation;
import qualitymeasures.QualityMeasure;
import qualitymeasures.SuppDiff;
import qualitymeasures.WRAccNorm;
import weka.core.Instances;
import weka.core.converters.ConverterUtils.DataSource;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Random;

public class Problema implements BinaryProblem {

    // AQUI VAN TODAS LAS FUNCIONES RELACIONADAS CON EL PROBLEMA
    // AQUI EN EL CONSTRUCTOR SE DEBERÍA DE LEER EL FICHERO DE DATOS Y CARGAR LOS DATOS
    // TAMBIEN SE TIENE QUE IMPLEMENTAR EL METODO DE EVALUACIÓN
    // Y EL METODO DE CREACION DE UN NUEVO INDIVIDUO

    public static int RANDOM_INITIALISATION = 0;
    public static int ORIENTED_INITIALISATION = 1;

    /**
     * The dataset. It contains all the information about the instances
     */
    private Instances dataset;

    /**
     * The class of the problem for the extraction of rules.
     */
    private Clase<BinarySolution> clase;
    private int clas;


    /**
     * The number of objectives to be used in a MOEA algorithm.
     */
    private int numObjectives = 2;


    /**
     * The number of linguistic labels to be used in numeric variables
     */
    private int numLabels = 3;


    /**
     *  The initialisation method used
     */
    private int initialisationMethod;

    /**
     * The seed of the random number generator
     */
    private int seed;

    /**
     * The fuzzy sets stored for each numeric variable
     */
    private ArrayList<ArrayList<Fuzzy>> fuzzySets;

    /**
     * The evaluator used for measuring the objectives of the individuals
     */
    private Evaluator evaluator;


    /**
     * The number of violated constraints in the problem
     */
    public NumberOfViolatedConstraints<BinarySolution> numberOfViolatedConstraints;

    /**
     * The overall constraint violation
     */
    public OverallConstraintViolation<BinarySolution> overallConstraintViolation;

    /**
     * The random number generator
     */
    public JMetalRandom rand;

    /**
     * It reads an ARFF or CSV file using the WEKA API.
     * @param path The path of the dataset
     */
    public void readDataset(String path) {

        // First, read the dataset and select the class
        DataSource source;
        seed = 1; // cambiar
        numberOfViolatedConstraints = new NumberOfViolatedConstraints<>();
        overallConstraintViolation = new OverallConstraintViolation<>();
        rand = JMetalRandom.getInstance();
        rand.setSeed(seed);
        clase = new Clase<>();

        try {
            source = new DataSource(path);
            dataset = source.getDataSet();
            // Con esto se le fija como clase el ultimo atributo si no estuviera especificado
            if (dataset.classIndex() == -1)
                dataset.setClassIndex(dataset.numAttributes() - 1);


            // Next, set the fuzzy linguistic labels for numeric variables
            fuzzySets = new ArrayList<>();
            for(int i = 0; i < dataset.numAttributes(); i++){
                if(i != dataset.classIndex() && dataset.attribute(i).isNumeric()){
                   double max = getMax(i);
                   double min = getMin(i);
                   fuzzySets.add(generateLinguistcLabels(min, max));
                } else {
                    fuzzySets.add(null);
                }
            }
            evaluator = new EvaluatorIndDNF();
            ArrayList<QualityMeasure> objs = new ArrayList<>();
            objs.add(new WRAccNorm());
            objs.add(new SuppDiff());
            evaluator.setObjectives(objs);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    @Override
    public int getNumberOfVariables() {
        return dataset.numAttributes();
    }

    @Override
    public int getNumberOfObjectives() {
        return numObjectives;
    }

    @Override
    public int getNumberOfConstraints() {
        return 0;
    }


    @Override
    public String getName() {
        return dataset.relationName();
    }

    @Override
    public void evaluate(BinarySolution solution) {
        // This is for test purposes
        evaluator.doEvaluation(solution, fuzzySets, dataset);

    }

    public int getNumObjectives() {
        return numObjectives;
    }

    public void setNumObjectives(int numObjectives) {
        this.numObjectives = numObjectives;
    }

    public int getNumLabels() {
        return numLabels;
    }

    public void setNumLabels(int numLabels) {
        this.numLabels = numLabels;
    }

    public int getInitialisationMethod() {
        return initialisationMethod;
    }

    public void setInitialisationMethod(int initialisationMethod) {
        this.initialisationMethod = initialisationMethod;
    }

    public int getSeed() {
        return seed;
    }

    public void setSeed(int seed) {
        this.seed = seed;
    }

    public ArrayList<ArrayList<Fuzzy>> getFuzzySets() {
        return fuzzySets;
    }

    public void setFuzzySets(ArrayList<ArrayList<Fuzzy>> fuzzySets) {
        this.fuzzySets = fuzzySets;
    }

    public Evaluator getEvaluator() {
        return evaluator;
    }

    public void setEvaluator(Evaluator evaluator) {
        this.evaluator = evaluator;
    }

    public Instances getDataset() {
        return dataset;
    }

    public void setDataset(Instances dataset) {
        this.dataset = dataset;
    }

    @Override
    public BinarySolution createSolution() {
        // Create a random individual
        Random rand = new Random(seed);
        DefaultBinarySolution sol = null;
        if(initialisationMethod == RANDOM_INITIALISATION){
            // By default, individuals are initialised at random
            sol = new DefaultBinarySolution(this);
        } else if(initialisationMethod == ORIENTED_INITIALISATION){
            // Oriented initialisation
            sol = OrientedInitialisation(rand, 0.25);

        }

        clase.setAttribute(sol, clas);
        return sol;
    }

    /**
     * Set the class of a pattern
     *
     * @param clas
     */
    public void setClass(int clas){
        this.clas = clas;
    }

    public int getNumberOfBits(int index) {
        if(dataset.attribute(index).isNumeric()){
            return numLabels;
        } else if(dataset.attribute(index).isNominal()){
            return dataset.attribute(index).numValues();
        } else {
            return -1; // Cambiar por excepción o algo así
        }
    }


    public int getTotalNumberOfBits() {
        int suma = 0;
        for(int index = 0; index < dataset.numAttributes(); index++){
            if(index != dataset.classIndex()){
                if(dataset.attribute(index).isNumeric()){
                    suma += numLabels;
                } else if(dataset.attribute(index).isNominal()){
                    suma += dataset.attribute(index).numValues();
                }
            }
        }

        return suma;

    }

    /**
     * Get the maximum value of a numeric variable
     * @param var
     */
    private double getMax(int var){
        double max = Double.NEGATIVE_INFINITY;
        for(int i = 0; i < dataset.numInstances(); i++){
            if(dataset.get(i).value(var) > max){
                max = dataset.get(i).value(var);
            }
        }

        return max;
    }

    /**
     * Get the minimum value of a numeric variable
     * @param var
     */
    private double getMin(int var){
        double min = Double.POSITIVE_INFINITY;
        for(int i = 0; i < dataset.numInstances(); i++){
            if(dataset.get(i).value(var) < min){
                min = dataset.get(i).value(var);
            }
        }

        return min;
    }

    /**
     * It generates the triangular liguistic labels for covering from min to max using
     * the specified number of linguistic labels
     *
     * @param min
     * @return
     */
    private ArrayList<Fuzzy> generateLinguistcLabels(double min, double max){
        double marca = (max - min) / ((double)(numLabels -1));
        double cutPoint = min + marca / 2;
        ArrayList<Fuzzy> sets = new ArrayList<>();

        for(int label = 0; label < numLabels; label++){
            ArrayList<Double> definitions = new ArrayList<>();
            double value = min + marca * (label -1);

            // Creation of x0 point
            if(label == 0){
                definitions.add(-1 * Double.MAX_VALUE);
            } else {
                definitions.add(Round(value, max));
            }

            // Creation of x1 point
            value = min + marca * label;
            definitions.add(Round(value, max));

            // Creation of x2 point
            value = min + marca * (label + 1);
            if(label == numLabels - 1){
                definitions.add(Double.MAX_VALUE);
            } else {
                definitions.add(Round(value, max));
            }

            // Create de triangular fuzzy set
            TriangularFuzzySet set = new TriangularFuzzySet(definitions, 1.0);
            sets.add(set);

            cutPoint += marca;
        }

        return sets;
    }



    /**
     * <p>
     * Rounds the generated value for the semantics when necesary
     * </p>
     *
     * @param val The value to round
     * @param tope
     * @return
     */
    public double Round(double val, double tope) {
        if (val > -0.0001 && val < 0.0001) {
            return (0);
        }
        if (val > tope - 0.0001 && val < tope + 0.0001) {
            return (tope);
        }
        return (val);
    }

    /**
     * It generates a random indivual initialising a percentage of its variables at random.
     * @param rand
     * @return
     */
    public DefaultBinarySolution OrientedInitialisation(Random rand, double pctVariables){
        DefaultBinarySolution sol = new DefaultBinarySolution(this);
        long maxVariablesToInitialise = Math.round(pctVariables * (dataset.numAttributes() - 1));
        int varsToInit = rand.nextInt((int) maxVariablesToInitialise) + 1;

        BitSet initialised = new BitSet(dataset.numAttributes());
        initialised.set(dataset.classIndex());
        int varInitialised = 0;

        while(varInitialised != varsToInit){
            int var = rand.nextInt(dataset.numAttributes());
            if (!initialised.get(var)){
                BinarySet value = new BinarySet(sol.getNumberOfBits(var));
                for(int i = 0; i < sol.getNumberOfBits(var);i++){
                    if(rand.nextBoolean())
                        value.set(i);
                    else
                        value.clear(i);
                }
                // check if the generated variable is empty and fix it if necessary
                if(value.cardinality() == 0){
                    value.set(rand.nextInt(sol.getNumberOfBits(var)));
                } else  if(value.cardinality() == sol.getNumberOfBits(var)){
                    value.clear(rand.nextInt(sol.getNumberOfBits(var)));
                }
                sol.setVariableValue(var,value);
                varInitialised++;
                initialised.set(var);
            }
        }

        sol.getVariableValue(dataset.classIndex()).clear();
        sol.getVariableValue(dataset.classIndex()).set(rand.nextInt(dataset.numClasses()));

        // clear the non-initialised variables
        for(int i = 0; i < sol.getNumberOfVariables(); i++){
            if(!initialised.get(i)){
                sol.getVariableValue(i).clear();
            }
        }

        return sol;
    }
}
