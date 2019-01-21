package main;

import attributes.Clase;
import attributes.Coverage;
import attributes.DiversityMeasure;
import attributes.GeneratedOperator;
import evaluator.Evaluator;
import evaluator.EvaluatorMapReduce;
import filters.MeasureFilter;
import filters.TokenCompetitionFilter;
import operators.selection.RankingAndCrowdingSelection;
import org.uma.jmetal.algorithm.multiobjective.nsgaii.NSGAII;
import org.uma.jmetal.operator.CrossoverOperator;
import org.uma.jmetal.operator.MutationOperator;
import org.uma.jmetal.operator.Operator;
import org.uma.jmetal.operator.SelectionOperator;
import org.uma.jmetal.problem.Problem;
import org.uma.jmetal.solution.BinarySolution;
import org.uma.jmetal.solution.Solution;
import org.uma.jmetal.util.JMetalException;
import org.uma.jmetal.util.evaluator.SolutionListEvaluator;
import org.uma.jmetal.util.solutionattribute.impl.DominanceRanking;
import qualitymeasures.Confidence;
import qualitymeasures.QualityMeasure;
import reinitialisation.NonEvolutionReinitialisation;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * This is the class to be used for adding modfications to the NSGA-II algorithm
 * <p>
 * In the run() function you can add whatever you want. This code is copied from AbstractEvolutionaryAlgorithm
 *
 * @param <S>
 */
public class NSGAIIModifiable<S extends Solution<?>> extends NSGAII<S> {

    private List<S> elitePopulation;

    /**
     * The operators to be employed within the genetic algorithm
     */
    private ArrayList<Operator> operators;

    /**
     * The probability of application of the operators
     */
    private ArrayList<Double> applicationProbabilities;

    /**
     * It counts the number of contributions each operator did
     */
    private ArrayList<Integer> contributions;


    private NonEvolutionReinitialisation<S> reinitialisation;

    /**
     * The quality measure for the filter
     */
    private QualityMeasure filter;

    /**
     * The threshold of the filter
     */
    private double filterThreshold;


    /**
     * The minimum count threshold to be set for updating the probabilities.
     */
    private int threshold = 2;


    public QualityMeasure getFilter() {
        return filter;
    }

    public void setFilter(QualityMeasure filter) {
        this.filter = filter;
    }

    public double getFilterThreshold() {
        return filterThreshold;
    }

    public void setFilterThreshold(double filterThreshold) {
        this.filterThreshold = filterThreshold;
    }


    public NSGAIIModifiable(Problem problem, int maxEvaluations, int populationSize, CrossoverOperator crossoverOperator, MutationOperator mutationOperator, SelectionOperator selectionOperator, Comparator<S> comparator, SolutionListEvaluator evaluator) {
        super(problem, maxEvaluations, populationSize, crossoverOperator, mutationOperator, selectionOperator, comparator, evaluator);
        operators = new ArrayList<>();
        operators.add(crossoverOperator);
        operators.add(mutationOperator);
        applicationProbabilities = new ArrayList<>();
        contributions = new ArrayList<>();
        for (Operator o : operators) {
            applicationProbabilities.add(0.0);
            contributions.add(0);
        }
        setElitePopulation(new ArrayList<>());
        reinitialisation = new NonEvolutionReinitialisation<>(((Double) (maxEvaluations * 0.25)).intValue(), ((BigDataEPMProblem) problem).getNumberOfClasses(), ((BigDataEPMProblem) problem).numExamples());
    }

    @Override
    public void run() {
        List<S> offspringPopulation;
        List<S> matingPopulation;

        initProgress();
        population = createInitialPopulation();
        population = evaluatePopulation(population);


        // Evolutionary process MAIN LOOP:
        int gen = 0;

        while (!isStoppingConditionReached()) {
            //matingPopulation = selection(population);

            offspringPopulation = reproduction(population);
            offspringPopulation = evaluatePopulation(offspringPopulation);

            // Reemplazo basado en fast non-dominated sorting
            population = replacement(population, offspringPopulation);

            // Aquí cosas adicionales como la reinicialización
            int numClasses = ((BigDataEPMProblem) problem).getNumberOfClasses();
            for(int i = 0; i < numClasses; i++) {
                if (reinitialisation.checkReinitialisation(population, problem, evaluations, i )) {
                    System.out.println("Re-inicio en clase: " + i + " evaluaciones: " + evaluations + "/" + maxEvaluations);
                    population = reinitialisation.doReinitialisation(population, problem, evaluations, i, this);
                    population = evaluatePopulation(population);
                    evaluations += getMaxPopulationSize();
                }
            }

            if(gen == 0){
                // If it is the first population, the elite will be the actual population.
                population.forEach(x -> elitePopulation.add((S) x.copy()));
            }

            // No tocar esto (se actualiza el número de evaluaciones y las probabilidades de aplicación de cada método)
            updateProgress(); gen++;
        }

        // At the end. Perform a token competition procedure and return
        population = evaluatePopulation(population);
        population = replacement(population, population);
        int numClasses = ((BigDataEPMProblem) problem).getNumberOfClasses();
        TokenCompetitionFilter<S> tc = new TokenCompetitionFilter<>();
        MeasureFilter<S> filter = new MeasureFilter<>(getFilter(), filterThreshold);

        List<S> newElite = new ArrayList<>();
        for(int i = 0; i < numClasses; i++){
            final int clas = i;
            List<S> pop = new ArrayList<>();
            pop.addAll(elitePopulation.stream().filter(x -> (int) x.getAttribute(Clase.class) == clas).collect(Collectors.toList()));
            List<S> a = population.stream().filter(x -> (int) x.getAttribute(Clase.class) == clas && (int) x.getAttribute(DominanceRanking.class) == 0).collect(Collectors.toList());
            pop.addAll(a);

            // Do token competiton
            pop = tc.doFilter(pop,i, (EvaluatorMapReduce) evaluator);

            // Sort the result by dominance and return the pareto front
            RankingAndCrowdingSelection<S> rankingAndCrowdingSelection = new RankingAndCrowdingSelection<>(pop.size(), dominanceComparator);
            pop = rankingAndCrowdingSelection.execute(pop)
                    .stream()
                    .filter(x -> (int) x.getAttribute(DominanceRanking.class) == 0)
                    .collect(Collectors.toList());

            double avgDiversityPop = pop.stream().mapToDouble(x -> ((QualityMeasure) x.getAttribute(DiversityMeasure.class)).getValue()).sum() / (double) pop.size();
            double currentAvgDiversityElite = elitePopulation
                    .stream()
                    .filter(x -> (int) x.getAttribute(Clase.class) == clas)
                    .mapToDouble(x -> ((QualityMeasure) x.getAttribute(DiversityMeasure.class)).getValue())
                    .sum() / (double) elitePopulation.stream().filter(x -> (int) x.getAttribute(Clase.class) == clas).count();

            // Substitute the current elite if and only if the avg wracc is better
            if(avgDiversityPop > currentAvgDiversityElite){
                pop = filter.doFilter(pop,i, (EvaluatorMapReduce) evaluator);
            } else if (avgDiversityPop == currentAvgDiversityElite){
                if(pop.size() < elitePopulation.stream().filter(x -> (int) x.getAttribute(Clase.class) == clas).count()){
                    pop = filter.doFilter(pop,i, (EvaluatorMapReduce) evaluator);
                }
            } else {
                pop = filter.doFilter(elitePopulation.stream().filter(x -> (int) x.getAttribute(Clase.class) == clas).collect(Collectors.toList()), i, (EvaluatorMapReduce) evaluator);
            }
            newElite.addAll(pop);

        }

        elitePopulation = newElite;

        // Apply the filter of confidence
        QualityMeasure filterMeasure = new Confidence();
    }

    @Override
    protected List<S> replacement(List<S> population, List<S> offspringPopulation) {
        int numClasses = ((BigDataEPMProblem) problem).getNumberOfClasses();
        List<S> toReturn = new ArrayList<>();
        for(int i = 0 ; i < numClasses; i++){
            final int clase = i;
            List<S> filterPop = population.stream().filter(x -> (int) x.getAttribute(Clase.class) == clase).collect(Collectors.toList());
            List<S> filterOff = offspringPopulation.stream().filter(x -> (int) x.getAttribute(Clase.class) == clase).collect(Collectors.toList());

            // Aquí iría el reemplazo basado en best-order sort
            List<S> jointPopulation = new ArrayList<>();
            jointPopulation.addAll(filterPop);
            jointPopulation.addAll(filterOff);

            RankingAndCrowdingSelection<S> rankingAndCrowdingSelection ;
            rankingAndCrowdingSelection = new RankingAndCrowdingSelection<S>(filterPop.size(), dominanceComparator);
            toReturn.addAll(rankingAndCrowdingSelection.execute(jointPopulation));
        }


        return  toReturn;
    }

    @Override
    protected void updateProgress() {
        super.updateProgress();
        // Aqui, para el NSGA-II adaptativo, actualizar los porcentajes de probabilidad para la ejecución de cada método
        double total = 0;
        for (int i = 0; i < contributions.size(); i++) {
            if (contributions.get(i) < threshold) {
                contributions.set(i, threshold);
            }

            total += contributions.get(i);
        }

        for (int i = 0; i < contributions.size(); i++) {
            applicationProbabilities.set(i, (double) contributions.get(i) / total);
            contributions.set(i, 0);
        }


    }

    @Override
    protected void initProgress() {
        System.out.println("Initialising the Fast-MOEA-BD algorithm...");
        evaluations = getMaxPopulationSize();
        if (evaluator instanceof Evaluator) {
            ((Evaluator<S>) evaluator).initialise(this.problem);
        }

        // set the same application probability to each operator
        double prob = 1.0 / (double) operators.size();
        for (int i = 0; i < applicationProbabilities.size(); i++) {
            applicationProbabilities.set(i, prob);
        }
    }

    @Override
    protected List<S> reproduction(List<S> population) {

        List<S> offspringPopulation = new ArrayList<>();
        int numClasses = ((BigDataEPMProblem) problem).getNumberOfClasses();

        for (int i = 0; i < numClasses; i++) {
            final int clase = i;
            List<S> filterPop = population.stream().filter(x -> (int) x.getAttribute(Clase.class) == clase).collect(Collectors.toList());
            int count = 0;
            while (count < filterPop.size()) {
                // Apply the different operators according to its probability.
                double prob = ((BigDataEPMProblem) problem).rand().nextDouble(0.0, 1.0);

                int index = getIndexOfOperator(prob);
                if (operators.get(index) instanceof CrossoverOperator) {
                    CrossoverOperator operator = (CrossoverOperator) operators.get(index);
                    int numParents = operator.getNumberOfRequiredParents();


                    // Select the parents
                    ArrayList<S> parents = new ArrayList<>();
                    for (int j = 0; j < numParents; j++) {
                        S execute = selectionOperator.execute(filterPop);

                        parents.add(execute);
                    }

                    // Apply the crossover operator.
                    List<S> result = (List<S>) operator.execute(parents);
                    for (S res : result) {
                        for(int k = 0; k < res.getNumberOfObjectives(); k++){
                            res.setObjective(k, Double.NEGATIVE_INFINITY);
                        }
                        contributions.set(index, contributions.get(index) + 1);
                        count++;
                    }
                    offspringPopulation.addAll(result);
                } else if (operators.get(index) instanceof MutationOperator) {
                    MutationOperator operator = (MutationOperator) operators.get(index);

                    S result = (S) operator.execute(selectionOperator.execute(filterPop));

                    for(int k = 0; k < result.getNumberOfObjectives(); k++){
                        result.setObjective(k, Double.NEGATIVE_INFINITY);
                    }
                    offspringPopulation.add(result);
                    contributions.set(index, contributions.get(index) + 1);
                    count++;

                } else {
                    throw new JMetalException("Esto que essss??");
                }
            }
        }

        if (offspringPopulation.size() != getMaxPopulationSize()) {
            while (offspringPopulation.size() != getMaxPopulationSize())
                offspringPopulation.remove(offspringPopulation.size() - 1);
        }

        return offspringPopulation;

    }


    /**
     * It adds a new operator in the algorithm
     *
     * @param ops
     */
    public void addOperators(ArrayList<Operator> ops) {
        for (Operator op : ops) {
            operators.add(op);
            applicationProbabilities.add(0.0);
            contributions.add(0);
        }
    }


    /**
     * It gets the index of the operator that will be applied according to the given probability
     *
     * @param prob
     * @return the index
     */
    public int getIndexOfOperator(double prob) {
        // Get the cummulative sum of probabilities
        double[] cummulativeProbs = new double[applicationProbabilities.size()];
        cummulativeProbs[0] = applicationProbabilities.get(0);
        for (int i = 1; i < cummulativeProbs.length; i++) {
            cummulativeProbs[i] = cummulativeProbs[i - 1] + applicationProbabilities.get(i);
        }

        // Get the index of the operator that match the probability.
        int index;
        for (index = 0; index < cummulativeProbs.length; index++) {
            if (index == 0) {
                if (prob >= 0.0 && prob < cummulativeProbs[index]) {
                    break;
                }
            } else {
                if (prob >= cummulativeProbs[index - 1] && prob < cummulativeProbs[index]) {
                    break;
                }
            }
        }
        return index;
    }

    /**
     * The elite population of this algorithm
     */
    public List<S> getElitePopulation() {
        return elitePopulation;
    }

    public void setElitePopulation(List<S> elitePopulation) {
        this.elitePopulation = elitePopulation;
    }

    public SolutionListEvaluator<S> getEvaluator(){
        return super.evaluator;
    }


    @Override
    public List<S> getResult() {
        return elitePopulation;
    }

    public Comparator<S> getDominanceComparator(){
        return this.dominanceComparator;
    }
}
