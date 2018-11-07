package main;

import evaluator.Evaluator;
import org.uma.jmetal.algorithm.multiobjective.nsgaii.NSGAII;
import org.uma.jmetal.operator.CrossoverOperator;
import org.uma.jmetal.operator.MutationOperator;
import org.uma.jmetal.operator.SelectionOperator;
import org.uma.jmetal.problem.Problem;
import org.uma.jmetal.solution.Solution;
import org.uma.jmetal.util.comparator.DominanceComparator;
import org.uma.jmetal.util.evaluator.SolutionListEvaluator;

import java.util.*;

/**
 * This is the class to be used for adding modfications to the NSGA-II algorithm
 *
 * In the run() function you can add whatever you want. This code is copied from AbstractEvolutionaryAlgorithm
 *
 * @param <S>
 */
public class NSGAIIModifiable<S extends Solution<?>> extends NSGAII<S> {

    public NSGAIIModifiable(Problem problem, int maxEvaluations, int populationSize, CrossoverOperator crossoverOperator, MutationOperator mutationOperator, SelectionOperator selectionOperator, Comparator<S> comparator, SolutionListEvaluator evaluator) {
        super(problem, maxEvaluations, populationSize, crossoverOperator, mutationOperator, selectionOperator, comparator, evaluator);
    }

    @Override
    public void run() {
        List<S> offspringPopulation;
        List<S> matingPopulation;

        initProgress();
        population = createInitialPopulation();
        population = evaluatePopulation(population);

        while (!isStoppingConditionReached()) {
            matingPopulation = selection(population);
            offspringPopulation = reproduction(matingPopulation);
            offspringPopulation = evaluatePopulation(offspringPopulation);

            // Reemplazo basado en fast non-dominated sorting
            population = replacement(population, offspringPopulation);

            // Aquí cosas adicionales como la reinicialización

            // No tocar esto (se actualiza el número de evaluaciones
            updateProgress();
        }

        // Remove repeated individuals (it uses the hashCode() function)
        HashSet<S> set = new HashSet<>();
        getPopulation().forEach(s -> set.add(s));
        ArrayList<S> newPop = new ArrayList<>();
        set.forEach(s -> newPop.add(s));
        ArrayList<Integer> a = new ArrayList<>();
        for(S b : newPop){
            a.add(b.hashCode());
        }
        setPopulation(newPop);
    }

    @Override
    protected List<S> replacement(List<S> population, List<S> offspringPopulation) {
        // Aquí iría el reemplazo basado en best-order sort
        return super.replacement(population, offspringPopulation);
    }

    @Override
    protected void updateProgress() {
        super.updateProgress();

        // Aqui, para el NSGA-II adaptativo, actualizar los porcentajes de probabilidad para la ejecución de cada método
    }

    @Override protected void initProgress() {
        System.out.println("Initialising the Fast-MOEA-BD algorithm...");
        evaluations = getMaxPopulationSize();
        if(evaluator instanceof Evaluator){
            ((Evaluator<S>) evaluator).initialise(this.problem);
        }
    }

    @Override
    protected List<S> reproduction(List<S> population) {
        // Aqui va el metodo de reproduccion que se usa en el NSGA-IIa
        int numberOfParents = crossoverOperator.getNumberOfRequiredParents() ;

        checkNumberOfParents(population, numberOfParents);

        List<S> offspringPopulation = new ArrayList<>(getMaxPopulationSize());
        for (int i = 0; i < getMaxPopulationSize(); i += numberOfParents) {
            List<S> parents = new ArrayList<>(numberOfParents);
            for (int j = 0; j < numberOfParents; j++) {
                parents.add(population.get(i+j));
            }

            List<S> offspring = crossoverOperator.execute(parents);

            for(S s: offspring){
                mutationOperator.execute(s);
                offspringPopulation.add(s);
            }
        }
        return offspringPopulation;
    }
}
