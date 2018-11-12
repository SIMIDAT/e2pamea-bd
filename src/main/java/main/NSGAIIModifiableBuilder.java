package main;

import org.uma.jmetal.operator.CrossoverOperator;
import org.uma.jmetal.operator.MutationOperator;
import org.uma.jmetal.operator.Operator;
import org.uma.jmetal.operator.SelectionOperator;
import org.uma.jmetal.problem.Problem;
import org.uma.jmetal.solution.Solution;
import org.uma.jmetal.util.evaluator.SolutionListEvaluator;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class NSGAIIModifiableBuilder<S extends Solution<?>> {
    private Problem<S> problem;
    private int maxEvaluations;
    private int populationSize;
    private CrossoverOperator<S> crossoverOperator;
    private MutationOperator<S> mutationOperator;
    private SelectionOperator<List<S>, S> selectionOperator;
    private SolutionListEvaluator<S> evaluator;
    private Comparator<S> comparator;
    private ArrayList<Operator> operators;

    public NSGAIIModifiableBuilder<S> addOperator(Operator op){
        if(operators == null){
            operators = new ArrayList<>();
        }
        operators.add(op);
        return this;
    }

    public NSGAIIModifiableBuilder<S> setProblem(Problem<S> problem) {
        this.problem = problem;
        return this;
    }

    public NSGAIIModifiableBuilder<S> setDominanceComparator(Comparator<S> comparator){
        this.comparator = comparator;
        return this;
    }

    public NSGAIIModifiableBuilder<S> setMaxEvaluations(int maxEvaluations) {
        this.maxEvaluations = maxEvaluations;
        return this;
    }

    public NSGAIIModifiableBuilder<S> setPopulationSize(int populationSize) {
        this.populationSize = populationSize;
        return this;
    }

    public NSGAIIModifiableBuilder<S> setCrossoverOperator(CrossoverOperator<S> crossoverOperator) {
        this.crossoverOperator = crossoverOperator;
        return this;
    }

    public NSGAIIModifiableBuilder<S> setMutationOperator(MutationOperator<S> mutationOperator) {
        this.mutationOperator = mutationOperator;
        return this;
    }

    public NSGAIIModifiableBuilder<S> setSelectionOperator(SelectionOperator<List<S>, S> selectionOperator) {
        this.selectionOperator = selectionOperator;
        return this;
    }

    public NSGAIIModifiableBuilder<S> setEvaluator(SolutionListEvaluator<S> evaluator) {
        this.evaluator = evaluator;
        return this;
    }

    public NSGAIIModifiable<S> build() {
        NSGAIIModifiable<S> algorithm =  new NSGAIIModifiable<S>(problem, maxEvaluations, populationSize, crossoverOperator, mutationOperator, selectionOperator, comparator, evaluator);
        if(operators != null)
            algorithm.addOperators(operators);
        return algorithm;
    }
}