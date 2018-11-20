package operators.mutation;

import org.uma.jmetal.operator.MutationOperator;
import org.uma.jmetal.solution.BinarySolution;
import org.uma.jmetal.util.pseudorandom.JMetalRandom;

import java.util.Random;

public class BiasedMutationDNF implements MutationOperator<BinarySolution> {

    /**
     * The mutation probability
     */
    double mutationProb;

    /**
     * The random number generator. It must be consistent with the whole problem
     */
    private JMetalRandom rand;


    public BiasedMutationDNF(double mutProb, JMetalRandom rand){

        this.mutationProb = mutProb;
        this.rand = rand;
    }

    @Override
    public BinarySolution execute(BinarySolution binarySolution) {
        return doMutation(binarySolution);
    }

    private BinarySolution doMutation(BinarySolution binarySolution) {
        BinarySolution sol = (BinarySolution) binarySolution.copy();

        if(rand.nextDouble(0.0, 1.0) <= mutationProb) {
            int var = rand.nextInt(0, sol.getNumberOfVariables() - 1);

            if (rand.nextDouble(0.0, 1.0) < 0.5) {
                // remove variable
                sol.getVariableValue(var).clear();
            } else {
                // random flip of variables
                for (int i = 0; i < sol.getNumberOfBits(var); i++) {
                    if (rand.nextDouble(0.0,1.0) < 0.5) {
                        sol.getVariableValue(var).flip(i);
                    }
                }
            }
        }

        return sol;
    }


}
