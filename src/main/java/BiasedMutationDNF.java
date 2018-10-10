import org.uma.jmetal.operator.MutationOperator;
import org.uma.jmetal.solution.BinarySolution;

import java.util.Random;

public class BiasedMutationDNF implements MutationOperator<BinarySolution> {

    /**
     * The mutation probability
     */
    double mutationProb;


    public BiasedMutationDNF(double mutProb){
        this.mutationProb = mutProb;
    }

    @Override
    public BinarySolution execute(BinarySolution binarySolution) {
        return doMutation(binarySolution);
    }

    private BinarySolution doMutation(BinarySolution binarySolution) {
        Random rand = new Random();

        if(rand.nextDouble() <= mutationProb) {
            int var = rand.nextInt(binarySolution.getNumberOfVariables());

            if (rand.nextBoolean()) {
                // remove variable
                binarySolution.getVariableValue(var).clear();
            } else {
                // random flip of variables
                for (int i = 0; i < binarySolution.getNumberOfBits(var); i++) {
                    if (rand.nextBoolean()) {
                        binarySolution.getVariableValue(var).flip(i);
                    }
                }
            }
        }

        return binarySolution;
    }


}
