import main.IndDNF;
import org.uma.jmetal.operator.impl.mutation.BitFlipMutation;
import org.uma.jmetal.solution.BinarySolution;
import org.uma.jmetal.util.pseudorandom.RandomGenerator;

public class BitFlipMutationDNF extends BitFlipMutation {

    public BitFlipMutationDNF(double mutationProbability) {
        super(mutationProbability);
    }

    public BitFlipMutationDNF(double mutationProbability, RandomGenerator<Double> randomGenerator) {
        super(mutationProbability, randomGenerator);
    }

    @Override
    public void doMutation(double probability, BinarySolution solution) {
        if(solution instanceof IndDNF){
            super.doMutation(probability, solution);
        } else {
            System.err.println("ERROR: Use of BitFlipMutationDNF class with a chromosome which is not IndDNF.");
            System.exit(-1);
        }

    }
}
