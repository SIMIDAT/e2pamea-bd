import main.IndDNF;
import org.uma.jmetal.operator.impl.crossover.HUXCrossover;
import org.uma.jmetal.solution.BinarySolution;
import org.uma.jmetal.solution.impl.DefaultBinarySolution;
import org.uma.jmetal.util.JMetalException;
import org.uma.jmetal.util.pseudorandom.RandomGenerator;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;

public class HUXCrossoverDNF extends HUXCrossover {
    public HUXCrossoverDNF(double crossoverProbability) {
        super(crossoverProbability);
    }

    public HUXCrossoverDNF(double crossoverProbability, RandomGenerator<Double> randomGenerator) {
        super(crossoverProbability, randomGenerator);
    }

    @Override
    public List<BinarySolution> doCrossover(double probability, BinarySolution parent1, BinarySolution parent2) throws JMetalException {

        if(! (parent1 instanceof  IndDNF)){
            throw  new JMetalException("parent1 in HUXCrossoverDNF is not an INDNF");
        }

        if(! (parent2 instanceof  IndDNF)){
            throw  new JMetalException("parent2 in HUXCrossoverDNF is not an INDNF");
        }

        List<BinarySolution> childs = super.doCrossover(probability, parent1, parent2);

        ArrayList<BinarySolution> toRet = new ArrayList<>();
        toRet.add(new IndDNF((DefaultBinarySolution) childs.get(0), ((IndDNF) parent1).getClas()));
        toRet.add(new IndDNF((DefaultBinarySolution) childs.get(1), ((IndDNF) parent2).getClas()));

        return toRet;

    }
}
