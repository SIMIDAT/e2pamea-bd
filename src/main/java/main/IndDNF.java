package main;

import org.uma.jmetal.problem.BinaryProblem;
import org.uma.jmetal.solution.BinarySolution;
import org.uma.jmetal.solution.Solution;
import org.uma.jmetal.solution.impl.DefaultBinarySolution;
import org.uma.jmetal.util.binarySet.BinarySet;
import weka.core.Instances;

import java.util.BitSet;

public class IndDNF extends DefaultBinarySolution {

    /**
     * The class that this individual belongs to.
     */
    private int clas;


    public IndDNF(BinaryProblem problem) {
        super(problem);
    }
}
