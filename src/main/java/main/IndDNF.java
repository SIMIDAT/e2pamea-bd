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

    /**
     * It determines whether the variable participates in the rule or not.
     *
     * A variable participates in the rules if not all its values are zero or one.
     * @param var
     * @return
     */
    public boolean participates(int var){
        return getVariableValue(var).cardinality() > 0 && getVariableValue(var).cardinality() < getVariableValue(var).getBinarySetLength();
    }

    public int getClas() {
        return clas;
    }

    public void setClas(int clas) {
        this.clas = clas;
    }
}
