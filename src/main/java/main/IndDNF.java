package main;

import org.uma.jmetal.problem.BinaryProblem;
import org.uma.jmetal.solution.BinarySolution;
import org.uma.jmetal.solution.Solution;
import org.uma.jmetal.solution.impl.DefaultBinarySolution;
import org.uma.jmetal.util.binarySet.BinarySet;
import weka.core.Attribute;
import weka.core.Instances;

import java.util.BitSet;
import java.util.Random;

public class IndDNF extends DefaultBinarySolution {

    /**
     * The class that this individual belongs to.
     */
    private int clas;


    public IndDNF(BinaryProblem problem) {
        super(problem);
        Random a = new Random(((Problema)problem).getSeed());
        clas = a.nextInt(((Problema) problem).getDataset().numClasses());
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


    /**
     * It returns if a rule is empty
     * @return
     */
    public boolean isEmpty(){
        for(int i = 0; i < getNumberOfVariables(); i++){
            if(participates(i)){
              return false;
            }
        }

        return true;
    }

    /**
     * It returns the class assigned to the individual
     * @return
     */
    public int getClas() {
        return clas;
    }

    /**
     * Setter method for the class
     * @param clas
     */
    public void setClas(int clas) {
        this.clas = clas;
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }


    /**
     * It prints the individual in a human-readable way
     *
     * @param problem
     * @return
     */
    public String print(BinaryProblem problem){
        String toRet = "";
        Problema problema = (Problema) problem;
        if(isEmpty()) return "Empty rule";

        for(int i = 0; i < this.getNumberOfVariables(); i++){
            if(participates(i)){
                Attribute attr = problema.getDataset().attribute(i);
                toRet += "Variable " +  attr.name() + ":\t";

                if(attr.isNominal()){
                    for(int j = 0; j < getNumberOfBits(i); i++) {
                        if (getVariableValue(i).get(j)) {
                            toRet += attr.value(j) + "\t";
                        }
                    }
                } else if(attr.isNumeric()) {
                    for(int j = 0; j < getNumberOfBits(i); i++) {
                        if (getVariableValue(i).get(j)) {
                            toRet += "Label " + i + " = " + problema.getFuzzySets().get(i).get(j).toString() + "\t";
                        }
                    }
                }
            }
        }

        toRet += "Class = " + problema.getDataset().classAttribute().value(clas);

        return toRet;
    }
}
