import org.uma.jmetal.solution.BinarySolution;
import org.uma.jmetal.solution.Solution;

import java.util.BitSet;

public class IndDNF implements Solution<Boolean> {


    // EL INDDNF NO PUEDE SER UN BITSET PORQUE LA FUNCION SETVARIABLEVALUE DEBE ACEPTAR UN BITSET
    // Y ESO NO NOS VALE, TIENEN QUE SER VALORES BOOLEANOS.

    // AQUI IRIAN TODOS LOS ELEMENTOS NECESARIOS PARA CREAR EL GEN DNF CON TODAS SUS OPTIMIZACIONES.

    @Override
    public void setObjective(int index, double value) {

    }

    @Override
    public double getObjective(int index) {
        return 0;
    }

    @Override
    public double[] getObjectives() {
        return new double[0];
    }

    @Override
    public Boolean getVariableValue(int index) {
        return null;
    }

    @Override
    public void setVariableValue(int index, Boolean value) {

    }

    @Override
    public String getVariableValueString(int index) {
        return null;
    }

    @Override
    public int getNumberOfVariables() {
        return 0;
    }

    @Override
    public int getNumberOfObjectives() {
        return 0;
    }

    @Override
    public Solution<Boolean> copy() {
        return null;
    }

    @Override
    public void setAttribute(Object id, Object value) {

    }

    @Override
    public Object getAttribute(Object id) {
        return null;
    }
}
