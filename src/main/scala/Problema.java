import com.sun.org.apache.xpath.internal.operations.Bool;
import org.uma.jmetal.problem.BinaryProblem;
import org.uma.jmetal.problem.DoubleProblem;
import org.uma.jmetal.problem.Problem;
import org.uma.jmetal.problem.impl.AbstractBinaryProblem;
import org.uma.jmetal.problem.impl.AbstractGenericProblem;
import org.uma.jmetal.solution.BinarySolution;
import org.uma.jmetal.solution.Solution;
import weka.core.Instances;
import weka.core.converters.ArffLoader;
import weka.core.converters.ConverterUtils.DataSource;

public class Problema implements Problem<IndDNF> {

    // AQUI VAN TODAS LAS FUNCIONES RELACIONES CON EL PROBLEMA
    // AQUI EN EL CONSTRUCTOR SE DEBERÍA DE LEER EL FICHERO DE DATOS Y CARGAR LOS DATOS
    // TAMBIEN SE TIENE QUE IMPLEMENTAR EL METODO DE EVALUACIÓN
    // Y EL METODO DE CREACION DE UN NUEVO INDIVIDUO

    // Con esto se lee el dataset de Arff o csv usando weka
    public void nada() {
        DataSource source;
        Instances data;
        try {
            source = new DataSource("/some/where/data.arff");
            data = source.getDataSet();

        // Con esto se le fija como clase el ultimo atributo si no estuviera especificado
        if (data.classIndex() == -1)
            data.setClassIndex(data.numAttributes() - 1);

        } catch (Exception e) {
            e.printStackTrace();
        }

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
    public int getNumberOfConstraints() {
        return 0;
    }

    @Override
    public String getName() {
        return null;
    }

    @Override
    public void evaluate(IndDNF solution) {

    }

    @Override
    public IndDNF createSolution() {
        return null;
    }



}
