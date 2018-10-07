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

import java.util.Random;

public class Problema implements Problem<IndDNF> {

    // AQUI VAN TODAS LAS FUNCIONES RELACIONES CON EL PROBLEMA
    // AQUI EN EL CONSTRUCTOR SE DEBERÍA DE LEER EL FICHERO DE DATOS Y CARGAR LOS DATOS
    // TAMBIEN SE TIENE QUE IMPLEMENTAR EL METODO DE EVALUACIÓN
    // Y EL METODO DE CREACION DE UN NUEVO INDIVIDUO

    /**
     * The dataset. It contains all the information about the instances
     */
    private Instances dataset;

    /**
     * The number of objectives to be used in a MOEA algorithm.
     */
    private int numObjectives;


    /**
     *  The initialisation method used
     */
    private String initialisationMethod;

    /**
     * The seed of the random number generator
     */
    private int seed;

    /**
     * It reads an ARFF or CSV file using the WEKA API for reading files.
     * @param path The path of the dataset
     */
    public void readDataset(String path) {
        DataSource source;
        seed = 1; // cambiar
        try {
            source = new DataSource(path);
            dataset = source.getDataSet();
        // Con esto se le fija como clase el ultimo atributo si no estuviera especificado
        if (dataset.classIndex() == -1)
            dataset.setClassIndex(dataset.numAttributes() - 1);

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Override
    public int getNumberOfVariables() {
        return dataset.numAttributes() - 1;
    }

    @Override
    public int getNumberOfObjectives() {
        return 2;
    }

    @Override
    public int getNumberOfConstraints() {
        return 0;
    }

    @Override
    public String getName() {
        return dataset.relationName();
    }

    @Override
    public void evaluate(IndDNF solution) {
        for(int i = 0; i < dataset.numAttributes(); i++){
            int index = i;
            if(i == dataset.classIndex()){
                index--;
            }

            // Evaluation code starts here, use "index" to access the variables of the dataset.
        }
    }

    @Override
    public IndDNF createSolution() {
        // Create a random individual
        Random rand = new Random(seed);
        if(initialisationMethod.equalsIgnoreCase("random")){
            IndDNF a = new IndDNF();
            for(int i = 0; i < a.getNumberOfVariables(); i++){
                a.setVariableValue(i, rand.nextBoolean());
            }
            return a;
        }
        return null;
    }



}
