package attributes;

import org.uma.jmetal.solution.Solution;
import org.uma.jmetal.util.solutionattribute.impl.GenericSolutionAttribute;
import qualitymeasures.QualityMeasure;

import java.util.ArrayList;

public class TestMeasures<S extends Solution<?>> extends GenericSolutionAttribute<S, ArrayList<QualityMeasure>> {
}
