package attributes;

import org.uma.jmetal.solution.Solution;
import org.uma.jmetal.util.solutionattribute.impl.GenericSolutionAttribute;
import qualitymeasures.WRAccNorm;

/**
 * Attribute for measuring the diversity measure
 */
public class DiversityMeasure<S extends Solution<?>> extends GenericSolutionAttribute<S, WRAccNorm> {
}
