package fuzzy;

import java.util.ArrayList;


/**
 * Class for the definition of a triangular fuzzy set.
 *
 * It has three values, the minimum, the medium and the maximum
 */
public class TriangularFuzzySet extends Fuzzy {

    /**
     * Constructor with the three points defining the fuzzy set within an arraylist
     * @param values The list of points defining the set
     * @param y The maximum belonging degree
     */
    public TriangularFuzzySet(ArrayList<Double> values, double y){
        super(values, y);
    }



    @Override
    public double getBelongingDegree(double x) {
        double x0 = getValue(0);
        double x1 = getValue(1);
        double x2 = getValue(2);

        if( x <= x0 || x >= x2) return 0.0;
        if( x < x1) return ((x - x0) * (getY() / (x1-x0)));
        if( x > x1) return ((x2 - x) * (getY() / (x2-x1)));
        return getY();
    }
}
