package fuzzy;

import java.lang.reflect.Array;
import java.util.ArrayList;

public abstract class Fuzzy {

    /**
     * The maximum belonging degree to be returned
     */
    private double y;

    /**
     * The values that defines the fuzzy set.
     */
    protected ArrayList<Double> values;


    public Fuzzy(ArrayList<Double> values, double y){
        this.values = (ArrayList) values.clone();
        this.y = y;
    }


    public double getY() {
        return y;
    }

    public void setY(double y) {
        this.y = y;
    }


    public double getValue(int index){
        return values.get(index);
    }

    public void setValue(int index, double value){
        values.set(index, value);
    }


    /**
     * It returns the belonging degree of the value of {@code x} with respect to this fuzzy set.
     * @param x
     * @return
     */
    public abstract double getBelongingDegree(double x);

    public abstract String toString();

}
