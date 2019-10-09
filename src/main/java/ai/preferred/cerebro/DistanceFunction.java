package ai.preferred.cerebro;

import java.io.Serializable;

/**
 * Calculates distance between 2 items.
 *
 //* @param <T> Type of distance between items (expect any numeric type: float, double, int, ..)
 */
@FunctionalInterface
public interface DistanceFunction<TVector> extends Serializable {

    /**
     * Gets the distance between 2 items.
     *
     * @param u from item
     * @param v to item
     * @return The distance between items.
     */
    double distance(TVector u, TVector v);

}
