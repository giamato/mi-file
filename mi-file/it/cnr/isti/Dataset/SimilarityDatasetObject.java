/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package it.cnr.isti.Dataset;

/**
 * Abstract class to represent objects compared using similarity in the database. To have dataset working with a particular
 * type of object, simply create a class for that type of object that implements this class.
 * @author Giuseppe Amato
 * ISTI-CNR, Pisa, Italy
 * giuseppe.amato@isti.cnr.it
 */
public interface SimilarityDatasetObject extends DatasetObject{
    /**
     * Computes distance betwen this and another SimilarityDatasetObject (of the same type).
     * @param o Object to compute the distance from.
     * @return distance between this and o.
     */
    public double distance(SimilarityDatasetObject o);
}
