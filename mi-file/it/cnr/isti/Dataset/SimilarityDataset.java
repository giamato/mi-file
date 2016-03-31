/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package it.cnr.isti.Dataset;

/**
 * Interface to represent datasets or databases of objects compared using similarity functions.
 * @author Giuseppe Amato
 * ISTI-CNR, Pisa, Italy
 * giuseppe.amato@isti.cnr.it
 */
public interface SimilarityDataset<DO extends SimilarityDatasetObject> extends Dataset{
/**
     * Computes distance between two objects of the dataset
     * @param o1 first object
     * @param o2 second object
     * @return distance between o1 and o2
     */
    public double distance(SimilarityDatasetObject o1, SimilarityDatasetObject o2);

    /**
     * Retrieves an object inthe dataset by using its identifier
     * @param id_p The object identifier
     * @return the obejct corresponding to id_p
     * @throws Exception something went wrong
     */
    public DO getObject(int id_p)  throws Exception;
}
