/*
 * Dataset.java
 *
 * Created on 28 giugno 2007, 9.43
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package it.cnr.isti.SimilaritySearch.Dataset;

/**
 * Interface to represent datasets or databases of objects.
 * @author Giuseppe Amato
 * ISTI-CNR, Pisa, Italy
 * giuseppe.amato@isti.cnr.it
 */
public interface Dataset extends java.io.Serializable{
    
    /**
     * Computes distance between two objects of the dataset
     * @param o1 first object
     * @param o2 second object
     * @return distance between o1 and o2
     */
    public double distance(DatasetObject o1, DatasetObject o2);
    
    /**
     * Sets the dataset to some file or directory where data are contained or should be stored
     * @param file file ot directory with the dataset
     */
    public void set(String file);
    
    /**
     * Size (number of objects) inthe dataset.
     * @return size of the dataset
     */
    public int size();
    
    /**
     * Saves (exports) the dataset in a file or directory
     * @param file file or directory where the dataset is stored
     */
    public void save(String file);
    
    /**
     * Sets the dataset to some binary file where data are contained or should be stored
     * @param file file with the binary dataset
     */
    public void binarySet(String file);

    /**
     * Retrieves an object inthe dataset by using its identifier
     * @param id_p The object identifier
     * @return the obejct corresponding to id_p
     */
    public DatasetObject getObject(int id_p);
    
}
