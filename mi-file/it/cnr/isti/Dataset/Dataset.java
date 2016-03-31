/*
 * Dataset.java
 *
 * Created on 28 giugno 2007, 9.43
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package it.cnr.isti.Dataset;

/**
 * Interface to represent datasets or databases of objects.
 * @author Giuseppe Amato
 * ISTI-CNR, Pisa, Italy
 * giuseppe.amato@isti.cnr.it
 */
public interface Dataset<DO extends DatasetObject> extends java.io.Serializable{
    
    /**
     * Sets the dataset to some file or directory where data are contained or should be stored
     * @param file file ot directory with the dataset
     * @throws Exception something went wrong
     */
    public void set(String file) throws Exception;
    
    /**
     * Size (number of objects) inthe dataset.
     * @return size of the dataset
     * @throws Exception something went wrong
     */
    public int size()  throws Exception;
    
   

    /**
     * Retrieves an object inthe dataset by using its identifier
     * @param id_p The object identifier
     * @return the obejct corresponding to id_p
     * @throws Exception something went wrong
     */
    public DO getObject(int id_p)  throws Exception;

    /**
     * Insert an object
     * @param obj object to be inserted
     * @return internal id of the object
     * @throws java.io.IOException
     */
    public int insert(DO obj) throws java.io.IOException;

    public void close() throws java.io.IOException;
    
}
