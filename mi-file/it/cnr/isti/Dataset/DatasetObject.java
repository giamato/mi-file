/*
 * Object.java
 *
 * Created on 21 aprile 2008, 16.50
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package it.cnr.isti.Dataset;

/**
 * Abstract class to represent objects in the database. To have dataset working with a particular
 * type of object, simply create a class for that type of object that implements this class.
 * @author Giuseppe Amato
 * ISTI-CNR, Pisa, Italy
 * giuseppe.amato@isti.cnr.it
 */
public interface DatasetObject extends java.io.Serializable{

    //private static final long serialVersionUID =2052534557459973320L;

    /**
     * Gets the internal id of this object.
     * @return the internal object identifier.
     * <p>
     * <b>IMPORTANT:</b> Note that the internal identifier is typically assigned/set
     * by the implementation of the dataset itself. It represents the way in which the dataset
     * internally identifies an object.
     */
    public int  getInternalId();
    /**
     * Sets the internal identifier for this object.
     * <p>
     * <b>IMPORTANT:</b> Note that the internal identifier is typically assigned/set
     * by the implementation of the dataset itself. It represents the way in which the dataset
     * internally identifies an object.
     */
    public void setInternalId(int id);
}
