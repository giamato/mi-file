/*
 * Object.java
 *
 * Created on 21 aprile 2008, 16.50
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package it.cnr.isti.SimilaritySearch.Dataset;

/**
 * Abstract class to represent objects in the MI_File. To have an MI_File working with a particular
 * type of object, simply create a class for that type of object that extends this class.
 * Note that objects MUST be serializable, since they are stored in the internal database of the MI_File.
 * The internal database of the MI_File (see DatasetInDatabase class) implements the Dataset interface.
 * @author Giuseppe Amato
 * ISTI-CNR, Pisa, Italy
 * giuseppe.amato@isti.cnr.it
 */
public abstract class DatasetObject implements java.io.Serializable{

    private static final long serialVersionUID =2052534557459973320L;
    
    /**
     * Computes distance betwen this and another DatasetObject (of the same type).
     * @param o Object to compute the distance from.
     * @return distance between this and o.
     */
    public abstract double distance(DatasetObject o);

    /**
     * Gets the internal id of this object.
     * @return the internal object identifier.
     * <p>
     * <b>IMPORTANT:</b> Note that the internal identifier is typically assigned/set
     * by an index structure when the object is inserted in the index itself. For instance the MI_File
     * uses the setInternalId() method when an object is inserted in the index to associate
     * the objects with the internal identifier assigned by the MI_File.
     */
    public abstract int  getInternalId();
    /**
     * Sets the internal identifier for this object.
     * <p>
     * <b>IMPORTANT:</b> Note that the internal identifier is assigned/set
     * by the MI-File when the object is inserted in the index itself. The MI-File
     * uses the setInternalId() method when an object is inserted in the index to associate
     * the objects with the internal identifier assigned by the MI-File.
     * Therefore using this method for other purpose might be dangerous and must be done carefully.
     * @param id The obejct identifier.
     */
    public abstract void setInternalId(int id);
}
