/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package it.cnr.isti.PersistentDataset;
import it.cnr.isti.Dataset.*;

/**
 * This class is equivalent to the DatasetInDatabase classes. The only difference is that the database content is compressed.
 *
 * Typically this class is not used directly, and it is mainly used by the internal algorithms of the MI_File.
 *
 *
 * @author Giuseppe Amato
 * ISTI-CNR, Pisa, Italy
 * giuseppe.amato@isti.cnr.it
 */
public class PersistentSimilarityDatasetCompressed<DO extends PersistentSimilarityDatasetObject> extends PersistentDatasetCompressed implements SimilarityDataset{

    /** Creates a new instance of PersistentSimilarityDatasetCompressed in the specified directory.
     * @param directory_p directory where the database is created.
     * @param mode the access mode: "r","rw","rws",rwd"
     * @throws java.io.IOException there were problems in opening the needed files
     */
    public PersistentSimilarityDatasetCompressed(String directory_p,String mode) throws java.io.IOException{
        super(directory_p,mode);
    }

    /** Creates a new instance of PersistentSimilarityDatasetCompressed in the specified directory.
     * @param directory_p directory where the database is created.
     * @param mode the access mode: "r","rw","rws",rwd""
     */
    public PersistentSimilarityDatasetCompressed(String directory_p) throws java.io.IOException{
        super(directory_p);
    }

    @Override
    public DO getObject(int i) throws Exception{
        return (DO)super.getObject(i);
    }

     /**
     * Computes distance between two objects of the dataset
     * @param o1 first object
     * @param o2 second object
     * @return distance between o1 and o2
     */
    public double distance(SimilarityDatasetObject o1, SimilarityDatasetObject o2){
        return o1.distance(o2);
    }

}
