/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package it.cnr.isti.SimilaritySearch.MI_File;

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
public class DatasetInDatabaseCompressed extends DatasetInDatabase{

    public DatasetInDatabaseCompressed(String directory_p) {
        super(directory_p);
        try{
            objDb=new ObjectDatabaseCompressed(directory_p);
        }catch(Exception e){
            e.printStackTrace();
        }
    }

}
