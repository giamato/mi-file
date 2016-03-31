/*
 * DatasetInDatabase.java
 *
 * Created on 16 maggio 2008, 9.09
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package it.cnr.isti.PersistentDataset;

import it.cnr.isti.Dataset.*;

/**
 * This class implements the Dataset interfaces and represents/manages the database of objects inserted in the MI_File.
 * Each object is associated with an internal identifier, which should not be confused with the
 * (external) object identifier that can be obtained by object.getInternalId(). Specifically, the getObjectFromOffset(id)
 * method of this class retrieves the object associated with the internal identifier id, which migh
 * be different than the external object identifier. In other words, id == getObjectFromOffset(id).getInternalId().
 *
 * Typically this class is not used directly, and it is mainly used by the internal algorithms of the MI_File.
 *
 *
 * @author Giuseppe Amato
 * ISTI-CNR, Pisa, Italy
 * giuseppe.amato@isti.cnr.it
 */
public class PersistentSimilarityDataset<DO extends PersistentSimilarityDatasetObject> extends PersistentDataset implements SimilarityDataset{


     /** Creates a new instance of PersistentSimilarityDataset.
     * @param indexDirectory_p the directory where the index is stored
     * @param mode "r","rw","rws",rwd"
     * @throws java.io.IOException there were problems in opening the needed files
     */
    public PersistentSimilarityDataset(String indexDirectory_p,String mode) throws java.io.IOException{
        super(indexDirectory_p,mode);
    }

    /** Creates a new instance of PersistentSimilarityDataset in the specified directory.
     * The database is open in read-only.
     * @param directory_p directory where the database is created.
     */
    public PersistentSimilarityDataset(String indexDirectory_p) throws java.io.IOException{
        super(indexDirectory_p);
    }

    @Override
    public DO getObject(int i) throws Exception{
        return (DO)super.getObject(i);
    }

     /**
     * Computes distance between two objects of the dataset
     * @param o1_p first object
     * @param o2_p second object
     * @return distance between o1 and o2
     */
    public double distance(SimilarityDatasetObject o1_p, SimilarityDatasetObject o2_p){
        DO o1=(DO) o1_p;
        DO o2=(DO) o2_p;
        return o1.distance(o2);
    }

    /**
     * Compute and prints the density of distances in the database
     */
    public void computeDistanceDensities(double max_dist) throws Exception{
        int bin_num=100;
        int testedSet=10000;
        double bin_size=max_dist/(double)bin_num;
        int[] dd=null;
        System.out.println("Computing distance densities...");
        DO[] objects=(DO[])new PersistentSimilarityDatasetObject[testedSet];
        dd=new int[bin_num];
        for(int i=0;i<bin_num;i++)
            dd[i]=0;
        for (int i=0; i<testedSet;i++){
            objects[i]=getObject(i);
        }

        for (int i=0; i<testedSet;i++)
        {
         for(int j=0;j<testedSet;j++)
         {
             double dist=distance(objects[i],objects[j]);
//             System.out.println(dist);
             int bin;
             if (dist >= max_dist){
                 bin=bin_num-1;
             } else
                 bin=(int)(dist/bin_size);
//                 dd[i][dist/bin_size]=dd[i][dist/bin_size]+1;
             dd[bin]=dd[bin]+1;        
         }
         //System.out.println(i);
        }
        double sum=0;
        for(int i=0;i<bin_num;i++){
            double dist=(i+1)*bin_size;
            double density=dd[i]/(double)(testedSet*testedSet);
            sum+=density;
            System.out.println(dist+"\t"+density);
        }
        System.out.println(sum);
    }  
}
