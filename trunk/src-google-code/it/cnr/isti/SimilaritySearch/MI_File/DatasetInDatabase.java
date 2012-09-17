/*
 * DatasetInDatabase.java
 *
 * Created on 16 maggio 2008, 9.09
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package it.cnr.isti.SimilaritySearch.MI_File;

import it.cnr.isti.SimilaritySearch.Dataset.DatasetObject;
import it.cnr.isti.SimilaritySearch.Dataset.Dataset;

/**
 * This class implements the Dataset interfaces and represents/manages the database of objects inserted in the MI_File.
 * Each object is associated with an internal identifier, which should not be confused with the
 * (external) object identifier that can be obtained by object.getInternalId(). Specifically, the getObject(id)
 * method of this class retrieves the object associated with the internal identifier id, which migh
 * be different than the external object identifier. In other words, id != getObject(id).getInternalId().
 *
 * Typically this class is not used directly, and it is mainly used by the internal algorithms of the MI_File.
 *
 *
 * @author Giuseppe Amato
 * ISTI-CNR, Pisa, Italy
 * giuseppe.amato@isti.cnr.it
 */
public class DatasetInDatabase implements Dataset{
    
    protected ObjectDatabase objDb;
    
    /** Creates a new instance of DatasetInDatabase in the specified directory.
     * @param directory_p directory where the database is created (in most cases the same directory of the MI_FIle index).
     */
    public DatasetInDatabase(String directory_p) {
        try{
            objDb=new ObjectDatabase(directory_p);
        }catch(Exception e){
            e.printStackTrace();
        } 
    }
    
     /** Sets the current database to the one stored in the specified directory.
      * set and binarySet have exactly the same behaviour.
     * @param directory_p directory where the database is created (in most cases the same directory of the MI_FIle index).
     */
    public void binarySet(String directory_p) {
        try{
            objDb=new ObjectDatabase(directory_p);
        }catch(Exception e){
            e.printStackTrace();
        } 
    }

    /** Sets the current database to the one stored in the specified directory.
      * set and binarySet have exactly the same behaviour.
     * @param directory_p directory where the database is created (in most cases the same directory of the MI_FIle index).
     */
    public void set(String directory_p) {
        try{
            objDb=new ObjectDatabase(directory_p);
        }catch(Exception e){
            e.printStackTrace();
        } 
    }
    
    public DatasetObject getObject(int i){
        DatasetObject obj=null;
        try{
//            System.out.println("reading object "+i);
            obj= objDb.getObject(i);
        }catch(Exception e){
            e.printStackTrace();
        }
        return obj;
    }

    /**
     * Inserts an object in the Database of the index. Note that this does not insert the object in the index, so it shoul be used with care.
     * @param obj
     * @return the assigned internal object id
     * @throws Exception
     */
    public int insert(DatasetObject obj) throws Exception {
        return objDb.insert(obj);
    }
    
    public ObjectDatabase getDatabase(){
        return objDb;
    }


    public int size(){
        int res=-1;
        try{
            res= objDb.size();
        }catch(Exception e){
            e.printStackTrace();
        } 
        return res;
    }
    /**
     * This method is not used in this implementation. It does not do anything!
     *
     * @param a useless
     */
    public void save(String a){
        // does nothing!!
    }
    
    public double distance(DatasetObject o1, DatasetObject o2){
        return o1.distance(o2);
    }

    /**
     * Compute and prints the density of distances in the database
     */
    public void computeDistanceDensities(double max_dist) {
        int bin_num=100;
        int testedSet=10000;
        double bin_size=max_dist/(double)bin_num;
        int[] dd=null;
        System.out.println("Computing distance densities...");
        DatasetObject[] objects=new DatasetObject[testedSet];
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
