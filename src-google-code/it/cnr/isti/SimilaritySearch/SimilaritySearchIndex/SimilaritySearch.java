/*
 * SimilaritySearch.java
 *
 * Created on 30 gennaio 2007, 18.32
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package it.cnr.isti.SimilaritySearch.SimilaritySearchIndex;

import it.cnr.isti.SimilaritySearch.Dataset.Dataset;
import it.cnr.isti.SimilaritySearch.Dataset.DatasetObject;
import java.util.*;


/**
 * Abstract class for any similarity search strategy.
 * @author Giuseppe Amato
 * ISTI-CNR, Pisa, Italy
 * giuseppe.amato@isti.cnr.it
 */
public abstract class SimilaritySearch {
    
    protected Dataset dataset=null;

    /**
     * Computes the distance between two objects.
     * @param o1 first object
     * @param o2 second object
     * @return distance between o1 and o2
     */
    public abstract double distance(DatasetObject o1, DatasetObject o2);
        
        
    /** Creates a new instance of SimilaritySearch */
    public SimilaritySearch(){
        
    }

    /** Creates a new instance of SimilaritySearch on a dataset/database
     * @param ds the dataset.
     */
    public SimilaritySearch(Dataset ds) {
        dataset=ds;
    }

    /**
     * Returns the associated dataset/database.
     * @return the dataset
     */
    public Dataset getDataset(){
        return dataset;
    }

    /**
     * Basic dafault implementaion of the kNN method.
     * Retrieves the internal identifiers, used in the database,
     * of the k closes objects to the query, along
     * with their spearman footrole distance to the query object. The real objects from the database should be retrieved
     * by using this.getDataset().getObject(identifier).
     * @param objectQuery the query object
     * @param k the number of retrieved objects
     * @return a TreeMap containing k pairs (distance, identifier) ordered according to the distance
     */
    public TreeMap<Double,Object> kNN(DatasetObject objectQuery, int k) {
        TreeMap res=null;             
        res=new TreeMap();
        for(int i=0; i<dataset.size()&&getDataset().getObject(i)!=null;i++)
        {
            double dist=distance(objectQuery,getDataset().getObject(i));
            Integer o=Integer.valueOf(i);
            if(res.size()<k){
                while(o!=null)
                {
                    o=(Integer)res.put(dist,o);
                    dist+=0.0001;
                }
            }
            else if(dist<(Double)res.lastKey())
            {
                while(o!=null)
                {
                    o=(Integer)res.put(dist,o);
                    dist+=0.0001;
                }
                res.remove(res.lastKey());
            }
        }
        return res;
    }     
 
     
    /**
     * Given a list of internal object identifiers and distances, returns the corresponding list of objects and distances
     * by retrieving the objects from the database.
     * @param identifiers list of pairs (identifier,distance)
     * @param k number of objects to be retrieved
     * @return list of pairs (distance, objects)
     */
    public TreeMap<Double,DatasetObject> retrieve(TreeMap<Integer,Double> identifiers, int k)
    {

        java.util.Iterator s=identifiers.entrySet().iterator();
        TreeMap res=null;
        if(dataset!=null)
        {
            res=new TreeMap();
            while(s.hasNext()&& k>0)
            {
                k--;
                java.util.Map.Entry<Integer,Double> e=(java.util.Map.Entry)s.next();
                Double dist = e.getValue();
                Integer o=e.getKey();
                DatasetObject object = dataset.getObject(o);
                object.setInternalId(o);   //Just to be sure. It should have already been done in objectDatabase.Insert()
                while(res.get(dist)!=null)
                {
                    dist+=0.00000001;
                }
                res.put(dist,object);
            }
        }
        else System.out.println("Retrieval not performed. No dataset associated!");
        return res;       
    }

    
    /**
     * Given a list of internal object identifiers and distances, returns the corresponding list of objects and distances
     * by retrieving the objects from the database and sorting them according to the real distance.
     * @param identifiers list of pairs (distance,identifier)
     * @param objectQuery object to be used to compute the real distance
     * @param k number of objects to be retrieved
     * @return list of pairs (distance, objects)
     */
    public TreeMap<Double,DatasetObject> sortAndRetrieve(TreeMap<Double,Integer> identifiers, DatasetObject objectQuery, int k)
    {
//        System.out.println("Filtering result set");
        java.util.Iterator<Double> s=identifiers.keySet().iterator();
//        if(s.hasNext())
//            s.next(); //skip first to eliminate query object
        TreeMap res=null;
        if(dataset!=null)
        {
            res=new TreeMap();
            while(s.hasNext()&& k>0)
            {
                k--;
                Double key=s.next();
                Integer o=identifiers.get(key);
                DatasetObject object = dataset.getObject(o);
                object.setInternalId(o);    //Just to be sure. It should have already been done in objectDatabase.Insert()
                Double dist = dataset.distance(objectQuery,object);
                while(res.get(dist)!=null)
                {
                    dist+=0.00000001;
                }
                res.put(dist,object);
            }
        }
        else System.out.println("Sort and retrieval not performed. No dataset associated!");
        return res;       
    }
    
    /**
     * Given a list of internal object identifiers and distances, returns the same list of identifiers
     * sorted  according to the real distance.
     * @param identifiers list of pairs (distance,identifier)
     * @param objectQuery object to be used to compute the real distance
     * @param k number of objects to be retrieved
     * @return list of pairs (distance, identifier)

     */
    public TreeMap<Double,Integer> sort(TreeMap<Double,Integer> identifiers, DatasetObject objectQuery, int k)
    {
//        System.out.println("Filtering result set");
        java.util.Iterator<Double> s=identifiers.keySet().iterator();
//        if(s.hasNext())
//            s.next(); //skip first to eliminate query object
        TreeMap res=null;
        if(dataset!=null)
        {
            res=new TreeMap();
            while(s.hasNext()&& k>0)
            {
                k--;
                Double key=s.next();
                Integer o=identifiers.get(key);
                Double dist = dataset.distance(objectQuery,dataset.getObject(o));
                while(res.get(dist)!=null)
                {
                    dist+=0.00000001;
                }
                res.put(dist,o);
            }
        }
        else System.out.println("Sort not performed. No dataset associated!");
        return res;       
    }
    
              
     
   
}
