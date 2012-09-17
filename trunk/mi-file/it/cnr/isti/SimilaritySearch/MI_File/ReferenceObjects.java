/*
 * ReferenceObjects.java
 *
 * Created on 22 aprile 2008, 9.13
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package it.cnr.isti.SimilaritySearch.MI_File;

import it.cnr.isti.SimilaritySearch.Dataset.DatasetObject;
import it.cnr.isti.SimilaritySearch.Dataset.Dataset;
import java.io.*;
import java.util.*;

/**
 * This class implements the Dataset interface and represent the set of reference objects
 * to be used in an MI_File. The reference objects dataset can be created by adding the objects
 * one by one, or by using a dataset of reference objects previously created and saved.
 * <p>
 * <b> IMPORTANT: </b> Once an MI_File has been created with a certain set of reference objects,
 * the set of reference objects should not be changed. An MI_File MUST be open always with the same set
 * of reference objects, elsewhere impredictable results will be obtained.
 *
 * @author Giuseppe Amato
 * ISTI-CNR, Pisa, Italy
 * giuseppe.amato@isti.cnr.it
 */
public class ReferenceObjects implements Dataset{

    private class ThreadedSearcher extends Thread{
        TreeMap<Double,Integer> res;
        int from;
        int to;
        DatasetObject object;
        int k;

        public ThreadedSearcher( DatasetObject object_p, int k_p,int from_p, int to_p){
            super();
            from=from_p;
            to=to_p;
            object=object_p;
            k=k_p;
        }

        public void run(){
  //          System.out.println("Started");
            kNearestReferenceObjectsThreaded();
  //          System.out.println("Finished");
        }

        public TreeMap<Double,Integer> getResult(){
            return res;
        };

        private void kNearestReferenceObjectsThreaded() {

            res=new TreeMap<Double,Integer>();
            for(int i=from; i<to;i++)
            {
                DatasetObject robj=ros.get(i);
                double dist= object.distance(robj);
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
        }



    }
    
//    private DatasetObject ros[];
    private ArrayList<DatasetObject> ros;
    private int numOfReferenceObjects;

    private static final long serialVersionUID =8257789209529044949L;
    
    /** Creates a new instance of ReferenceObjects. The data set is uninitialized. */
    public ReferenceObjects() {
    }
    
    /**
     * Sets the number of objects, in the dataset, to be used as reference objects. It must be smaller
     * than the total number of objects in the dataset.
     * @param val
     */
    public void setNumOfReferenceObjects(int val){
        numOfReferenceObjects=val;
    }
    
    public double distance(DatasetObject o1, DatasetObject o2){
        return o1.distance(o2);
    }
    
    public DatasetObject getObject(int id){
 //       return ros[id];
        return ros.get(id);
    }
    
    /**
     * Initialize the dataset so that it can contain size objects.
     * @param size size of the dataset.
     */
    public void initializeEmpty(int size){
        ros=new ArrayList<DatasetObject>(size);
    };
    
    /**
     * Add an object to the dataset.
     * @param o the added object.
     */
    public void add(DatasetObject o){
        ros.add(o);
    }

    /**
     * Sets this dataset of reference objects to a previously saved dataset of reference objects.
     * @param file the file containing the saved reference objects.
     *
     * set() and binarySet() do the same things.
     */
    public void binarySet(String file){
        try{
            ObjectInputStream ros_file=new ObjectInputStream(new FileInputStream(file));
            ReferenceObjects ro=(ReferenceObjects)ros_file.readObject();
            ros=ro.ros;
            ros_file.close();
        }catch(Exception e){
            System.out.println("Cannot open file "+file+" to load reference objects;");
            e.printStackTrace();
        }
    }
    
    /**
     * Sets this dataset of reference objects to a previously saved dataset of reference objects.
     * @param file the file containing the saved reference objects.
     * 
     * set() and binarySet() do the same things.
     */
    public void set(String file){
        binarySet(file);
    }


    public void save(String file){
        try{
            ObjectOutputStream ros_file=new ObjectOutputStream(new FileOutputStream(file));
            ros_file.writeObject(this);
            ros_file.close();
        }catch(Exception e){
            System.out.println("Cannot open file "+file+" to save reference objects;");
            e.printStackTrace();
        }    
    }
    
    public int size(){
        return ros.size();
    }
    
    
    /**
     * Retrieves the k nearest reference objects closest to an object. These reference objects will be the object
     * representation in the transformed space.
     * @param object the object to be represented by ordering of reference objects.
     * @param k the number of reference objects used to represent other objects.
     * @return the ordered list of reference objects that represent object
     */
    public TreeMap<Double,Integer> kNearestReferenceObjects(DatasetObject object, int k) {
        return kNearestReferenceObjectsThreaded(object,k);
  //      return kNearestReferenceObjectsSequential(object,k);
    }

    /**
     * Retrieves the k nearest reference objects closest to an object. These reference objects will be the object
     * representation in the transformed space. The number of closest reference objects will be equal to the intrinsic dimensionality
     * IMPORTANT: This is just experimental. Obtained quality is not high!
     * @param obj the object to be represented by ordering of reference objects.
     * @param max_ks the intrinsic dimensionality will be computed on the mqax_ks closest objects
     * @return the ordered list of reference objects that represent object
     */
    public TreeMap<Double,Integer> kNearestReferenceObjectsIntrinsic(DatasetObject obj,int max_ks){
        TreeMap<Double,Integer> knp=kNearestReferenceObjects(obj,1000);  //at most 1000 closest reference objects
        int id=computeLocalIntrinsicDimensionality(knp,max_ks);
        System.err.println("Intrinsic dimensionality: "+id);
        if(id<knp.size()){
            TreeMap temp_q_knp=knp;
            knp=new TreeMap<Double,Integer>();
            Iterator<java.util.Map.Entry<Double,Integer>> iter = temp_q_knp.entrySet().iterator();
            for(int n=0;n<id;n++)
            {
                java.util.Map.Entry<Double,Integer> e=iter.next();
                knp.put(e.getKey(),e.getValue());
            }
        }else
            knp=kNearestReferenceObjects(obj,id);
        return knp;
    }


    /**
     * Supposing d is the dimensionality of the space, the number n of objects included in
     * a ball region of radius r is equal to c*r^d for some constant c. Here we thake a
     * list knp of pairs (dist,r_obj) of referenec objects ordered according to their distance to
     * the center of a ball region and we perform a power regression to decide what is the best value
     * of dimension d that make n=c*r^d fits with knp.
     *
     * @param knp the list of pairs (dist,objer) to use for the power regression
     * @param ks, the intrinsic dimensionality will be computed on the first ks objects
     * @return the intrinsic dimensionality
     */
    private int computeLocalIntrinsicDimensionality(TreeMap<Double,Integer> knp,int ks){
        int n=0;
        double Exy=0;
        double Ex=0;
        double Exx=0;
        double Ey=0;
        int count=0;
        for(java.util.Map.Entry<Double,Integer> pair:knp.entrySet()){
            double x=Math.log10(pair.getKey());         //we use the logarithm trick to transform power regression into linear regression
            double y=Math.log10(++n);
            Exy+=x*y;
            Ex+=x;
            Ey+=y;
            Exx+=x*x;
            if(count++==ks)
                break;
        }
        double var_x=Exx/n-(Ex/n)*(Ex/n);
        double covar_xy=Exy/n-(Ex/n)*(Ey/n);
        double id=covar_xy/var_x;                       //linear regression is solved using the covariance technique
        return (int) id;
    }


     private TreeMap<Double,Integer> kNearestReferenceObjectsSequential(DatasetObject object, int k) {
        TreeMap res=null;     
         
        res=new TreeMap<Double,Integer>();
        for(int i=0; i<numOfReferenceObjects;i++)
        {
            DatasetObject robj=ros.get(i);
            double dist= object.distance(robj);
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

    private TreeMap<Double,Integer> kNearestReferenceObjectsThreaded(DatasetObject object, int k){
        int numOfThreads =Runtime.getRuntime().availableProcessors()*2;
        ThreadedSearcher[]threads =new ThreadedSearcher[numOfThreads];
        int numOfElementsPerProcessor=numOfReferenceObjects/numOfThreads;
        if((numOfReferenceObjects%numOfThreads)!=0)
            numOfElementsPerProcessor++;
        for(int proc=0;proc<numOfThreads;proc++){
            int start=proc*numOfElementsPerProcessor;
            int end=Math.min((1+proc)*numOfElementsPerProcessor,numOfReferenceObjects);
            threads[proc]=new ThreadedSearcher(object, k,start, end);
            threads[proc].start();
        }

        ThreadedSearcher ts=new ThreadedSearcher(object, k,1, 1000);
        for(int proc=0;proc<numOfThreads;proc++){
            try{
                threads[proc].join();
            }catch(Exception e){
             e.printStackTrace();
            };
        }

        TreeMap<Double,Integer> res=threads[0].getResult();
        for(int proc=1;proc<numOfThreads;proc++){
            for(java.util.Map.Entry<Double,Integer> e:threads[proc].getResult().entrySet()){
                double dist= e.getKey();
                Integer o=e.getValue();
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
        }


        return res;
    }

}
