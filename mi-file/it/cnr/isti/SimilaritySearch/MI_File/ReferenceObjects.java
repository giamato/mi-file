/*
 * ReferenceObjects.java
 *
 * Created on 22 aprile 2008, 9.13
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package it.cnr.isti.SimilaritySearch.MI_File;

import it.cnr.isti.Dataset.*;
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

    /**
     * @return the smartStrategy
     */
    public int getSmartStrategy() {
        return smartStrategy;
    }

    /**
     * @param smartStrategy the smartStrategy to set
     */
    public void setSmartStrategy(int smartStrategy) {
        this.smartStrategy = smartStrategy;
    }

    /**
     * @return the smartThreshold
     */
    public double getSmartThreshold() {
        return smartThreshold;
    }

    /**
     * @param smartThreshold the smartThreshold to set
     */
    public void setSmartThreshold(double smartThreshold) {
        this.smartThreshold = smartThreshold;
    }

    /**
     * @param useAesa the useAesa to set
     */
    public void setUseAesa(boolean useAesa) {
        this.useAesa = useAesa;
    }

    private class ThreadedSearcher extends Thread{
        TreeMap<Double,Integer> res;
        int from;
        int to;
        SimilarityDatasetObject object;
        int k;
        ReferenceObjects caller;
        AESA aesa;


        
        public synchronized void setStatus(int k_p,int from_p, int to_p,ReferenceObjects caller_p){
            if(useAesa)
                setStatusWithAesa(k_p,from_p,to_p,caller_p);
            else{
                from=from_p;
                to=to_p;
                k=k_p;
                caller=caller_p;
            }
        }

        private synchronized void setStatusWithAesa(int k_p,int from_p, int to_p,ReferenceObjects caller_p){
            from=from_p;
            to=to_p;
            k=k_p;
            caller=caller_p;
            if(aesa==null){
                System.err.println("Loading AESA...");
                aesa=new AESA(to-from);
                for(int i=from;i<to;i++)
                    aesa.add(ros.get(i), i);
            }
        }

        public synchronized void setQuery(SimilarityDatasetObject object_p){
            object=object_p;
        }

        private int released=0;
        synchronized public void p(){
            try{
                released--;
                while(released<0){
                    //System.out.println("sleep: "+ this.getName()+ released);
                    this.wait();
                    //System.out.println("awake: "+ this.getName()+ released);
                }
            }catch (Exception e){
                e.printStackTrace();
            }
        }

        synchronized public void v(){
            try{
                released++;
                this.notify();
            }catch (Exception e){
                e.printStackTrace();
            }
        }


        public void run(){
            while(true){
                p();
      //          System.out.println("Started");
                kNearestReferenceObjectsThreaded();
                caller.v();
      //          System.out.println("Finished");
            }
        }

        public synchronized TreeMap<Double,Integer> getResult(){
            return res;
        };

        public synchronized void cleanResult(){
            res=null;
        }

        private synchronized void kNearestReferenceObjectsThreaded() {
            if(useAesa)
                kNearestReferenceObjectsThreadedWithAesa();
            else{
                res=new TreeMap<Double,Integer>();
                for(int i=from; i<to;i++)
                {
                    SimilarityDatasetObject robj=ros.get(i);
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

        private synchronized void kNearestReferenceObjectsThreadedWithAesa() {
            res=aesa.kNN(object, k);
            //System.out.println("Computed Distances: "+aesa.getComputedDistances());
        }
    }

    private boolean useAesa=false; //it consumes a lot of resources when having several sessions
    
//    private SimilarityDatasetObject ros[];
    private ArrayList<SimilarityDatasetObject> ros;
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
    
    public double distance(SimilarityDatasetObject o1, SimilarityDatasetObject o2){
        return o1.distance(o2);
    }
    
    public SimilarityDatasetObject getObject(int id){
 //       return ros[id];
        return ros.get(id);
    }
    
    /**
     * Initialize the dataset so that it can contain size objects.
     * @param size size of the dataset.
     */
    public void initializeEmpty(int size){
        ros=new ArrayList<SimilarityDatasetObject>(size);
    };
    
    /**
     * Add an object to the dataset.
     * @param o the added object.
     */
    public int insert(DatasetObject o){
        ros.add((SimilarityDatasetObject)o);
        return -1;
    }

    public void close(){
        //does nothing
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
    static int instanceCounter=0;
    private transient Object monitor=new Object();
    public TreeMap<Double,Integer> kNearestReferenceObjects(SimilarityDatasetObject object, int k) {
        synchronized(monitor){   //to guarantee that Mi_File is thread safe, just one thread at time must enter this method
            try{
                while(instanceCounter>0)
                        this.wait();
                 //System.out.println("instances: "+ (++instanceCounter)+this);
                if(object!=null){
                    TreeMap<Double,Integer> res=kNearestReferenceObjectsThreaded(object,k);
                    //System.out.println("out: "+ (instanceCounter--));
                    monitor.notify();
                    return res;
                 }
                else{
                     //System.out.println("out: "+ (instanceCounter--));
                     monitor.notify();
                    return new TreeMap<Double,Integer>();
             //   return kNearestReferenceObjectsSequential(object,k);
                }
             }catch(Exception e){
                     e.printStackTrace();
            }
            return null;
        }
    }

    public final static int DISTANCE_RATIO=1;
    public final static int INTRINSIC_DIMENSIONALITY=2;
    public final static int DISTANCE_CLUSTERING=3;
    private int smartStrategy=DISTANCE_RATIO;
    private double smartThreshold=0.85;
    private int max_k=1000;

    /**
     * Retrieves the k nearest reference objects closest to an object. These reference objects will be the object
     * representation in the transformed space. The number of closest reference objects will be decided automatically
     * IMPORTANT: This is just experimental. Obtained quality is not high!
     * @param obj the object to be represented by ordering of reference objects.
     * @param len the correct choice for the number of reference objects will be decided using the  len closest reference objects
     * @return the ordered list of reference objects that represent object
     */
    public TreeMap<Double,Integer> kNearestReferenceObjectsSmart(SimilarityDatasetObject obj,int len){
        TreeMap<Double,Integer> knp=kNearestReferenceObjects(obj,len);  //at most 1000 closest reference objects
        int k=0;
        if(getSmartStrategy()==DISTANCE_RATIO)
            k=distanceRatioStrategy(knp,len, getSmartThreshold());
        if(getSmartStrategy()==INTRINSIC_DIMENSIONALITY)
            k=6*computeLocalIntrinsicDimensionality(knp,len);  // multiply by 6 to have the same average number of ki <==== CHECK IT!!!!
        if(getSmartStrategy()==DISTANCE_CLUSTERING)
            return clusterDistances(knp);
        System.err.println("Number of closest reference objects: "+k);
        if(k<knp.size()){
            TreeMap temp_q_knp=knp;
            knp=new TreeMap<Double,Integer>();
            Iterator<java.util.Map.Entry<Double,Integer>> iter = temp_q_knp.entrySet().iterator();
            for(int n=0;n<k;n++)
            {
                java.util.Map.Entry<Double,Integer> e=iter.next();
                knp.put(e.getKey(),e.getValue());
            }
        }else
            knp=kNearestReferenceObjects(obj,k);
        return knp;
    }

    /**
     * Clusters pairs in the full list, containing the ordered set of reference objects,
     * according to the associated distance (reference objects having almost the same distance)
     * are represented by a single reference object), and 
     * return a list where just the cluster representative are contained. The cluster
     * representative are the reference objects with higher id (to uniquely
     * represent clusters)
     * @param fullList the set of reference objects ordered according to distance from
     * object to be represented
     * @return list of reference objects representing clusters of distances
     */
    private TreeMap<Double,Integer> clusterDistances(TreeMap<Double,Integer> fullList){
        double clusterDist=0;
        int currObject=-1;
        TreeMap<Double,Integer> res=new TreeMap<Double,Integer>();
        int position=0;
        for(java.util.Map.Entry<Double,Integer> pair:fullList.entrySet()){
            ++position;
            double dist=pair.getKey();
            int obj=pair.getValue();
            double distRatio=clusterDist/dist;
            //double distRatio=(dist-clusterDist)/dist;
            //if(distRatio>smartThreshold){ //we found a new cluster
            if(distRatio<smartThreshold){ //we found a new cluster
                if(currObject!=-1) //it is the first object. Before inserting it we should check next ones
                    res.put(clusterDist, currObject); //put representative of previous cluster
                clusterDist=dist; //first distance of the cluster
                currObject=obj; //current representative of the new cluster
                System.err.println(position);
            }else{                
                currObject=Math.max(currObject, obj); //current representative of current cluster is the highest id
            }
         //   clusterDist=dist; //distance of current object
        }
        return res;
    }


    /**
     * Supposing d is the dimensionality of the space, the number n of objects included in
     * a ball region of radius r is equal to c*r^d for some constant c. Here we thake a
     * list knp of pairs (dist,r_obj) of referenec objects ordered according to their distance to
     * the center of a ball region and we perform a power regression to decide what is the best value
     * of dimension d that make n=c*r^d fits with knp.
     *
     * @param knp the list of pairs (dist,objer) to use for the power regression
     * @param len, the intrinsic dimensionality will be computed on the first len objects
     * @return the intrinsic dimensionality
     */
    private int computeLocalIntrinsicDimensionality(TreeMap<Double,Integer> knp,int len){
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
            if(count++==len)
                break;
        }
        double var_x=Exx/n-(Ex/n)*(Ex/n);
        double covar_xy=Exy/n-(Ex/n)*(Ey/n);
        double id=covar_xy/var_x;                       //linear regression is solved using the covariance technique
        return (int) id;
    }


    /**
     * Chooses the optimal number of reference objects checking when ratio of the distance between the first closest reference object
     * and the current one is smaller than the threshold.
     *
     * @param knp the list of pairs (dist,objer) to use for computing the distance ratios
     * @param len, length of the list
     * @param threshold to be used to decide that we reached the optimal value
     * @return the optimal value
     */
    private int distanceRatioStrategy(TreeMap<Double,Integer> knp,int len,double threshold){
        int k=0;
        double dist_1=0;
        double dist_2=0;
        for(java.util.Map.Entry<Double,Integer> pair:knp.entrySet()){
            if(dist_1==0)
                dist_1=dist_2;
            dist_2=pair.getKey();
            double ratio=dist_1/dist_2;
            //System.out.print(" "+ratio);
            if(dist_1!=0&&(k++==len|| ratio<=threshold))
                break;
        }
        return (k<=max_k)?k:max_k;
    }


     private TreeMap<Double,Integer> kNearestReferenceObjectsSequential(SimilarityDatasetObject object, int k) {
        TreeMap res=null;     
         
        res=new TreeMap<Double,Integer>();
        for(int i=0; i<numOfReferenceObjects;i++)
        {
            SimilarityDatasetObject robj=ros.get(i);
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

    
    private int released=0;
    synchronized public void p(){
        try{
            released--;
            while(released<0){
                this.wait();
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    synchronized public void v(){
        try{
            released++;
            this.notify();
        }catch (Exception e){
            e.printStackTrace();
        }
    }


    private ThreadedSearcher[]threads=null;

    private TreeMap<Double,Integer> kNearestReferenceObjectsThreaded(SimilarityDatasetObject object, int k){
        int numOfThreads =Runtime.getRuntime().availableProcessors()*2;
        if (threads==null)
            threads = new ThreadedSearcher[numOfThreads];
        int numOfElementsPerProcessor=numOfReferenceObjects/numOfThreads;
        if((numOfReferenceObjects%numOfThreads)!=0)
            numOfElementsPerProcessor++;
        for(int proc=0;proc<numOfThreads;proc++){
            int start=proc*numOfElementsPerProcessor;
            int end=Math.min((1+proc)*numOfElementsPerProcessor,numOfReferenceObjects);
            if(threads[proc]==null){
                threads[proc]=new ThreadedSearcher();
                threads[proc].setDaemon(true);
                threads[proc].start();
            }
            threads[proc].setStatus(k,start, end, this); // this must be done here, not when threads are initialized. Elsewhere if ks is changed at runtime, this is not seen
            threads[proc].setQuery(object);
            threads[proc].v();
        }

        //ThreadedSearcher ts=new ThreadedSearcher(object, k,1, 1000);
        for(int proc=0;proc<numOfThreads;proc++){
            try{
                //threads[proc].join();
                p();
            }catch(Exception e){
             e.printStackTrace();
            };
        }

        TreeMap<Double,Integer> res=threads[0].getResult();
        threads[0].cleanResult();
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
            threads[proc].cleanResult();
        }
        return res;
    }

}
