/*
 * kNearestPivots.java
 *
 * Created on 12 aprile 2007, 17.11
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package it.cnr.isti.SimilaritySearch.MI_File;


import it.cnr.isti.SimilaritySearch.Dataset.*;
import it.cnr.isti.SimilaritySearch.SimilaritySearchIndex.SimilaritySearch;
import java.util.*;
import java.io.*;

/**
 * This is the main class to use MI-Files. MI-Files allow you to perform approximate similarity search
 * on huge datasets. The technique is based on the use of a space transformation where data objects
 * are represented by ordered sequences of reference objects. The sequence of reference objects that represent
 * a data object is ordered according to the distance of the reference objects from the data object being represented.
 * Distance between two data objects is measured by computing the spearmann footrule distance between
 * the two sequence of reference objects that represent them. The closer the two data objects
 * the most similar the two sequence of reference objects. The index is based on the use of inverted files.
 * More details on the technique can be found in the paper "Approximate Similarity Search in Metric Spaces Using
 * Inverted Files", by Giuseppe Amato and Pasquale Savino, presented at Infoscale 2008.
 * 
 * <p>
 * In order to use the MI_File library you have to perform some preliminary steps
 *<p>
 * 1) You have to create a class that extends the abstract class Dataset.DatasetObject that
 * encapsulate your data objects.
 *<p>
 * 2) Create a set of reference objects. To do that you have to use the class ReferenceObjects. The
 * size of the set of reference objects can be estimated as 2*sqrt(dataset size).
 * <p>
 * 3) Decide the number ki of reference objects to use to represent data objects (parameter p_ki of the MI_File
 * constructors). Data objects are represented by the ki closest reference objects.
 * This number is typically much smaller than the total number of reference objects.
 * A good value is typically the inthrinsical dimensionality of the dataset. In case this value
 * is not available you can use the dimensionality (in case of vector data).
 * <p>
 * 4) Create an instance of MI_File, using the set of reference objects above.
 * <p>
 * <b> IMPORTANT: </b> Once an MI_File has been created with a certain set of reference objects,
 * the set of reference objects should not be changed. An MI_File MUST be open always with the same set
 * of reference objects, elsewhere unpredictable results will be obtained.
 * <p>
 * 5) Start inserting data objects to be indexed.
 * <p>
 * 6) From now on, you can open the MI_File, using always the same set of Reference objects, insert new data objects or use the search methods (kNN, kNNRetrieve, and kNNRetrieveAndSort).
 * <p>
 * Note that there are other parameters you can use to play with.
 * <p>
 * ks determines the number of reference objects
 * to be used to represent the query; it should be smaller than ki.
 * <p>
 * MaxPosDiff can be used to access just
 * a portion of the posting lists; if MaxPosDiff is 10 just the portion of the posting lists corresponding to objects
 * having reference objects in position +/- 10, with respect to those of the query, is accessed.
 * <p>
 * The package Examples contains source code examples of the above steps.
 *
 * @author Giuseppe Amato
 * ISTI-CNR, Pisa, Italy
 * giuseppe.amato@isti.cnr.it
 */
public class MI_File extends SimilaritySearch{
    
    private InvertedFile invFile=null;
    private ObjectDatabase objDb=null;
    private int ks;    //number of nearest pivots used when searching
    private int ki;    //number of nearest pivots used when indexing
    private int n_ro; //total number of reference objects
    private int max_pos_diff; //consider just objects with at most max_pos_diff with respect to the query
    private String indexDirectory;
    private ReferenceObjects ros;
    
    private long totalSearchTime;
    private long indexAccessTime;
    private long tempResultmanagementTime;
    private long databaseAccessTime;
    private int numberOfObjectsFound;
    private int executedQueries=0;
    
    

   
    
    /** Open an MI-File. If no existing index is contained in the directory specified by
     * p_indexDirectory, or if the directory does not exists, an empty index is created in the directory.
     * The MI-File can be closed by using the close() method.
     * @param p_n_ro Specifies the total number of reference objects used in the MI-File.
     * If a new MI-File is being created, it indicates the total number of reference objects that will be used
     * in the index.
     * If an existing MI-File is being opened, it must be the same than the number used when the index was created.
     * @param p_ki Specifies the number of reference objects used for indexing. It must be smaller than p_n_ro
     * @param p_ks Specifies the number of reference objects used for searching. It can be alse changed by using
     * setKs() when the index is open.
     * @param p_indexDirectory Specifies the directory where the index is located
     * @param ref_obj_file specifies the pathname of the file containg the reference objects (See
     * javadoc for ReferenceObjects)
     */
    public MI_File(int p_n_ro, int p_ki, int p_ks, String p_indexDirectory, String ref_obj_file) {
//        super(dataset);
        try{
            n_ro=p_n_ro;
            ki=p_ki;
            ks=p_ks;
            max_pos_diff=ki;
            indexDirectory=p_indexDirectory;
            ros=new ReferenceObjects();
            ros.set(ref_obj_file);
            ros.setNumOfReferenceObjects(p_n_ro);
            invFile=new InvertedFile(n_ro,indexDirectory);
            dataset=new DatasetInDatabaseCompressed(indexDirectory);
            objDb=((DatasetInDatabaseCompressed)dataset).getDatabase();
        }catch(Exception e){
            e.printStackTrace();
        }
    }

    /** Open an MI-File by specifying where the database of objects is located. By default the database
     * is contained in the same directory of the index itself. If you need to store the database in a
     * different location than the index, then you have to use this constructor.
     * 
     * If no existing index is contained in the directory specified by
     * p_indexDirectory, or if the directory does not exists, an empty index is created in the directory.
     * The MI-File can be closed by using the close() method.
     * @param p_n_ro Specifies the total number of reference objects used in the MI-File.
     * If a new MI-File is being created, it indicates the total number of reference objects that will be used
     * in the index.
     * If an existing MI-File is being opened, it must be the same than the number used when the index was created.
     * @param p_ki Specifies the number of reference objects used for indexing. It must be smaller than p_n_ro
     * @param p_ks Specifies the number of reference objects used for searching. It can be alse changed by using
     * setKs() when the index is open.
     * @param p_indexDirectory Specifies the directory where the index is located
     * @param p_dbDirectory Specify the location where the database containing the inserted objects is stored. 
     * @param ref_obj_file specifies the pathname of the file containg the reference objects (See
     * javadoc for ReferenceObjects)
     */
    public MI_File(int p_n_ro, int p_ki, int p_ks, String p_indexDirectory, String p_dbDirectory, String ref_obj_file) {
//        super(dataset);
        try{
            n_ro=p_n_ro;
            ki=p_ki;
            ks=p_ks;
            max_pos_diff=ki;
            indexDirectory=p_indexDirectory;
            ros=new ReferenceObjects();
            ros.set(ref_obj_file);
            ros.setNumOfReferenceObjects(p_n_ro);
            invFile=new InvertedFile(n_ro,indexDirectory);
            dataset=new DatasetInDatabaseCompressed(p_dbDirectory);
            objDb=((DatasetInDatabaseCompressed)dataset).getDatabase();
        }catch(Exception e){
            e.printStackTrace();
        }
    }
    
    /**
     * Returns the number of objects stored and indexed in the MI-File.
     * @return the number of objects stored and indexed in the MI-File.
     */
    public int size(){
        try{
            return objDb.size();
        }catch(Exception e){
            return 0;
        }
    }
    
    /**
     * Close the current MI-File. All files are closed and all in-memory data structures are freed.
     */
    public void close(){
        invFile.close();
        objDb.close();
    }
    
   
    @Override
    protected void finalize () throws Throwable{
        close();
    }
    
    
    
    /**
     *Returns the total number of reference objects used in the current MI-File.
     * @return the total number of reference objects used in the current MI-File
     */
    public int getRoNumber(){
        return n_ro;
    }
    
    /**
     * Returns the number of reference objects used for indexing.
     * @return the number of reference objects used for indexing in the current MI-File.
     */
    public int getKi(){
        return ki;
    }
    
    /**
     * Returns the current number of reference objects used for searching. It can be changed using setKs().
     * @return the current number of reference objects used for searching.
     */
    public int getKs()
    {
        return ks;
    }
    
    /**
     * Sets the number of reference objects used for searching in an open MI-File.
     * @param par The number of reference objects used for searching
     */
    public void setKs(int par)
    {
        ks=par;
    }
    
    /**
     * Gets the current maximum posistion difference between reference object of the query and retrieved objects.
     * @return the current maximum posistion difference between reference object of the query and retrieved objects.
     */
    public int getMaxPosDiff()
    {
        return max_pos_diff;
    }
    
    /**
     * Sets the current maximum posistion difference between reference object of the query and retrieved objects.
     * It can be used to access just a fraction of the posting lists, increasing the approximation and reducing the
     * response time.
     * When an MI-File is open or created, it is initially set to Ks.
     * @param diff the current maximum posistion difference between reference object of the query and retrieved objects.
     */
    public void setMaxPosDiff(int diff)
    {
        max_pos_diff=diff;
    }
    
    /**
     * Gets the reference objects used in this MI-FIle. See javadoc for ReferenceObjects.
     * @return the reference objects used in this MI-FIle.
     */
    public ReferenceObjects getROs(){
        return ros;
    }
  
    
    
    /**
     * Begins bulk loading of the index. Insertions are executed using bulk loading. First beginBulkLoad()
     * called. The you can call bulkInsert() repetitively to insert as meny objects as you want inthe index,
     * finally you call endBulLoad() to commit the insertions. Bulk Loading can be executed many times.
     * @throws Exception if something goes wrong an exception is thrown.
     */
    public void beginBulkLoad() throws Exception{
        File bulk_load_done_f=new File(indexDirectory+"/bulk_load_done.dat");
        File bulk_load_in_progress_f=new File(indexDirectory+"/bulk_load_in_progress.dat");
        File indexDirectory_f=new File(indexDirectory);
        if(!indexDirectory_f.exists())
            indexDirectory_f.mkdirs();
        if(!bulk_load_in_progress_f.exists()){
            bulk_load_done_f.delete();
            bulk_load_in_progress_f.createNewFile();
        }else{
            throw new Exception("Cannot sart bulk load...another bulk load already in progress");
        }
    }

    /**
     * Ends current bulk loading of the index. Insertions are executed using bulk loading. First beginBulkLoad() is
     * called. Then you can call bulkInsert() repetitively to insert as meny objects as you want inthe index,
     * finally you call endBulLoad() to commit the insertions. Bulk Loading can be executed many times.
     * @throws Exception if something goes wrong, an exception is thrown.
     */
    public void endBulkLoad() throws Exception{
        File bulk_load_in_progress_f=new File(indexDirectory+"/bulk_load_in_progress.dat");
        if(bulk_load_in_progress_f.exists()){
            invFile.flush();      
            invFile.sortPostingList();
            bulk_load_in_progress_f.delete();
            File bulk_load_done_f=new File(indexDirectory+"/bulk_load_done.dat");
            try{
                bulk_load_done_f.createNewFile();
            }catch(Exception e){
                throw new Exception("Cannot commit end of bulk load...unable to create bulk_load_done.dat file."); 
//                System.out.println("Cannot commit end of bluk load");
//                e.printStackTrace();
            }
        }else
            throw new Exception("no bulk load to be ended...beginBulkLoad not executed or failed"); 
    }

    /**
     * Insert an object in the index, during the bulk loading procedure.
     * Insertions are executed using bulk loading. First beginBulkLoad()
     * called. The you can call bulkInsert() repetitively to insert as meny objects as you want inthe index,
     * the you call endBulLoad() to commit the insertions. Bulk Loading can be executed many times.
     *
     * Inserted objects are interanally automaticaly associated with an unique identifier that
     * identifies the obejct in the
     * MI_File database. When a kNN search is executed the result is a list of such identifiers.
     * The real objects from the database should be retrieved
     * by using this.getDataset().getObject(identifier), or useing the kNNRetrieve() or kNNRetrieveAndSort()
     * methods. The MI_File makes the association between objects
     * and internal identifiers by calling the method setInternalId() of the object during the insertion.
     * Therefore any other use of the method setInternalId() for other purposes is dangerous.
     *
     *
     * @param obj the object to be inserted. obj should be of type DatasetObject
     * @return the internal identifier assigned to the inserted object.
     * @throws Exception if something goes wrong, an exception is thrown.
     */
    public int bulkInsert(DatasetObject obj) throws Exception{
        File bulk_load_in_progress_f=new File(indexDirectory+"/bulk_load_in_progress.dat");
        if(bulk_load_in_progress_f.exists()){
            int id=objDb.insert(obj);
            TreeMap<Double,Integer> knp;
            if(isUseIntrinsicDimensionality()){
                knp=ros.kNearestReferenceObjectsIntrinsic(obj,ks);
            }else
                knp=ros.kNearestReferenceObjects(obj,ki);
            Iterator iter=knp.entrySet().iterator();
            int pos=0;
            while(iter.hasNext())
            {
                Map.Entry e=(Map.Entry)iter.next();
                int piv=(Integer)e.getValue();
                invFile.insert(id,piv,pos);
        //                        System.out.print(" "+piv+" "+pos);
                pos++;
            }
            return id;
        }else
            throw new Exception("Cannot execute bulk insert...beginBulkLoad not executed or failed"); 
    }

    
    /**
     * Computes distance between two objects.
     * @param o_1 first object
     * @param o_2 second object
     * @return distance between o_1 and o_2
     */
    public double distance(DatasetObject o_1, DatasetObject o_2)
    {
        int dist=0;
        TreeMap q_knp=ros.kNearestReferenceObjects(o_1,ks);
        Iterator q_iter=q_knp.keySet().iterator();
        int q_p_pos=0;
        while(q_iter.hasNext())
        {
            q_p_pos++;
            Integer q_p=(Integer)q_knp.get(q_iter.next());
            int obj_p_pos=0;
            boolean found=false;
            TreeMap knp=ros.kNearestReferenceObjects(o_2,ks);
            Iterator obj_iter=knp.keySet().iterator();
            while(obj_iter.hasNext()&&!found)
            {
                obj_p_pos++;
                Integer obj_p=(Integer) knp.get(obj_iter.next());
                if(obj_p.equals(q_p))
                    found=true;
            }
            dist+=java.lang.Math.abs(q_p_pos-obj_p_pos);
        }
        return dist;
    }

    private boolean saveMemoryMode=false;

    /**
     * MI_File can be used in "Save Memory Mode", this method says if the "Save Memory Mode" is on.
     * @return true when "Save Memory Mode" is on.
     */
    public boolean isSaveMemoryMode() {
        return saveMemoryMode;
    }

    /**
     * MI_File can be used in "Save Memory Mode", this method sets  the "Save Memory Mode"  on or off.
     * When "Save Memory Mode" is off MI_File consumes less memory when processign queries. However it migh be slower.
     * By default "Save Memory Mode" is off.
     * @param saveMemoryMode true (on) false (off)
     */
    public void setSaveMemoryMode(boolean saveMemoryMode) {
        this.saveMemoryMode = saveMemoryMode;
    }
     

     
    /**
     * Retrieves the MI_File internal identifiers, used in the MI_File database,
     * of the k closes objects to the query, along
     * with their spearman footrole distance to the query object. The real objects from the database should be retrieved
     * by using this.getDataset().getObject(identifier).
     * @param objectQuery the query object
     * @param k the number of retrieved objects
     * @return a TreeMap containing k pairs (distance, identifier) ordered according to the distance
     */
    @Override
    public TreeMap<Double,Object> kNN(DatasetObject objectQuery, int k){
         //PURE APPROXIMATE RESULT
         databaseAccessTime=0;
         TreeMap res=incrementalkNNSearch(objectQuery,k);
         totalSearchTime=indexAccessTime;
         return res;
     }

    /**
     * Retrieves the k closes objects to the query, along
     * with their spearman footrole distance to the query object.
     * @param objectQuery the query object
     * @param k the number of retrieved objects
     * @return a TreeMap containing k pairs (distance, DatasetObject) ordered according to the distance
     */
    public TreeMap<Double,Object> kNNRetrieve(DatasetObject objectQuery, int k){
         TreeMap<Double,Integer> q_knp;
         if(isUseIntrinsicDimensionality()){
            q_knp=ros.kNearestReferenceObjectsIntrinsic(objectQuery,ks);
         }else
             q_knp=ros.kNearestReferenceObjects(objectQuery,ks);
         return kNNRetrieve(q_knp,k);
     }


    /**
     * Retrieves the k closes objects to the query, along
     * with their real distance distance to the query object. The result list is sorted according
     * to the real distance between objects and the query, rather than the spearman foortrule distance.
     * The k closest objects are obtained by retrieving k*amp objects
     * (sorted according to the spearmann footrule distance) and by taking the k closest to the query
     * according to the actual distance
     * @param objectQuery the query object
     * @param k the number of retrieved objects
     * @param amp the k best objects from the k*amp obejcts retrieved will be returned
     * @return a TreeMap containing k pairs (distance, DatasetObject) ordered according to the distance
     */
    public TreeMap<Double,Object> kNNRetrieveAndSort(DatasetObject objectQuery, int k, int amp){
         //SMART impplementation:
         k=k*amp;
         TreeMap res= incrementalkNNSearch(objectQuery,k);
         //reordering of the first k results according to the object id to perform a sequential scan in the db
         long time=System.currentTimeMillis();
         TreeMap res1= new TreeMap();
         Iterator res_iter=res.values().iterator();
         for(int i=0;i<k;i++)
             if(res_iter.hasNext()){
                Integer o= (Integer)res_iter.next();
                 res1.put((double)o,o);
             }
         TreeMap res_final=  sortAndRetrieve(res1,objectQuery,k);
         databaseAccessTime=System.currentTimeMillis()-time;
         totalSearchTime=indexAccessTime+databaseAccessTime;
//         System.err.println("Elapsed time: Index acces: "+ indexAccessTime +", Object retrieve and sort: "+(databaseAccessTime));
         return res_final;

     }
     
    
     
     

    /**
     * Return the number of entries read in the posting list, since the last resetNumberOfReads() was executed.
     * @return number of entries read
     */
    public static long getNumberOfReads(){
        return InvertedFileIterator.getNumOfReads();
    }

    /**
     * Reset the counter for the number of reads.
     */
    public static void resetNumberOfReads(){
        InvertedFileIterator.resetNumOfReads();
    }

    /**
     * Returns the elapsed time in last query.
     * @return Total elapsed time
     */
    public long getTotalSearchTime() {
        return totalSearchTime;
    }

    /**
     * Returns the time spent in accessing the index during the last query
     * @return index access time
     */
    public long getIndexAccessTime() {
        return indexAccessTime;
    }

    /**
     * Returns the time spent managing temporary results and queues
     * @return Temporary results management time
     */
    public long getTempResultmanagementTime() {
        return tempResultmanagementTime;
    }

    /**
     * Returns the time spent by accessing objects in the database. This makes sens when kNNRetrieve() or
     * kNNretrieveAndReorder() is used.
     * @return Databse access time
     */
    public long getDatabaseAccessTime() {
        return databaseAccessTime;
    }

    /**
     * Total number of objects found. This corresponds to the final number of objects in the temporary result,
     * which is typically much larger than k.
     * @return number of objects found.
     */
    public int getNumberOfObjectsFound() {
        return numberOfObjectsFound;
    }

    /**
     * Rturns the total number of queries executed so far.
     * @return number of query executed
     */
    public int getExecutedQueries(){
        return executedQueries;
    }

    private boolean useIntrinsicDimensionality=false;

    /**
     * Checks if the intrinsic dimensionality computatin is used to estimate the appropriate value of ki and ks
     * @return true if intrinsic dimensionality is used, false if it is not used (in this case ki and ks shoule be set explicitely)
     */
    public boolean isUseIntrinsicDimensionality() {
        return useIntrinsicDimensionality;
    }

    /**
     * Set the intrinsic dimensionality computatin to be used to estimate the appropriate value of ki and ks.
     * IMPORTANT: With current implementation the quality of retrieval is very poor when intrinsic dimensionality usage is set to true.
     * This was just experimental. It is better to leave it as false (default usage).
     *
     * @param useIntrinsicDimensionality true if intrinsic dimensionality should be used, false if it should not be used (in this case ki and ks shoule be set explicitely). Defoult is false
     */
    public void setUseIntrinsicDimensionality(boolean useIntrinsicDimensionality) {
        this.useIntrinsicDimensionality = useIntrinsicDimensionality;
    }


    private TreeMap incrementalkNNSearch(DatasetObject qobj, int k){
        TreeMap<Double,Integer> q_knp;
        if(isUseIntrinsicDimensionality()){
            q_knp=ros.kNearestReferenceObjectsIntrinsic(qobj,ks);
        }else
            q_knp=ros.kNearestReferenceObjects(qobj,ks);
        return incrementalkNNSearch(q_knp,k);
    }

   


    private TreeMap kNNRetrieve(TreeMap q_knp, int k){
         //SMART impplementation:
         TreeMap res= incrementalkNNSearch(q_knp,k);
         //reordering of the first k results according to the object id to perform a sequential scan in the db
         long time=System.currentTimeMillis();
         TreeMap res1= new TreeMap();
         Iterator res_iter=res.entrySet().iterator();
         for(int i=0;i<k;i++)
             if(res_iter.hasNext()){
                java.util.Map.Entry e= (java.util.Map.Entry)res_iter.next();
                res1.put(e.getValue(),e.getKey());
             }
         TreeMap res_final=  retrieve(res1,k);
         databaseAccessTime=System.currentTimeMillis()-time;
         totalSearchTime=indexAccessTime+databaseAccessTime;
//         System.err.println("Elapsed time: Index acces: "+ indexAccessTime +", Object retrieve and sort: "+(databaseAccessTime));
         return res_final;

     }

     private TreeMap incrementalkNNSearch(TreeMap q_knp, int k)
     {
        long time=System.currentTimeMillis();
        TempRes temp_res=new TempRes(ki+1,k);
        boolean stop=false;
        Iterator q_iter = q_knp.entrySet().iterator();
        //for(int pos=0;pos<ks & q_iter.hasNext();pos++)  //Here we check that we use less than ks referece objects. However, size of q_knp shold be always smaller than ks
        for(int pos=0;q_iter.hasNext();pos++)
        {
            Map.Entry e=(Map.Entry)q_iter.next();
            int piv=(Integer)e.getValue();
//            System.out.print("---Posting List:"+piv+"\n");
            int low_inv_file_pos=pos-max_pos_diff;
            int high_inv_file_pos=Math.min(pos+max_pos_diff,ki);  //With ki upper bound
            InvertedFileIterator inv_file_iter=invFile.getPostingList(piv,low_inv_file_pos,high_inv_file_pos);
            while(inv_file_iter.hasNext())
            {
                InvertedFileIterator.Item item=inv_file_iter.next();
                int o=item.getObject();
                int s=item.getScore();
                int inc=Math.abs(pos-s);    //the "sperman footrule" distance is the sum of the position differences
//                int inc=(pos-s)*(pos-s);    //the "sperman rho" distance is the sum of the position differences power 2
                
                //temp_res.put(o,inc,ks,pos+1); //here we use ks as number of posting lists to be accessed. However the real number of posting list is the size of q_knp
                temp_res.put(o,inc,q_knp.size(),pos+1);
            }
            if (isSaveMemoryMode())
                invFile.releasePostingList(piv); //memory saving but less efficiency
        }

         //previous version: perform oredering of the result according to the abstract distance
/*         Iterator temp_iter=temp_res.iterator();
         TreeMap res=new TreeMap();
         System.err.println("Found " + temp_res.size()+" objects");
         while(temp_iter.hasNext())
         {
             Map.Entry e =(Map.Entry)temp_iter.next();
             Integer o=(Integer) e.getKey();
             double dist=((TempRes.ObjInfo)e.getValue()).getCurrentScore();
             while(o!=null)     //if two objects have the same distance the old one is not replaced but it is given dist+0.0001
             {
                 o=(Integer)res.put(dist,o);
                 dist+=Math.random()*0.0001;
             }
         }
         return res;/**/

        //new version: exploiting the ordering performed by the put method
        TreeMap res=temp_res.getOrderedRes();
        indexAccessTime=System.currentTimeMillis()-time;
        tempResultmanagementTime=temp_res.getPutTime();
        numberOfObjectsFound=temp_res.numberOfObjectsFound();
        temp_res.recycle();
        executedQueries++;
        return res;
     }

    

    
}
