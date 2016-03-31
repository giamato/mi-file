/*
 * kNearestPivots.java
 *
 * Created on 12 aprile 2007, 17.11
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package it.cnr.isti.SimilaritySearch.MI_File;


import it.cnr.isti.PersistentDataset.*;
import it.cnr.isti.Dataset.*;
import it.cnr.isti.InvertedFile.*;
import it.cnr.isti.SimilaritySearch.SimilaritySearchIndex.SimilaritySearch;
import java.util.*;
import java.io.*;
import java.nio.ByteBuffer;

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
 * 1) You have to create a class that extends the abstract class PersistentDataset.PersistentDatasetObject that
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
    private String mode;
    
    

   
    
    /** Open an MI-File in read-only mode. To open the Mi-File in write mode,
     * the equivalent constructor accxepting the mode parameter shouls be used.
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
     * @param ref_obj_file specifies the pathname of the file containg the reference objects (See
     * javadoc for ReferenceObjects)
     */
    public MI_File(int p_n_ro, int p_ki, int p_ks, String p_indexDirectory, String ref_obj_file) {
        this("r",p_n_ro, p_ki, p_ks, p_indexDirectory, ref_obj_file);
    }


    /** Open an MI-File specifying the access mode. To open the Mi-File in write mode,
     * the equivalent constructor accxepting the mode parameter shouls be used.
     * If no existing index is contained in the directory specified by
     * p_indexDirectory, or if the directory does not exists, an empty index is created in the directory.
     * The MI-File can be closed by using the close() method.
     * @param mode access mode "r", "rw", "rws", "rwd"
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
    public MI_File(String mode,int p_n_ro, int p_ki, int p_ks, String p_indexDirectory, String ref_obj_file) {
        try{
            this.mode=mode;
            n_ro=p_n_ro;
            ki=p_ki;
            ks=p_ks;
            max_pos_diff=ki;
            //max_pos_diff=ks;
            indexDirectory=p_indexDirectory;
            ros=new ReferenceObjects();
            ros.set(ref_obj_file);
            ros.setNumOfReferenceObjects(p_n_ro);
            invFile=new InvertedFile(n_ro,ki,indexDirectory);
            dataset=new PersistentSimilarityDatasetCompressed(indexDirectory,mode);
        }catch(Exception e){
            e.printStackTrace();
        }
    }

    /**Open an MI-File in read-only modeby specifying where the database of objects is located.
     * To open the Mi-File in write mode,
     * the equivalent constructor accxepting the mode parameter shouls be used. By default the database
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
    public MI_File(int p_n_ro, int p_ki, int p_ks, String p_indexDirectory, String p_dbDirectory, String ref_obj_file){
        this("r",p_n_ro, p_ki, p_ks, p_indexDirectory, p_dbDirectory, ref_obj_file);
    }

    /**Open an MI-File specifying the access mode and where the database of objects is located.
     * To open the Mi-File in write mode,
     * the equivalent constructor accxepting the mode parameter shouls be used. By default the database
     * is contained in the same directory of the index itself. If you need to store the database in a
     * different location than the index, then you have to use this constructor.
     *
     * If no existing index is contained in the directory specified by
     * p_indexDirectory, or if the directory does not exists, an empty index is created in the directory.
     * The MI-File can be closed by using the close() method.
     * @param mode access mode "r", "rw", "rws", "rwd"
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
    public MI_File(String mode,int p_n_ro, int p_ki, int p_ks, String p_indexDirectory, String p_dbDirectory, String ref_obj_file) {
//        super(dataset);
        try{
            this.mode=mode;
            n_ro=p_n_ro;
            ki=p_ki;
            ks=p_ks;
            max_pos_diff=ki;
            //max_pos_diff=ks;
            indexDirectory=p_indexDirectory;
            ros=new ReferenceObjects();
            ros.set(ref_obj_file);
            ros.setNumOfReferenceObjects(p_n_ro);
            invFile=new InvertedFile(n_ro,ki,indexDirectory);
            dataset=new PersistentSimilarityDatasetCompressed(p_dbDirectory,mode);
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
            return dataset.size();
        }catch(Exception e){
            return 0;
        }
    }
    
    /**
     * Close the current MI-File. All files are closed and all in-memory data structures are freed.
     */
    public void close(){
        try{
            if(invFile!=null)
                invFile.close();
            if(dataset!=null)
                dataset.close();
        invFile=null;
        dataset=null;
        }catch(Exception e){
            e.printStackTrace();
        }
    }
    
   
    @Override
    protected void finalize () throws Throwable{
        super.finalize();
        ros=null;
        close();
    }

    /**
     * Returns the associated dataset/database.
     * @return the dataset
     */
    @Override
    public SimilarityDataset getDataset(){
        return dataset;
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
     * When an MI-File is open or created, it is initially set to ki.
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
        if(mode=="rw"||mode=="rws"||mode=="rwd"){
            File bulk_load_done_f=new File(indexDirectory+"/bulk_load_done.dat");
            File bulk_load_in_progress_f=new File(indexDirectory+"/bulk_load_in_progress.dat");
            File indexDirectory_f=new File(indexDirectory);
            if(!indexDirectory_f.exists())
                indexDirectory_f.mkdirs();
            if(!bulk_load_in_progress_f.exists()){
                bulk_load_done_f.delete();
                bulk_load_in_progress_f.createNewFile();
            }else
                throw new Exception("Cannot sart bulk load...another bulk load already in progress");
        }else
            throw new Exception("MI-File index is read-only. Cannot execute bulk load.");
    }

    /**
     * Ends current bulk loading of the index. Insertions are executed using bulk loading. First beginBulkLoad() is
     * called. Then you can call bulkInsert() repetitively to insert as meny objects as you want inthe index,
     * finally you call endBulLoad() to commit the insertions. Bulk Loading can be executed many times.
     * @throws Exception if something goes wrong, an exception is thrown.
     */
    public void endBulkLoad() throws Exception{
        if(mode=="rw"||mode=="rws"||mode=="rwd"){
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
        }else
            throw new Exception("MI-File index is read-only. Cannot execute end bulk load.");
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
     * by using this.getDataset().getObjectFromOffset(identifier), or useing the kNNRetrieve() or kNNRetrieveAndSort()
     * methods. The MI_File makes the association between objects
     * and internal identifiers by calling the method setInternalId() of the object during the insertion.
     * Therefore any other use of the method setInternalId() for other purposes is dangerous.
     *
     *
     * @param obj the object to be inserted. obj should be of type PersistentSimilarityDatasetObject
     * @return the internal identifier assigned to the inserted object.
     * @throws Exception if something goes wrong, an exception is thrown.
     */
    public int bulkInsert(PersistentSimilarityDatasetObject obj) throws Exception{
        if(mode=="rw"||mode=="rws"||mode=="rwd"){
            File bulk_load_in_progress_f=new File(indexDirectory+"/bulk_load_in_progress.dat");
            if(bulk_load_in_progress_f.exists()){
                int id=(dataset).insert(obj);
                TreeMap<Double,Integer> knp;
                if(isUseSmartNumberOfClosestROs()){
                    knp=ros.kNearestReferenceObjectsSmart(obj,ki);
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
        }else throw new Exception("MI-File index is read-only. Cannot insert objects.");
    }

    public void optimizeStorage() throws Exception{
         if(mode=="rw"||mode=="rws"||mode=="rwd"){
             File defragTempDir=new File(this.indexDirectory+"/defrag");
             if(!defragTempDir.exists())
                 defragTempDir.mkdirs();
             //fileCopy(this.indexDirectory+"/offsets.dat",this.indexDirectory+"/defrag/offsets.dat");
             File defragOffsetFile =new File(this.indexDirectory+"/defrag/offsets.dat");
             if(defragOffsetFile.exists())
                 if(!defragOffsetFile.delete())
                     throw new Exception("optimized offset file already exists. Cannot proceed. Optimization aborted");
             RandomAccessFile raDefragOffsetFile=new RandomAccessFile(defragOffsetFile,"rw");
             
             
             /*for(long l=0;l<dataset.size();l++)
                raDefragOffsetFile.writeLong(-1);
             raDefragOffsetFile.close();*/
             
             int bufferSize=100000;
            //System.out.println("initializing with buffer size "+bufferSize);
            byte buffer[]=new byte[bufferSize*8];
            ByteBuffer bb=ByteBuffer.wrap(buffer);
            for(int i=0;i<bufferSize;i++)
                bb.putLong(-1);
            long q=dataset.size()/bufferSize;
            int r=(int)dataset.size() % bufferSize;
            for(int i=0;i<q;i++)
                raDefragOffsetFile.write(buffer);
            raDefragOffsetFile.write(buffer, 0, r*8);
             
             
             
             
             PersistentSimilarityDatasetCompressed defragmented=new PersistentSimilarityDatasetCompressed(this.indexDirectory+"/defrag/", "rw");
             for(int piv=0;piv<n_ro;piv++){
                 System.err.println("optimize objects closest to reference object "+piv);
                 for(PostingListEntry e:invFile.getPostingList(piv, 0, 0)){
                     PersistentSimilarityDatasetObject obj =(PersistentSimilarityDatasetObject)dataset.getObject(e.getObject());
                     /*boolean fault=true;
                     try{
                        defragmented.getObject(obj.getInternalId());
                     }catch(Exception ex){
                         fault=false;
                     }
                     if(fault)
                         System.err.println("object "+obj.getInternalId()+" is already in the database");*/
                     defragmented.update(obj.getInternalId(), obj);
                 }
             }
             defragmented.close();
             dataset.close();      
             File offsets=new File(this.indexDirectory+"/offsets.dat");
             File database=new File(this.indexDirectory+"/database.dat");
             if(!offsets.delete())
                 throw new Exception("Failed to delete non optimized offset.dat and database.dat files. The optimized files can be found in the defrag directory");
             if(!database.delete())
                 throw new Exception("Failed to delete non optimized database.dat file. The optimized file can be found in the defrag directory");
             if(!defragOffsetFile.renameTo(new File(this.indexDirectory+"/offsets.dat")))
                throw new Exception("Failed to move cleaned offset.dat and database.dat files. The optimized files can be found in the defrag directory");
             File defragDatabaseFile =new File(this.indexDirectory+"/defrag/database.dat");
             if(!defragDatabaseFile.renameTo(new File(this.indexDirectory+"/database.dat")))
                throw new Exception("Failed to move cleaned database.dat files. The optimized files can be found in the defrag directory");
             dataset=new PersistentSimilarityDatasetCompressed(indexDirectory,mode);
         }else throw new Exception("MI-File index is read-only. Cannot optimize database storage.");
    }

    static private void fileCopy(String file1,String file2) throws IOException {
		File f1=new File(file1);
		File f2=new File(file2);
		InputStream in=new FileInputStream(f1);
		OutputStream out=new FileOutputStream(f2);
		byte[] buf = new byte[1024];
        int len;
        while ((len = in.read(buf)) > 0){
          out.write(buf, 0, len);
        }
        in.close();
        out.close();
    }


    
    /**
     * Computes distance between two objects.
     * @param o_1 first object
     * @param o_2 second object
     * @return distance between o_1 and o_2
     */
    @Override
    public double distance(SimilarityDatasetObject o_1, SimilarityDatasetObject o_2)  //do we need this? or the Default distance is enough?
    {
        int dist=0;
        TreeMap q_knp;
        if(this.isUseSmartNumberOfClosestROs())
            q_knp=ros.kNearestReferenceObjectsSmart((PersistentSimilarityDatasetObject)o_1, ks);
        else
            q_knp=ros.kNearestReferenceObjects((PersistentSimilarityDatasetObject)o_1,ks);
        Iterator q_iter=q_knp.keySet().iterator();
        int q_p_pos=0;
        while(q_iter.hasNext())
        {
            q_p_pos++;
            Integer q_p=(Integer)q_knp.get(q_iter.next());
            int obj_p_pos=0;
            boolean found=false;
            TreeMap knp;
            if(this.isUseSmartNumberOfClosestROs())
                knp=ros.kNearestReferenceObjectsSmart((PersistentSimilarityDatasetObject)o_2, ks);
            else
                knp=ros.kNearestReferenceObjects((PersistentSimilarityDatasetObject)o_2,ks);
            Iterator obj_iter=knp.keySet().iterator();
            while(obj_iter.hasNext()&&!found)
            {
                obj_p_pos++;
                Integer obj_p=(Integer) knp.get(obj_iter.next());
                if(obj_p.equals(q_p))
                    found=true;
            }
            int d;
            if(found)
                d=java.lang.Math.abs(q_p_pos-obj_p_pos);
            else
                d=obj_p_pos+1;
            dist+=d;
        }
        return dist;
    }

    private boolean saveMemoryMode=true;

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
     * by using this.getDataset().getObjectFromOffset(identifier).
     * @param q the query object
     * @param k the number of retrieved objects
     * @return a TreeMap containing k pairs (distance, identifier) ordered according to the distance
     */
    @Override
    public TreeMap<Double,Object> kNN(SimilarityDatasetObject q, int k){
         //PURE APPROXIMATE RESULT
         databaseAccessTime=0;
         TreeMap res=incrementalkNNSearch(q,k);
         totalSearchTime=indexAccessTime;
         return res;
     }

    /**
     * Retrieves the k closes objects to the query, along
     * with their spearman footrole distance to the query object.
     * @param q the query object
     * @param k the number of retrieved objects
     * @return a TreeMap containing k pairs (distance, SimilarityDatasetObject) ordered according to the distance
     */
    public TreeMap<Double,Object> kNNRetrieve(SimilarityDatasetObject q, int k){
         TreeMap<Double,Integer> q_knp;
         if(isUseSmartNumberOfClosestROs()){
            q_knp=ros.kNearestReferenceObjectsSmart((PersistentSimilarityDatasetObject)q,ks);
         }else
             q_knp=ros.kNearestReferenceObjects((PersistentSimilarityDatasetObject)q,ks);
         return kNNRetrieve(q, q_knp,k);
     }


    /**
     * Retrieves the k closes objects to the query, along
     * with their real distance distance to the query object. The result list is sorted according
     * to the real distance between objects and the query, rather than the spearman foortrule distance.
     * The k closest objects are obtained by retrieving k*amp objects
     * (sorted according to the spearmann footrule distance) and by taking the k closest to the query
     * according to the actual distance
     * @param q the query object
     * @param k the number of retrieved objects
     * @param amp the k best objects from the k*amp obejcts retrieved will be returned
     * @return a TreeMap containing k pairs (distance, SimilarityDatasetObject) ordered according to the distance
     */
    public TreeMap<Double,Object> kNNRetrieveAndSort(SimilarityDatasetObject q, int k, int amp){
        //System.out.println("k:"+k+" ro:"+n_ro+" ks:"+ks+" ki:"+ki+" mpd:"+max_pos_diff+" amp:"+amp);
         //SMART impplementation:
         k=k*amp;
         TreeMap res= incrementalkNNSearch(q,k);
         //reordering of the first k results according to the object id to perform a sequential scan in the db
         long time=System.currentTimeMillis();
         TreeMap res1= new TreeMap();
         Iterator res_iter=res.values().iterator();
         for(int i=0;i<k;i++)
             if(res_iter.hasNext()){
                Integer o= (Integer)res_iter.next();
                 res1.put((double)o,o);
             }
         TreeMap<Double,Object> res_sorted=  sortAndRetrieve(res1,q,k);
         databaseAccessTime=System.currentTimeMillis()-time;
         totalSearchTime=indexAccessTime+databaseAccessTime;
//         System.err.println("Elapsed time: Index acces: "+ indexAccessTime +", Object retrieve and sort: "+(databaseAccessTime));
         TreeMap<Double,Object> res_final=new TreeMap<Double,Object>();
         int count=k/amp;
         for(Map.Entry e:res_sorted.entrySet()){
             count--;
             if(count<0)
                 break;
             res_final.put((Double)e.getKey(), e.getValue());
         }
         return res_final;

     }
     
    
     
     

    /**
     * Return the number of entries read in the posting list, since the last resetNumberOfReads() was executed.
     * @return number of entries read
     */
    public static long getNumberOfReads(){
        return PostingListIterator.getNumOfReads();
    }

    /**
     * Reset the counter for the number of reads.
     */
    public static void resetNumberOfReads(){
        PostingListIterator.resetNumOfReads();
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

    private boolean useSmartNumberOfClosestROs=false;

    /**
     * Checks if a smart strategy is used to estimate the appropriate value of ki and ks
     * @return true if intrinsic dimensionality is used, false if it is not used (in this case ki and ks shoule be set explicitely)
     */
    public boolean isUseSmartNumberOfClosestROs() {
        return useSmartNumberOfClosestROs;
    }

    /**
     * Set a smart strategy to be used to estimate the appropriate value of ki and ks.
     * IMPORTANT: With current implementation the quality of retrieval is very poor
     * This was just experimental. It is better to leave it as false (default usage).
     *
     * @param useIntrinsicDimensionality true if intrinsic dimensionality should be used, false if it should not be used (in this case ki and ks shoule be set explicitely). Defoult is false
     */
    public void setUseSmartNumberOfClosestROs(boolean useIntrinsicDimensionality) {
        this.useSmartNumberOfClosestROs = useIntrinsicDimensionality;
    }

    /**
     * Set the strategy to be used to compute the number of closest reference objects
     * @param s the strategy
     */
    public void setSmartNumberOfClosestROsStrategy(int s){
        ros.setSmartStrategy(s);
    }



    /**
     * Set the threshold for the smart strategy if needed.
     * @param tr
     */
    public void setSmartThreshold(double tr){
        ros.setSmartThreshold(tr);
    }


    private TreeMap incrementalkNNSearch(SimilarityDatasetObject q, int k){
        TreeMap<Double,Integer> q_knp;
        if(isUseSmartNumberOfClosestROs()){
            q_knp=ros.kNearestReferenceObjectsSmart((PersistentSimilarityDatasetObject)q,ks);
            ki=q_knp.size();  //we will not use the maximum value of ki, but that of the query.
        }else
            q_knp=ros.kNearestReferenceObjects((PersistentSimilarityDatasetObject)q,ks);
        return incrementalkNNSearch(q, q_knp,k);
    }

   


    private TreeMap kNNRetrieve(SimilarityDatasetObject q, TreeMap q_knp, int k){
         //SMART impplementation:
         TreeMap res= incrementalkNNSearch(q,q_knp,k);
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

     private TreeMap incrementalkNNSearch(SimilarityDatasetObject q, TreeMap q_knp, int k)
     {
        TreeMap res=null;
        try{
            long time=System.currentTimeMillis();
            //CandidateSet candidateSet=new CandidateSet(ki+1,k);
            CandidateSet candidateSet=new CandidateSetWithPermutationDistance(ki+1,k);
            //CandidateSet candidateSet=new CandidateSetWithPivotDistance(ros,dataset,q,k);
            //CandidateSet_OK candidateSet=new CandidateSet_OK(ki+1,k);
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
                //PostingListIterator inv_file_iter=invFile.getPostingList(piv,low_inv_file_pos,high_inv_file_pos);
                Set<PostingListEntry> entries= invFile.getPostingList(piv, low_inv_file_pos,high_inv_file_pos);
                //int n=dataset.size();
                //int pl_size=inv_file_iter.getPostingListSize();
                //int portion_size=(int)inv_file_iter.getPostingListPortionSize();
                //System.out.println(pos+"--"+piv+"---"+portion_size);
                //double pl_idf=Math.log((double)n/(double)pl_size);
                //double portion_idf=Math.log((double)n/(double)portion_size);
                //if(pl_idf>1)
                int size=entries.size();
                for(PostingListEntry item:entries)
                {
                    int o=item.getObject();
                    int s=item.getScore();
                    //candidateSet.put(o,inc,ks,pos+1); //here we use ks as number of posting lists to be accessed. However the real number of posting list is the size of q_knp
                    //candidateSet.put(o,s,pos,piv,ks,pos+1);//here we use ks as number of posting lists to be accessed. However the real number of posting list is the size of q_knp
                    candidateSet.put(o,s,pos,piv,q_knp.size(),pos+1);
                }
                if (isSaveMemoryMode())
                    invFile.releasePostingList(piv); //memory saving but less efficiency
            }

             //previous version: perform oredering of the result according to the abstract distance
    /*         Iterator temp_iter=candidateSet.iterator();
             TreeMap res=new TreeMap();
             System.err.println("Found " + candidateSet.size()+" objects");
             while(temp_iter.hasNext())
             {
                 Map.Entry e =(Map.Entry)temp_iter.next();
                 Integer o=(Integer) e.getKey();
                 double dist=((CandidateSet.ObjInfo)e.getValue()).getCurrentScore();
                 while(o!=null)     //if two objects have the same distance the old one is not replaced but it is given dist+0.0001
                 {
                     o=(Integer)res.put(dist,o);
                     dist+=Math.random()*0.0001;
                 }
             }
             return res;/**/

            //new version: exploiting the ordering performed by the put method
            res=candidateSet.getSortedCandidateSet();
            indexAccessTime=System.currentTimeMillis()-time;
            tempResultmanagementTime=candidateSet.getPutTime();
            numberOfObjectsFound=candidateSet.numberOfObjectsFound();
            candidateSet.recycle();
            executedQueries++;
        }catch(Exception e){
            e.printStackTrace();
        }
        return res;
     }

    

    
}
