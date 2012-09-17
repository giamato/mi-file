package Examples;


import java.util.TreeMap;
import java.io.*;

import it.cnr.isti.SimilaritySearch.MI_File.ReferenceObjects;
import it.cnr.isti.SimilaritySearch.MI_File.MI_File;
import it.cnr.isti.SimilaritySearch.Dataset.DatasetObject;
import it.cnr.isti.SimilaritySearch.Dataset.Dataset;

/**
 * This class demonstrates how to use the MI_File class.
 * @author Giuseppe Amato
 * ISTI-CNR, Pisa, Italy
 * giuseppe.amato@isti.cnr.it
 */
public class MI_File_Usage_Example {

    int datasetSize=100000;
    int ref_objects_num=1000;
    int ki=32;
    int ks=20;
    int maxPosDiff=15;
    String indexDirectory=".//examples/index";
    String referenceObjectsFile=".//examples//Vector32DRefObj.dat";

    public static void main(String[] args){
        MI_File_Usage_Example example=new MI_File_Usage_Example();


        example.createReferenceObjects();
        example.populateMI_File();
        example.searchMI_File();
    }

    /**
     * Example on how to create a set of reference objects
     */
    public void createReferenceObjects(){

        System.out.println("Creating "+ref_objects_num+ " reference objects...");

        //We first create the directory to store the referenceo object file
        //in case it does not exists.
        File indexDirectory_f=new File(indexDirectory);
                    if(!indexDirectory_f.exists())
                        indexDirectory_f.mkdirs();

        //Let's check if a reference object file already exists.
        //We do not want to overwrite it. Existing MI_File (if any)
        //would become unusable.
        File file=new File(referenceObjectsFile);
        if (!file.exists()){
            //Create an empty set of Reference Objects
            ReferenceObjects ros=new ReferenceObjects();
            ros.initializeEmpty(ref_objects_num);

            //now we add the reference objects
            for(int i=0;i<ref_objects_num;i++){
                DatasetObject ref_obj=new Vector32D(i,null);
                //Here we initialize it randomly. However this is just an example.
                //In reality you migh want to use a random sample of objects from the real dataset to be indexed.
                ((Vector32D)ref_obj).initializeRandom();
                //Let's add it to the set of reference objects
                ros.add(ref_obj);
            }
            //now we can save the set of reference objects
            ros.save(referenceObjectsFile);
        }
        else
            System.out.println("Reference object file already exist! Manually remove it if you want to create a new one.");
    }


    /**
     * Populate the MI_File.
     */
    public void populateMI_File(){

        System.out.println("Inserting "+datasetSize+ " objects in the database. It might take a while...");

        //Create and/or open the MI_File
        MI_File mi_file=new MI_File(ref_objects_num, ki, ks, indexDirectory, referenceObjectsFile);

        try{
            //Let's begin bulk load
            mi_file.beginBulkLoad();

            //now we insert the objects
            for(int i=0;i<datasetSize;i++){
                DatasetObject obj=new Vector32D(i,null);
                //Here we initialize it randomly. However this is just an example.
                //In reality you will insert the objects from the real dataset to be indexed.
                ((Vector32D)obj).initializeRandom();
                //Let's insert it
                mi_file.bulkInsert(obj);
                if((i % 1000)== 0 )
                    System.out.println(i + " objects inserted in the MI-File...");
            }

            //now we can end bulk load
            mi_file.endBulkLoad();
            //We can also close the MI_File
            mi_file.close();
        }catch(Exception e){
            e.printStackTrace();
        }
    }

    /**
     * Execute searches in the MI-File
     */
    public void searchMI_File(){

        System.out.println("Performing similarity searches...");

        int queryObjectId=30; //The query
        int k=10; //we retrieve k objects
        int amp=10; //we sort k*amp objects

        //Open the MI_File
        MI_File mi_file=new MI_File(ref_objects_num, ki, ks, indexDirectory, referenceObjectsFile);

        //Let's play with parameters
        mi_file.setKs(ks);
        mi_file.setMaxPosDiff(maxPosDiff);

        //Let's get an object from the database to use as query. Note that you
        //can also use objects not inthe database.
        DatasetObject query=mi_file.getDataset().getObject(queryObjectId);

        //Now let's search for the k most similar objects to it...
        TreeMap<Double,Object> res=mi_file.kNN(query, k);
        //...and print the result
        System.out.println("\nkNN search Result:");
        this.printRes(res, queryObjectId, k, mi_file.getDataset());
        
        //Let's retrieve the actual objects not just their identifiers
        res=mi_file.kNNRetrieve(query, k);
        //...and print the result
        System.out.println("\nkNNretrieve search Result:");
        this.printRes(res, queryObjectId, k, mi_file.getDataset());

         //Let's retrieve the actual objects not just their identifiers and als sort the results according to the real distance
        res=mi_file.kNNRetrieveAndSort(query, k,amp);
        //...and print the result
        System.out.println("\nkNNretrieveAndSort search Result:");
        this.printRes(res, queryObjectId, k, mi_file.getDataset());
    }
    
    private void printRes(TreeMap<Double,Object> inp, int objectQuery,int k,Dataset dataset)
    {
            java.util.Iterator s=inp.keySet().iterator();
            for(int i=0;s.hasNext()&&i<k;i++)
            {
                double distance=(Double)s.next();
                Object o=inp.get(distance);
                System.out.print(i);
                String className=o.getClass().getName();
                if(className.equals("java.lang.Integer"))
                    System.out.print(" object: "+o);
                else
                    System.out.print(" object: "+((DatasetObject)o).getInternalId());

                System.out.print("\t Computed distance: "+ distance);
                if(dataset!=null){
                    if(className.equals("java.lang.Integer"))
                        System.out.println("\t Actual distance: "+ dataset.distance(dataset.getObject(objectQuery),dataset.getObject((Integer)inp.get(distance))));
                    else
                        System.out.println("\t Actual distance: "+ dataset.distance(dataset.getObject(objectQuery), (DatasetObject)o));
                }
                else
                    System.out.println();
            }
    }


}
