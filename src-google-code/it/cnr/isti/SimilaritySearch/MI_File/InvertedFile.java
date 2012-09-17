/*
 * InvertedFile.java
 *
 * Created on 19 aprile 2007, 21.19
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package it.cnr.isti.SimilaritySearch.MI_File;



import java.io.*;
import java.nio.*;

/**
 *
 * @author amato
 */
class InvertedFile {
    private String indexDirectory;
    static private InvertedFileIterator[] iterators;
    private int lexiconSize;
    static private Integer instanceCounter=Integer.valueOf(0);
    static private BufferedOutputStream[] writeInvFile;
    private int bufferSize=4096*2;
    
    
    /** Creates a new instance of InvertedFile */
    public InvertedFile(int lexiconSizePar, String p_indexDirectory) {
        synchronized(this){
            lexiconSize=lexiconSizePar;
            indexDirectory=p_indexDirectory;
            if(iterators==null)
                iterators =new InvertedFileIterator[lexiconSize];
            if(writeInvFile==null)
                writeInvFile=new BufferedOutputStream[lexiconSize];
        }
        synchronized(instanceCounter){
            instanceCounter++;
//            System.err.println("Created a new inverted file instance. Total instances: "+instanceCounter);
        }
    }
    
    public void insert(int obj, int entry, int score)
    {
        try{
            if (writeInvFile[entry]==null){
                String newDirectory=indexDirectory+"/new";
                File pl_f=new File(newDirectory+"/pl_e"+entry+".dat");
                if(!pl_f.exists()){
                    File indexDirectory_f=new File(indexDirectory);
                    if(!indexDirectory_f.exists())
                        indexDirectory_f.mkdirs();
                    File newDirectory_f=new File(newDirectory);
                    if(!newDirectory_f.exists())
                        newDirectory_f.mkdirs();
                    pl_f.createNewFile();
                }
                writeInvFile[entry]=new BufferedOutputStream(new FileOutputStream(pl_f,true),bufferSize);
            }            
            byte[] obj_buff=it.cnr.isti.SimilaritySearch.MI_File.Conversions.intToByteArray(obj);
            byte[] score_buff=it.cnr.isti.SimilaritySearch.MI_File.Conversions.intToByteArray(score);
            writeInvFile[entry].write(obj_buff);
            writeInvFile[entry].write(score_buff);
//            pl_w.close();
        }catch(Exception e)
        {        
            System.err.println("Cannot create or insert entry in posting list for entry "+entry+" and score "+score);
            e.printStackTrace();
        }
    }

    public void releasePostingList(int entry){
        synchronized(iterators){
            if (iterators[entry]!=null){
                iterators[entry].close();
                iterators[entry]=null;
            }
        }
    }
    
    public InvertedFileIterator getPostingList(int entry)
    {
        synchronized(iterators){
            if (iterators[entry]==null)
                iterators[entry]=new InvertedFileIterator(entry,indexDirectory,lexiconSize,this);
        }
        iterators[entry].initialisation(0,lexiconSize);
        return iterators[entry];
    }
    
    public InvertedFileIterator getPostingList(int entry,int fromScore)
    {
        synchronized(iterators){
            if (iterators[entry]==null)
                iterators[entry]=new InvertedFileIterator(entry,indexDirectory,lexiconSize,this);
        }
        iterators[entry].initialisation(fromScore,lexiconSize);
        return iterators[entry];
    }
    
    public InvertedFileIterator getPostingList(int entry,int fromScore, int toScore)
    {
//        System.err.println("Total memory: "+Runtime.getRuntime().totalMemory()+"Free memory: "+Runtime.getRuntime().freeMemory()+"Max memory: "+Runtime.getRuntime().maxMemory());
        synchronized(iterators){
            if (iterators[entry]==null)
                iterators[entry]=new InvertedFileIterator(entry,indexDirectory,lexiconSize,this);
        }
        iterators[entry].initialisation(fromScore,toScore);
        return iterators[entry];
    }

    public void flush() {
        System.err.println("Flushing inverted file...");
        for(int entry=0;entry<writeInvFile.length;entry++)
            if(writeInvFile[entry]!=null)
                try{
                    writeInvFile[entry].flush();
                    writeInvFile[entry].close();
                    writeInvFile[entry]=null;
                }catch(Exception e){
                    System.err.println("Cannot flush inverted file.");
                    e.printStackTrace();
                }
        System.err.println("Finished flushing inverted file!");
    }
    
    protected void finalize() throws Throwable{
        synchronized(instanceCounter){
            instanceCounter--;
//            System.err.println("Finalizing an expired inverted file instance. Total instances: "+instanceCounter);
            if(instanceCounter==0)
                close();
        }
    }
    
    
    public void clean(){
        synchronized(instanceCounter){         
//        System.err.printf("Cleaning inverted file...");
            for(int entry=0;entry<lexiconSize;entry++)
                if(iterators[entry]!=null){
                    iterators[entry].close();
                    iterators[entry]=null;
                }
//        System.err.println("Finished Cleaning inverted file!");
        }
    }


    public void close(){
        flush(); // if something goes wrong, the new posting lists might not be stored on disk. Let's flush them!
        synchronized(instanceCounter){
            if(instanceCounter==0){
                clean();
/*                System.err.printf("Closing inverted file...");
                for(int entry=0;entry<lexiconSize;entry++)
                    if(iterators[entry]!=null){
                        iterators[entry].close();
                        iterators[entry]=null;
                    }
                System.err.println("Finished closing inverted file!");*/
            }
        }
    }

    private void moveInvertedFileInTemp(String invFileDir, String tempDir) throws Exception{
        // move the old inverted file in the temp directory
        System.err.println("moving the old inverted file in the temp directory...");
        File temp_f=new File(tempDir);
        if(temp_f.exists())
            if (!temp_f.delete())
                throw new Exception("Cannot delete the temporary directory. endBulkLoad failed!");

        File invFile_f=new File(invFileDir);

        if(invFile_f.exists())
            if(!invFile_f.renameTo(temp_f))
                throw new Exception("Cannot move old posting lists in the temporary directory. endBulkLoad failed!");;

        if(!invFile_f.exists())
            if(!invFile_f.mkdir())
                throw new Exception("Cannot create the directory for the new sorted posting lists. endBulkLoad failed!");;
        
    }

    private long countObjectsWithScoreInOldPostingLists(RandomAccessFile ospl_ra, int[] scoreCounter, int[] objectsWithScore, int sizeOfPostingListIndex) throws Exception{

        MappedByteBuffer ospl=ospl_ra.getChannel().map(java.nio.channels.FileChannel.MapMode.READ_ONLY,0,sizeOfPostingListIndex); //NEW

        long sizeOfOldPostingList=0;
        //ospl_ra.seek(0); //old
        ospl.position(0); //new
        sizeOfOldPostingList=(ospl_ra.length()-sizeOfPostingListIndex);
        //int lastPos=ospl_ra.readInt();// old
        int lastPos=ospl.getInt(); //new
        for(int i=0;i<writeInvFile.length;i++){
            //int pos=ospl_ra.readInt();//old
            int pos=ospl.getInt();
            scoreCounter[i]=(pos-lastPos)/InvertedFileIterator.getElementSize(); //Score counter should be initialized with the number of entry present in the already existing posting list
            objectsWithScore[i]=scoreCounter[i]; //at the beginnng we will insert the objects in the old posting lists so the already inserted objects is set to that number
            lastPos=pos;
        }
        return sizeOfOldPostingList;
    }

    private void countObjectsWithScoreInUnsortedPostingLists(MappedByteBuffer pl, int[] scoreCounter) throws Exception{
        while(pl.hasRemaining()){
            int o=pl.getInt();
            int score=pl.getInt();
            scoreCounter[score]++;
        }
    }

    private void copyOldIntoNewPostingList(RandomAccessFile ospl_ra, MappedByteBuffer spl, int[] scoreCounter, int[] objectsWithScore, int sizeOfPostingListIndex) throws Exception{
        int insertedObjects=0;
        int writePosition=sizeOfPostingListIndex;
        for(int i=0;i<writeInvFile.length;i++){
            int startPosition=sizeOfPostingListIndex+insertedObjects*InvertedFileIterator.getElementSize();
            int buffSize=objectsWithScore[i]*InvertedFileIterator.getElementSize();
            insertedObjects+=objectsWithScore[i];
            MappedByteBuffer ospl=ospl_ra.getChannel().map(java.nio.channels.FileChannel.MapMode.READ_ONLY,startPosition,buffSize);
            spl.position(writePosition);
            spl.put(ospl);
            writePosition+=scoreCounter[i]*InvertedFileIterator.getElementSize();
        }
        ospl_ra.getChannel().close();
        ospl_ra.close();
    }

    private void copyUnsortedIntoNewPostingList(MappedByteBuffer pl, MappedByteBuffer spl, int[] scoreCounter, int[] objectsWithScore, int sizeOfPostingListIndex){
        pl.position(0);
        while(pl.hasRemaining()){
            int o=pl.getInt();
            int score=pl.getInt();
            int write_position=0;
            for(int si=0;si<score;si++)
                write_position+=scoreCounter[si];
            write_position+=objectsWithScore[score];
            int write_position_in_bytes=sizeOfPostingListIndex+write_position*InvertedFileIterator.getElementSize();
            spl.position(write_position_in_bytes);
            spl.putInt(o);
            objectsWithScore[score]++;
        }
    }

    private void updatePostingListIndex(MappedByteBuffer spl,int[] scoreCounter, int sizeOfPostingListIndex){
        int score_position=0;
        int s=0;
        spl.position(s);
        int score_position_in_bytes=sizeOfPostingListIndex+score_position*InvertedFileIterator.getElementSize();
        spl.putInt(score_position_in_bytes);
        for(s=0;s<scoreCounter.length;s++){
            spl.position((s+1)*4);
            score_position+=scoreCounter[s];
            score_position_in_bytes=sizeOfPostingListIndex+score_position*InvertedFileIterator.getElementSize();
            spl.putInt(score_position_in_bytes);
        }
    }

    private void closeOpenFiles(MappedByteBuffer pl, MappedByteBuffer spl, RandomAccessFile pl_ra, RandomAccessFile spl_ra) throws Exception{
        spl.force();
        spl_ra.getChannel().close();
        spl_ra.close();
        spl=null;
        //close the unordered posting list
        pl_ra.getChannel().close();
        pl_ra.close();
        pl=null;
    }

    private void cleanUpOldAndTemporaryfiles(String newDir, String tempDir){
        System.err.println("deleting unneeded temporary files");
        System.gc();
        for(int entry=0;entry<writeInvFile.length;entry++){
            File pl_f=new File(newDir+"/pl_e"+entry+".dat");
            File ospl_f=new File(tempDir+"/pl_e"+entry+"_sorted.dat");
            //delete the unordered posting list
            if(pl_f.exists()){
                boolean deleted=false;
                for(int i=0;i<10000&&!deleted;i++)
                    deleted=pl_f.delete();
                if(!deleted)
                     System.err.println("Warining failed to delete unordered posting list");
            }
            //delete the old renamed ordered posting list
            if(ospl_f.exists()){
                boolean deleted=false;
                for(int i=0;i<10000&&!deleted;i++){
                    deleted=ospl_f.delete();
                }
                if(!deleted)
                         System.err.println("Warining failed to delete old ordered posting list");
            }
        }
    }
    
    public void sortPostingList(){
        File sort_done_f =new File(indexDirectory+"/sort_done.dat");
        if(!sort_done_f.exists())
            sort_done_f.delete();
        System.err.println("Sorting posting lists, depending on their size and number, it might take a while...");
        try{
            String newDir=indexDirectory+"/new";
            String tempDir=indexDirectory+"/temp";
            String invFileDir=indexDirectory+"/invfile";
            //first let's move the inverted file somewehere else
            moveInvertedFileInTemp(invFileDir,tempDir);
            //let's process the new posting lists
            for(int entry=0;entry<writeInvFile.length;entry++){
                if(((double)(((double)entry*100)/(double)writeInvFile.length)%10)==0)
                    System.err.println(((entry*100)/writeInvFile.length)+"% posting lists processed...");

                File pl_f=new File(newDir+"/pl_e"+entry+".dat");
                File ospl_f=new File(tempDir+"/pl_e"+entry+"_sorted.dat");
                File spl_f=new File(invFileDir+"/pl_e"+entry+"_sorted.dat");

                if(!pl_f.exists()){           //if there is not unordered posting list we just move the old posting list back
                    ospl_f.renameTo(spl_f);
                }
                else  {
                    RandomAccessFile pl_ra=new RandomAccessFile(pl_f,"r");
                    RandomAccessFile ospl_ra=null;

                    int[] scoreCounter=new int[writeInvFile.length];            //array maintaining for each possible score the number of objects having that score
                    int[] objectsWithScore=new int[writeInvFile.length];          //array indicating for each possible score what is the current number of objects inserted in the ordered posting list being created
                    int sizeOfPostingListIndex=(scoreCounter.length+1)*4;       //size of the persistent array used as an index in the posting list to access objects having a certain score
                    //We first count how many objects have a certain score in the old posting lists and we also get the size of the old posting list
                    long sizeOfOldPostingList=0;
                    if(ospl_f.exists()){
                        ospl_ra=new RandomAccessFile(ospl_f,"rw");
                        sizeOfOldPostingList=countObjectsWithScoreInOldPostingLists(ospl_ra,scoreCounter, objectsWithScore, sizeOfPostingListIndex);
                    }
                    //We add the number of new objects that have a certain score in the new unordered posting list
                    MappedByteBuffer pl=pl_ra.getChannel().map(java.nio.channels.FileChannel.MapMode.READ_ONLY,0,pl_ra.length());
                    countObjectsWithScoreInUnsortedPostingLists(pl, scoreCounter);
                    //let's create a new ordered posting list  
                    RandomAccessFile spl_ra=new RandomAccessFile(spl_f,"rw");   //We create a new ordered posting list
                    MappedByteBuffer spl=spl_ra.getChannel().map(java.nio.channels.FileChannel.MapMode.READ_WRITE,0,sizeOfOldPostingList+pl_ra.length()/2+sizeOfPostingListIndex);  //given that we do not store the score, //The length of the new posting list should be the old ordered one + half of the unordered one
                    //if an ordered posting list already exists its content should be copied in the new created odered posting list
                    if(ospl_f.exists()){
                        copyOldIntoNewPostingList(ospl_ra, spl, scoreCounter, objectsWithScore, sizeOfPostingListIndex);
                    }
                    //Now we can re-read the unordered posting list and copy the element in the proper sector of the ordered posting list
                    copyUnsortedIntoNewPostingList(pl, spl, scoreCounter, objectsWithScore, sizeOfPostingListIndex);
                    //We update the index of the ordered posting list
                    updatePostingListIndex(spl,scoreCounter, sizeOfPostingListIndex);
                    //close the new sorted posting list
                    closeOpenFiles(pl,  spl,  pl_ra,  spl_ra);
                }
            }
            //deleting old and temporary files
            cleanUpOldAndTemporaryfiles( newDir,  tempDir);
            
            sort_done_f.createNewFile();
        }catch(Exception e){
                System.out.println("Cannot sort posting list");
                e.printStackTrace();
            }
        System.err.println("Sorting posting lists finished!");
    }
    
}
