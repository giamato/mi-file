/*
 * InvertedFile.java
 *
 * Created on 19 aprile 2007, 21.19
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package it.cnr.isti.InvertedFile;



import java.io.*;
import java.nio.*;
import java.util.Set;

/**
 *
 * @author amato
 */
public class InvertedFile {
    final private String indexDirectoryName;
    final private ByteBuffer[] postingLists;
    final private int[][] postingListIndexes;
    final private int lexiconSize;
    final private int maxScore;
    final private BufferedOutputStream[] writeInvFile;
    final private int bufferSize=4096*2;
    
    
    /** Creates a new instance of InvertedFile */
    public InvertedFile(int lexiconSizePar, int maxScorePar, String p_indexDirectory) {
        lexiconSize=lexiconSizePar;
        maxScore=maxScorePar;
        indexDirectoryName=p_indexDirectory;
        postingLists =new ByteBuffer[lexiconSize];
        postingListIndexes =new int[lexiconSize][];
        writeInvFile=new BufferedOutputStream[lexiconSize];
        File indexDirectory=new File(indexDirectoryName);
        if(!indexDirectory.exists())
            indexDirectory.mkdirs();
    }
    
    public synchronized void insert(int obj, int postingListId, int score){
        try{
            if(score<0||score>maxScore)
                throw new Exception("Score associated to the entry being inserted must be in the interval "+0+".."+maxScore);
            if(postingListId >= lexiconSize)
                throw new Exception(" the posting list identifier mujst be smaller than "+lexiconSize);
            if (writeInvFile[postingListId]==null){
                String newDirectory=indexDirectoryName+"/new";
                File pl_f=new File(newDirectory+"/pl_e"+postingListId+".dat");
                if(!pl_f.exists()){
                    File indexDirectory_f=new File(indexDirectoryName);
                    if(!indexDirectory_f.exists())
                        indexDirectory_f.mkdirs();
                    File newDirectory_f=new File(newDirectory);
                    if(!newDirectory_f.exists())
                        newDirectory_f.mkdirs();
                    pl_f.createNewFile();
                }
                writeInvFile[postingListId]=new BufferedOutputStream(new FileOutputStream(pl_f,true),bufferSize);
            }            
            byte[] obj_buff=Conversions.intToByteArray(obj);
            byte[] score_buff=Conversions.intToByteArray(score);
            writeInvFile[postingListId].write(obj_buff);
            writeInvFile[postingListId].write(score_buff);
//            pl_w.close();
        }catch(Exception e)
        {        
            System.err.println("Cannot create or insert entry in posting list for entry "+postingListId+" and score "+score);
            e.printStackTrace();
        }
    }

    public synchronized void releasePostingList(int postingListId){
        synchronized(postingLists){
            if (postingLists[postingListId]!=null){
                postingLists[postingListId]=null;
            }
        }
    }

    public synchronized Set<PostingListEntry> getPostingList(int postingListId)
    {
        return new PostingListEntrySet(postingListId,this);
    }

    public synchronized Set<PostingListEntry> getPostingList(int postingListId,int fromScore)
    {
        return new PostingListEntrySet(postingListId,this, fromScore);
    }

    public synchronized Set<PostingListEntry> getPostingList(int postingListId,int fromScore, int toScore)
    {
        return new PostingListEntrySet(postingListId,this, fromScore, toScore);

    }

    public synchronized void flush() {
        System.err.println("Flushing inverted file...");
        for(int postingListId=0;postingListId<writeInvFile.length;postingListId++)
            if(writeInvFile[postingListId]!=null)
                try{
                    writeInvFile[postingListId].flush();
                    writeInvFile[postingListId].close();
                    writeInvFile[postingListId]=null;
                }catch(Exception e){
                    System.err.println("Cannot flush inverted file.");
                    e.printStackTrace();
                }
        System.err.println("Finished flushing inverted file!");
    }
    
    protected void finalize() throws Throwable{
        close();
    }
    
    
    public synchronized void clean(){
    for(int postingListId=0;postingListId<lexiconSize;postingListId++)
        if(postingLists[postingListId]!=null){
            postingLists[postingListId]=null;
        }
    }


    public void close(){
        flush(); // if something goes wrong, the new posting lists might not be stored on disk. Let's flush them!
        clean();
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
        //for(int i=0;i<writeInvFile.length;i++){
        for(int i=0;i<scoreCounter.length;i++){
            //int pos=ospl_ra.readInt();//old
            int pos=ospl.getInt();
            scoreCounter[i]=(pos-lastPos)/PostingListIterator.getElementSize(); //Score counter should be initialized with the number of postingListId present in the already existing posting list
            objectsWithScore[i]=scoreCounter[i]; //at the beginnng we will insert the objects in the old posting lists so the already inserted objects is set to that number
            lastPos=pos;
        }
        return sizeOfOldPostingList;
    }

    private void countObjectsWithScoreInUnsortedPostingLists(MappedByteBuffer pl, int[] scoreCounter) throws Exception{
        while(pl.hasRemaining()){
            int o=pl.getInt();//we skip the object
            int score=pl.getInt();
            scoreCounter[score]++;
        }
    }

    private void copyOldIntoNewPostingList(RandomAccessFile ospl_ra, MappedByteBuffer spl, int[] scoreCounter, int[] objectsWithScore, int sizeOfPostingListIndex) throws Exception{
        int insertedObjects=0;
        int writePosition=sizeOfPostingListIndex;
        //for(int i=0;i<writeInvFile.length;i++){
        for(int i=0;i<objectsWithScore.length;i++){
            int startPosition=sizeOfPostingListIndex+insertedObjects*PostingListIterator.getElementSize();
            int buffSize=objectsWithScore[i]*PostingListIterator.getElementSize();
            insertedObjects+=objectsWithScore[i];
            MappedByteBuffer ospl=ospl_ra.getChannel().map(java.nio.channels.FileChannel.MapMode.READ_ONLY,startPosition,buffSize);
            spl.position(writePosition);
            spl.put(ospl);
            writePosition+=scoreCounter[i]*PostingListIterator.getElementSize();
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
            int write_position_in_bytes=sizeOfPostingListIndex+write_position*PostingListIterator.getElementSize();
            spl.position(write_position_in_bytes);
            spl.putInt(o);
            objectsWithScore[score]++;
        }
    }

    private void updatePostingListIndex(MappedByteBuffer spl,int[] scoreCounter, int sizeOfPostingListIndex){
        int score_position=0;
        int s=0;
        spl.position(s);
        int score_position_in_bytes=sizeOfPostingListIndex+score_position*PostingListIterator.getElementSize();
        spl.putInt(score_position_in_bytes);
        for(s=0;s<scoreCounter.length;s++){
            spl.position((s+1)*4);
            score_position+=scoreCounter[s];
            score_position_in_bytes=sizeOfPostingListIndex+score_position*PostingListIterator.getElementSize();
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
        for(int postingListId=0;postingListId<writeInvFile.length;postingListId++){
            File pl_f=new File(newDir+"/pl_e"+postingListId+".dat");
            File ospl_f=new File(tempDir+"/pl_e"+postingListId+"_sorted.dat");
            //delete the unordered posting list
            if(pl_f.exists()){
                boolean deleted=false;
                for(int i=0;i<100000&&!deleted;i++)
                    deleted=pl_f.delete();
                if(!deleted)
                     System.err.println("Warining failed to delete unordered posting list");
            }
            //delete the old renamed ordered posting list
            if(ospl_f.exists()){
                boolean deleted=false;
                for(int i=0;i<100000&&!deleted;i++){
                    deleted=ospl_f.delete();
                }
                if(!deleted)
                         System.err.println("Warining failed to delete old ordered posting list");
            }
        }
    }
    
    private void cleanUpUnsortedAndOldPostingLists(String unsortedDirName, String oldfilesDirName, String tempDirName) throws Exception{
        System.err.println("cleaning up");
        System.gc();
        writeLock();  //we needit just here
        File oldfilesDir=new File(oldfilesDirName);
        String deleteMeDirName=indexDirectoryName+"/delete-me";
        File deleteMeDir=new File(deleteMeDirName);
        if(oldfilesDir.exists()){
            boolean renamed=false;
            for(int i=0;i<100000&&!renamed;i++)
                renamed=oldfilesDir.renameTo(deleteMeDir);
            if(!renamed)
                 System.err.println("Warning: failed to rename old inverted file directory into delete-me directory");
        }
        File tempDir=new File(tempDirName);
        if(tempDir.exists()){
            boolean renamed=false;
            for(int i=0;i<100000&&!renamed;i++){
                renamed=tempDir.renameTo(oldfilesDir);
            }
            if(!renamed)
                     System.err.println("Warining failed to rename temporary inverted file directory");
        }
        writeUnlock();
        for(int postingListId=0;postingListId<writeInvFile.length;postingListId++){
            File pl_f=new File(unsortedDirName+"/pl_e"+postingListId+".dat");
            File ospl_f=new File(deleteMeDirName+"/pl_e"+postingListId+"_sorted.dat");
            //delete the unsortedDirName posting list
            if(pl_f.exists()){
                boolean deleted=false;
                for(int i=0;i<100000&&!deleted;i++)
                    deleted=pl_f.delete();
                if(!deleted)
                     System.err.println("Warning: failed to delete unsorted posting list: " +postingListId);
            }
            //delete the old renamed ordered posting list
            if(ospl_f.exists()){
                boolean deleted=false;
                for(int i=0;i<100000&&!deleted;i++){
                    deleted=ospl_f.delete();
                }
                if(!deleted)
                         System.err.println("Warining failed to delete old sorted posting list: " +postingListId);
            }
        }
        File unsortedDir=new File(unsortedDirName);
        if(unsortedDir.exists()){
            boolean deleted=false;
            for(int i=0;i<100000&&!deleted;i++)
                deleted=unsortedDir.delete();
            if(!deleted)
                 System.err.println("Warning: failed to delete unsorted inverted file directory");
        }
        if(deleteMeDir.exists()){
            boolean deleted=false;
            for(int i=0;i<100000&&!deleted;i++)
                deleted=deleteMeDir.delete();
            if(!deleted)
                 System.err.println("Warning: failed to delete old inverted file directory");
        }
    }
    
    private void writeLock() throws Exception{
        File writeLock =new File(indexDirectoryName+"/write-lock.dat");
        boolean stop=true;
        while(stop){
            if(writeLock.exists())
                Thread.sleep(1000);
            else{
                writeLock.createNewFile();
                stop=false;
            }
        }
    }
    
    private void writeUnlock() throws Exception{
        File writeLock =new File(indexDirectoryName+"/write-lock.dat");
        if(writeLock.exists())
            writeLock.delete();
    }
    
    public void checkWriteLock() throws Exception{
        File writeLock =new File(indexDirectoryName+"/write-lock.dat");
        boolean stop=true;
        while(stop){
            if(writeLock.exists()){
                System.out.println("Waiting......");
                clean();
                Thread.sleep(1000);
            }
            else{
                stop=false;
            }
        }
    }
    
    public void sortPostingList(){
        System.err.println("Sorting posting lists, depending on their size and number, it might take a while...");
        try{   
            flush();
            String newDirName=indexDirectoryName+"/new";
            String tempDirName=indexDirectoryName+"/temp";
            String invFileDirName=indexDirectoryName+"/invfile";
            //let's process the new posting lists
            
            File tempDir=new File(tempDirName);
            if(!tempDir.exists())
                tempDir.mkdirs();
            
            for(int postingListId=0;postingListId<writeInvFile.length;postingListId++){
                if(((double)(((double)postingListId*100)/(double)writeInvFile.length)%10)==0)
                    System.err.println(((postingListId*100)/writeInvFile.length)+"% posting lists processed...");

                File pl_f=new File(newDirName+"/pl_e"+postingListId+".dat");
                File ospl_f=new File(invFileDirName+"/pl_e"+postingListId+"_sorted.dat");
                File spl_f=new File(tempDirName+"/pl_e"+postingListId+"_sorted.dat");

                if(!pl_f.exists()){           //if there is not unsorted posting list we just move the old posting list back
                    ospl_f.renameTo(spl_f);
                }
                else  {
                    RandomAccessFile pl_ra=new RandomAccessFile(pl_f,"r");
                    RandomAccessFile ospl_ra=null;

                    int[] scoreCounter=new int[maxScore+1];            //array maintaining for each possible score the number of objects having that score
                    int[] objectsWithScore=new int[maxScore+1];          //array indicating for each possible score what is the current number of objects inserted in the ordered posting list being created
                    int sizeOfPostingListIndex=(maxScore+2)*4;       //size of the persistent array used as an index in the posting list to access objects having a certain score
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
            cleanUpUnsortedAndOldPostingLists(newDirName, invFileDirName,tempDirName);
            
        }catch(Exception e){
                System.out.println("Cannot sort posting list");
                e.printStackTrace();
            }
        System.err.println("Sorting posting lists finished!");
    }

    public void sortPostingList_old(){
        File sort_done_f =new File(indexDirectoryName+"/sort_done.dat");
        if(sort_done_f.exists())
            sort_done_f.delete();
        System.err.println("Sorting posting lists, depending on their size and number, it might take a while...");
        try{
            String newDir=indexDirectoryName+"/new";
            String tempDir=indexDirectoryName+"/temp";
            String invFileDir=indexDirectoryName+"/invfile";
            //first let's move the inverted file somewehere else
            moveInvertedFileInTemp(invFileDir,tempDir);
            //let's process the new posting lists
            for(int postingListId=0;postingListId<writeInvFile.length;postingListId++){
                if(((double)(((double)postingListId*100)/(double)writeInvFile.length)%10)==0)
                    System.err.println(((postingListId*100)/writeInvFile.length)+"% posting lists processed...");

                File pl_f=new File(newDir+"/pl_e"+postingListId+".dat");
                File ospl_f=new File(tempDir+"/pl_e"+postingListId+"_sorted.dat");
                File spl_f=new File(invFileDir+"/pl_e"+postingListId+"_sorted.dat");

                if(!pl_f.exists()){           //if there is not unordered posting list we just move the old posting list back
                    ospl_f.renameTo(spl_f);
                }
                else  {
                    RandomAccessFile pl_ra=new RandomAccessFile(pl_f,"r");
                    RandomAccessFile ospl_ra=null;

                    int[] scoreCounter=new int[maxScore+1];            //array maintaining for each possible score the number of objects having that score
                    int[] objectsWithScore=new int[maxScore+1];          //array indicating for each possible score what is the current number of objects inserted in the ordered posting list being created
                    int sizeOfPostingListIndex=(maxScore+2)*4;       //size of the persistent array used as an index in the posting list to access objects having a certain score
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

    public String getIndexDirectoryName() {
        return indexDirectoryName;
    }

    public ByteBuffer[] getPostingLists() {
        return postingLists;
    }

    public int[][] getPostingListIndexes() {
        return postingListIndexes;
    }

    public int getLexiconSize() {
        return lexiconSize;
    }

    public int getMaxScore() {
        return maxScore;
    }

    public BufferedOutputStream[] getWriteInvFile() {
        return writeInvFile;
    }
    
    
    
}
