/*
 * InvertedFileIterator.java
 *
 * Created on 19 aprile 2007, 17.12
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package it.cnr.isti.SimilaritySearch.MI_File;
import java.util.*;
import java.io.*;
import java.nio.*;

/**
 *
 * @author amato
 */
class InvertedFileIterator implements Iterator
{
    
    
    public class Item
    {
        private int object;
        private int score;
        
        public Item(int o,int s)
        {
            object=o;
            score=s;
        }
        
        public int getObject()
        {
            return object;
        }
        
        public int getScore()
        {
            return score;
        }
        
        public String toString()
        {
            return "object: " + object + " score: " + score;
        }
        
    }
    
    private RandomAccessFile postingListFile=null;
    private MappedByteBuffer postingListMemoryMapped;
    private int postingListIndex[];
    static private int elementSize=1*4; //We store just the object identifier, not the score
    private int entry;
    private int lexiconSize;
    private long fromPos;
    private long toPos;
    private long currentPos;
    private int currentScore;
    static private long numOfReads=0;

    private boolean inUse=false;
   
    
    
    /** Creates a new instance of InvertedFileIterator */
    public void initialisation(int fromScoreParam, int toScoreParam){
        synchronized(this){//one thread at time access one posting list
            if(postingListMemoryMapped!=null){
                if(inUse){
//                    System.out.println("Waiting for posting list "+entry+ "to become available");
                    try{
                        this.wait(10000); //one thread at time access one posting list. However if a tread takes too long we do not starve
                    }catch(Exception e){
                        e.printStackTrace();
                    }
                }
//                System.out.println("Taking a lock on posting list "+entry);
                inUse=true;
                currentScore=Math.max(fromScoreParam,0);
                currentPos=fromPos=postingListIndex[Math.max(fromScoreParam,0)];
                toPos=postingListIndex[Math.min(toScoreParam+1,postingListIndex.length-1)];
                postingListMemoryMapped.position((int)fromPos-postingListIndex[0]);
            }
        }
    }
    
    public InvertedFileIterator(int entryPar,String indexDirectory,int lexiconSizePar, InvertedFile invFile){
        synchronized(this){
            entry=entryPar;
            lexiconSize=lexiconSizePar;
            try{
                File pl_f=new File(indexDirectory+"/invfile/pl_e"+entry+"_sorted.dat");
                if(pl_f.exists())
                {
                    if(postingListFile==null){
//                        System.out.println("Opening posting list "+entry);
                        postingListFile=new RandomAccessFile(pl_f,"r");
                    }
                    if(postingListIndex==null){
//                        System.out.println("Reading index of posting list "+entry);
                        postingListIndex=new int[lexiconSize]; //positions range from 0 up to (including) the number of reference objects 
                        //Can we do the following by using MemoryMappedBuffers rather than random access files?
                        postingListFile.seek(0);
                        for(int s=0;s<postingListIndex.length;s++){
                            byte[] position_buff=new byte[4];
                            postingListFile.read(position_buff);
                            int position=it.cnr.isti.SimilaritySearch.MI_File.Conversions.byteArrayToInt(position_buff);
                            postingListIndex[s]=position;
                        }
                    }                    
    //            System.out.println(fromScoreParam+" "+toScoreParam+" "+fromPos+" "+toPos);
    //                pl=invFile[entry].getChannel().map(java.nio.channels.FileChannel.MapMode.READ_ONLY,fromPos,toPos-fromPos);
                    if(postingListMemoryMapped==null){
                        postingListMemoryMapped=postingListFile.getChannel().map(java.nio.channels.FileChannel.MapMode.READ_ONLY,postingListIndex[0],postingListFile.length()-postingListIndex[0]);
                    }
                }
                else{
                    currentPos=fromPos=toPos=0;
                }
            }catch(Exception e){
                System.err.println("Cannot open posting list");
                e.printStackTrace();    
            }
        }
    }
    
    public void close(){
        try{
            postingListFile.getChannel().close();
            postingListFile.close();
            postingListFile=null;
            postingListMemoryMapped=null;
            postingListIndex=null;
        }catch(Exception e){
            System.err.println("Failed to close a posting list");
            e.printStackTrace();
        }
    }
    
    
/*    public InvertedFileIterator(RandomAccessFile[] invFileParam,int[][] postingListIndexPar,MappedByteBuffer[] plsPar,int entryParam, int fromScoreParam, int toScoreParam, String p_indexDirectory) {
        initialisation(invFileParam,postingListIndexPar,plsPar,entryParam,fromScoreParam,toScoreParam,p_indexDirectory);
    }
    
    public InvertedFileIterator(RandomAccessFile[] invFileParam,int[][] postingListIndexPar,MappedByteBuffer[] plsPar,int entryParam, int fromScoreParam, String p_indexDirectory) {
         initialisation(invFileParam,postingListIndexPar,plsPar,entryParam,fromScoreParam,invFileParam.length,p_indexDirectory);
    }
    
    public InvertedFileIterator(RandomAccessFile[] invFileParam,int[][] postingListIndexPar,MappedByteBuffer[] plsPar,int entryParam, String p_indexDirectory) {
         initialisation(invFileParam,postingListIndexPar,plsPar,entryParam,0,invFileParam.length,p_indexDirectory);
    }*/
    
    static public void resetNumOfReads()
    {
        numOfReads=0;
    }
    
    static public int getElementSize(){
        return elementSize;
    }
    
    static public long getNumOfReads()
    {
        return numOfReads;
    }
    
    public void remove() throws UnsupportedOperationException
    {
        throw new UnsupportedOperationException("remove is not supported by InvertedFileIterator");
    }
    
    public Item next() throws NoSuchElementException
    {
        if(postingListMemoryMapped!=null && 
                entry >=0 && 
                entry < lexiconSize &&
                currentPos >=0 &&
                currentPos < toPos)
        {
            numOfReads++;
            int o=postingListMemoryMapped.getInt();
            while(currentPos>=postingListIndex[currentScore+1])
                currentScore++;
            int score=currentScore;
            currentPos+=elementSize;
//            System.out.println("Object:"+o+" Score:"+score);
            return new Item(o,score);
        }
        synchronized(this){
//            System.out.println("Releasing lock on posting list "+entry);
            inUse=false;
            this.notify(); //release exclusive access to the posting list
        }
        throw new NoSuchElementException("InvertedFile not Initialized, non valid entry, or no more elements in this posting list");
    }
    
    public boolean hasNext()
    {
        if(postingListMemoryMapped!=null && 
                entry >=0 && 
                entry < lexiconSize &&
                currentPos >=0 &&
                currentPos < toPos)
            return true;
        else{
            synchronized(this){
//                System.out.println("Releasing lock on posting list "+entry);
                inUse=false;
                this.notify(); //release exclusive access to the posting list
            }
            return false;
        }
         
    }
    
}
