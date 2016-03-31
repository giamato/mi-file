/*
 * PostingListIterator.java
 *
 * Created on 19 aprile 2007, 17.12
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package it.cnr.isti.InvertedFile;
import java.util.*;
import java.io.*;
import java.nio.*;

/**
 *
 * @author amato
 */
public class PostingListIterator implements Iterator
{
    static final private int elementSize=1*4; //We store just the object identifier, not the score

    private final PostingListEntrySet entries;
    private long fromPos=-1;
    private long toPos=-1;
    private long currentPos;
    private int currentScore;
    static private long numOfReads=0;

    //private boolean inUse=false;
   
    
    
    /** Creates a new instance of PostingListIterator */
    public PostingListIterator(PostingListEntrySet entries){
        this.entries=entries;
        if(entries.getPostingList()!=null){
            currentScore=Math.max(entries.getFromScore(),0);
            int bufferOffset=entries.getBufferOffset();
            try{
                currentPos=fromPos=entries.getPostingListIndex()[Math.max(entries.getFromScore(),0)]-bufferOffset;
                //currentPos=fromPos=0;
            }catch(Exception e){
                e.printStackTrace();
            }
            toPos=entries.getPostingListIndex()[Math.min(entries.getToScore()+1,entries.getPostingListIndex().length-1)]-bufferOffset;
            //toPos=entries.getPostingList().capacity();
            
            //System.out.println(fromPos+" "+toPos+" "+bufferOffset);
        }
    }

    public int getPostingListSize(){
        return (entries.getPostingListIndex()[entries.getPostingListIndex().length-1]-entries.getPostingListIndex()[0])/elementSize;
    }

    public int getPostingListPortionSize(){
        return (int)(toPos-fromPos)/elementSize;
    }
    
    static public void resetNumOfReads(){
        numOfReads=0;
    }
    
    static public int getElementSize(){
        return elementSize;
    }
    
    static public long getNumOfReads(){
        return numOfReads;
    }
    
    public void remove() throws UnsupportedOperationException{
        throw new UnsupportedOperationException("remove is not supported by InvertedFileIterator");
    }
    
    public synchronized PostingListEntry next() throws NoSuchElementException{
        if(entries.getPostingList()!=null && 
                currentPos >=0 &&
                currentPos < toPos)
        {
            //entries.getPostingList().position((int)currentPos-entries.getPostingListIndex()[0]);
            entries.getPostingList().position((int)currentPos);
            numOfReads++;
            int o=entries.getPostingList().getInt();
            while(currentPos>=entries.getPostingListIndex()[currentScore+1]-entries.getBufferOffset())
                currentScore++;
            int score=currentScore;
            currentPos+=elementSize;
            //System.out.println("Object:"+o+" Score:"+score);
            return new PostingListEntry(o,score);
        }
        throw new NoSuchElementException("InvertedFile not Initialized, non valid entry, or no more elements in this posting list");
    }
    
    public synchronized boolean hasNext(){
        if(entries.getPostingList()!=null && 
                currentPos >=0 &&
                currentPos < toPos)
            return true;
        else
            return false;       
    }
    
}
