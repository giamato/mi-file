/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package it.cnr.isti.InvertedFile;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.Iterator;

/**
 *
 * @author Amato
 */
public class PostingListEntrySet extends java.util.AbstractSet<PostingListEntry>{

    final InvertedFile invFile;
    final int fromScore, toScore;
    final private ByteBuffer postingList;
    private int bufferOffset;
    final private int postingListIndex[];


    public PostingListEntrySet(int postingListId, InvertedFile invFile){       
        this.invFile=invFile;
        this.fromScore=0;
        this.toScore=invFile.getMaxScore();
        loadPostingList(postingListId);
        postingList=invFile.getPostingLists()[postingListId];
        postingListIndex=invFile.getPostingListIndexes()[postingListId];
    }

    public PostingListEntrySet(int postingListId, InvertedFile invFile, int fromScore){
        this.invFile=invFile;
        this.fromScore=0;
        this.toScore=invFile.getMaxScore();
        loadPostingList(postingListId);
        postingList=invFile.getPostingLists()[postingListId];
        postingListIndex=invFile.getPostingListIndexes()[postingListId];


    }

    public PostingListEntrySet(int postingListId, InvertedFile invFile, int fromScore, int toScore){
        this.invFile=invFile;
        this.fromScore=fromScore;
        this.toScore=toScore;
        loadPostingList(postingListId);
        postingList=invFile.getPostingLists()[postingListId];
        postingListIndex=invFile.getPostingListIndexes()[postingListId];  
    }

    public InvertedFile getInvFile() {
        return invFile;
    }

    public int getFromScore() {
        return fromScore;
    }

    public int getToScore() {
        return toScore;
    }

    public ByteBuffer getPostingList() {
        return postingList;
    }

    public int[] getPostingListIndex() {
        return postingListIndex;
    }

    public int getBufferOffset() {
        return bufferOffset;
    }
    
    
    
    private void loadPostingList(int postingListId){
        try{
            invFile.clean();
            invFile.checkWriteLock();
            
            File pl_f=new File(invFile.getIndexDirectoryName()+"/invfile/pl_e"+postingListId+"_sorted.dat");
            RandomAccessFile postingListFile=null;
            if(invFile.getPostingListIndexes()[postingListId]==null){
                if(pl_f.exists()){
                    postingListFile=new RandomAccessFile(pl_f,"r");
                    invFile.getPostingListIndexes()[postingListId]=new int[invFile.getMaxScore()+2]; //positions range from 0 up to maxScore+1 included
                    //Can we do the following by using MemoryMappedBuffers rather than random access files?
                    postingListFile.seek(0);
                    for(int s=0;s<invFile.getPostingListIndexes()[postingListId].length;s++){
                        byte[] position_buff=new byte[4];
                        postingListFile.read(position_buff);
                        int position=Conversions.byteArrayToInt(position_buff);
                        invFile.getPostingListIndexes()[postingListId][s]=position;
                    }
                }
            }                    
            if(invFile.getPostingLists()[postingListId]==null){
                if(postingListFile==null){
                    if(pl_f.exists()){
                        postingListFile=new RandomAccessFile(pl_f,"r");
                    }else
                        return; //cannot map becouse it does not exist
                }
                
                //WITH MEMORY MAPPED FILES
                /*bufferOffset=invFile.getPostingListIndexes()[postingListId][0];
                long size=postingListFile.length()-invFile.getPostingListIndexes()[postingListId][0];
                invFile.getPostingLists()[postingListId]=postingListFile.getChannel().map(java.nio.channels.FileChannel.MapMode.READ_ONLY,bufferOffset,size);/**/
                
                //WITH MANUALLY MANAGED BYTEBUFFERS
                bufferOffset=invFile.getPostingListIndexes()[postingListId][Math.max(fromScore,0)];
                long size=invFile.getPostingListIndexes()[postingListId][Math.min(toScore+1,invFile.getPostingListIndexes()[postingListId].length-1)]-bufferOffset;
                byte[] buffer=new byte[(int)size];
                postingListFile.seek(bufferOffset);
                postingListFile.read(buffer);
                invFile.getPostingLists()[postingListId]=ByteBuffer.wrap(buffer);/**/
            }
            if(postingListFile!=null)
                postingListFile.close();
        }catch(Exception e){
            System.err.println("Cannot open posting list");
            e.printStackTrace();    
        }
    }



    @Override
    public Iterator<PostingListEntry> iterator() {
        PostingListIterator iterator=new PostingListIterator(this);
        return iterator;
    }

    @Override
    public int size() {
        PostingListIterator iterator=new PostingListIterator(this);
        return iterator.getPostingListPortionSize();
    }


}
