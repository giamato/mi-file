/*
 * CandidateSet.java
 *
 * Created on 12 dicembre 2008, 9.59
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package it.cnr.isti.SimilaritySearch.MI_File;

import java.util.*;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.iterator.TIntObjectIterator;

abstract class CandidateSet
    {
        
        static final private ArrayList<TIntObjectHashMap> tempResPool=new ArrayList<TIntObjectHashMap>();
        private TIntObjectHashMap tempRes=null;

        private int k;  // number of objects to be retrieved

        abstract public TreeMap getSortedCandidateSet();
        abstract public void put(int o, int score, int position, int pivot, int postingListToBeAccessed, int accessedPostingLists);

        
        public CandidateSet(int k)
        {
            synchronized(tempResPool){
                if(tempResPool.isEmpty()){
//                    System.err.println("Creating a new temp result.");
                    tempRes=new TIntObjectHashMap(); //here we maintain the total list of documents found
                }
                else{
                    tempRes=tempResPool.remove(0);
 //                   System.err.println("Recycled an old temp result. Pool size: "+tempResPool.size());
                }
            }
            this.k=k;
        }
        
        final public void recycle() {
            getTempRes().clear();
            synchronized(tempResPool){
                tempResPool.add(getTempRes());
            }
            tempRes=null;
 //           System.err.println("Recycling used tempresult. Pool size: "+tempResPool.size());
        }
        

        final public int size(){
            return getTempRes().size();
        }
        
        private long putTime=0;
        
        final public long getPutTime(){
            return putTime;
        }

        final protected void setPutTime(long time){
            putTime=time;
        }
        
        public TIntObjectIterator iterator()
        {
            return getTempRes().iterator();
        }
        
        final public int numberOfObjectsFound(){
            return getTempRes().size();
        }

    /**
     * @return the tempRes
     */
    public TIntObjectHashMap getTempRes() {
        return tempRes;
    }

    /**
     * @return the k
     */
    public int getK() {
        return k;
    }
        
    }
