/*
 * TempRes.java
 *
 * Created on 12 dicembre 2008, 9.59
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package it.cnr.isti.SimilaritySearch.MI_File;

import java.util.*;

class TempRes
    {
        
        private class DoubleWrapperComparator implements Comparator{
            public int compare(Object o1, Object o2){
                double d1= ((doubleWrapper)o1).value;
                double d2= ((doubleWrapper)o2).value;
                if (d1==d2)
                    return 0;
                if(d1>d2)
                    return 1;
                else
                    return -1;
            }
        }
    
        private class doubleWrapper{
            double value=0;
            
        @Override
            public boolean equals(Object obj) {
                return ((doubleWrapper)obj).value==value;
            }
        }

        private class intWrapper{
            int value=0;

        @Override
            public int hashCode() {
                return value;
            }

            
        @Override
            public boolean equals(Object obj) {
                return ((intWrapper)obj).value==value;
            }
        
        

        }
        
        static final private ArrayList<Hashtable<intWrapper,doubleWrapper>> tempResPool=new ArrayList<Hashtable<intWrapper,doubleWrapper>>();
        private Hashtable<intWrapper,doubleWrapper> tempRes=null;
        private TreeMap<doubleWrapper,intWrapper> orderedRes=null;
        private Hashtable<intWrapper,doubleWrapper> objectsOfOrderedRes=null;
        private double kDist=Double.MAX_VALUE;
        private int maxEntries;
        private int k;

        
        public TempRes(int numEntriespar, int k_p)
        {
            synchronized(tempResPool){
                if(tempResPool.isEmpty()){
//                    System.err.println("Creating a new temp result.");
                    tempRes=new Hashtable<intWrapper,doubleWrapper>(); //here we maintain the total list of documents found
                }
                else{
                    tempRes=tempResPool.remove(0);
 //                   System.err.println("Recycled an old temp result. Pool size: "+tempResPool.size());
                }
            }
            orderedRes=new TreeMap<doubleWrapper,intWrapper>(new DoubleWrapperComparator());       //here we maintain the k current best using the distance as entry
            objectsOfOrderedRes=new Hashtable<intWrapper,doubleWrapper>(); //here we maintain the k current best using the oid as entry
            maxEntries=numEntriespar;
            k=k_p;
        }
        
        public void recycle() {
            tempRes.clear();
            synchronized(tempResPool){
                tempResPool.add(tempRes);
            }
            tempRes=null;
 //           System.err.println("Recycling used tempresult. Pool size: "+tempResPool.size());
        }
        

        public int size(){
            return tempRes.size();
        }
        

/*        //if object o is already one of the k current best, we eliminate it
        private void eliminateDuplicates(TreeMap<Double,Integer> orderedRes, Hashtable<Integer,Double> objectsOfOrderedRes, int o){
            if(objectsOfOrderedRes.containsKey(o)){
                Double d=objectsOfOrderedRes.remove(o);
                o=orderedRes.remove(d);
            }
        }
        
        int orderedInsertions;
        private void orderedInsert(int oid, double dist){
            double rdist=0;
            if(orderedRes.size()<k){
                Integer o=Integer.valueOf(oid);
                eliminateDuplicates(orderedRes,objectsOfOrderedRes,oid);  // if it is already there we remove it
                while(o!=null)
                {
                    orderedInsertions++;
                    dist+=rdist;
                    Double odist=Double.valueOf(dist);
                    objectsOfOrderedRes.put(o,odist);
                    o=orderedRes.put(odist,o);           //there could be another object with the same distance: we have to reinsrt it
                    rdist+=0.0001*Math.random();
                }
                kDist=orderedRes.lastKey();
            }
            else if(dist<kDist)
            {
                Integer o=Integer.valueOf(oid);
                eliminateDuplicates(orderedRes,objectsOfOrderedRes,oid); // if it is already there we remove it
                while(o!=null)
                {
                    orderedInsertions++;
                    dist+=rdist;
                    Double odist=Double.valueOf(dist);
                    objectsOfOrderedRes.put(o,odist);
                    o=orderedRes.put(odist,o);        //there could be another object with the same distance: we have to reinsrt it
                    rdist+=0.0001*Math.random();
                }
                if(orderedRes.size()>k){
                    Integer to=orderedRes.remove(orderedRes.lastKey());  
                    double d1= objectsOfOrderedRes.remove(to);
                }
                kDist=orderedRes.lastKey();
            }
        } /**/
        
        //if object o is already one of the k current best, we eliminate it
        private intWrapper eliminateDuplicateDistances(TreeMap<doubleWrapper,intWrapper> orderedRes, doubleWrapper d){
           intWrapper o =orderedRes.remove(d);
           return o;
        }
        
        private doubleWrapper eliminateDuplicateObjects(Hashtable<intWrapper,doubleWrapper> objectsOfOrderedRes, intWrapper o){
            doubleWrapper d=objectsOfOrderedRes.remove(o);
            return d;
        }
        
        int orderedInsertions;
        private void orderedInsert(int oid, double dist){
            double rdist=0;
            if(orderedRes.size()<=k){
                intWrapper o=new intWrapper();
                o.value=oid;
                doubleWrapper d=eliminateDuplicateObjects(objectsOfOrderedRes,o);  // if it is already there we remove it
                if (d!=null)
                    eliminateDuplicateDistances(orderedRes,d);
                else
                    d=new doubleWrapper();
                while(o!=null)
                {
                    orderedInsertions++;
                    dist+=rdist;
                    d.value=dist;
                    objectsOfOrderedRes.put(o,d);
                    o=orderedRes.put(d,o);           //there could be another object with the same distance: we have to reinsrt it
                    rdist+=0.0001*Math.random();
                    if(o!=null)
                        d=new doubleWrapper();
                }
                kDist=orderedRes.lastKey().value;
            }
            else if(dist<kDist)
            {
                intWrapper o=new intWrapper();
                o.value=oid;
                doubleWrapper d=eliminateDuplicateObjects(objectsOfOrderedRes,o);  // if it is already there we remove it
                if (d!=null)
                    eliminateDuplicateDistances(orderedRes,d);
                if(orderedRes.size()>=k-1){
                    intWrapper to=orderedRes.remove(orderedRes.lastKey());  
                    d= objectsOfOrderedRes.remove(to);
                }
                while(o!=null)
                {
                    orderedInsertions++;
                    dist+=rdist;
                    d.value=dist;
                    objectsOfOrderedRes.put(o,d);
                    o=orderedRes.put(d,o);        //there could be another object with the same distance: we have to reinsrt it
                    rdist+=0.0001*Math.random();
                    if(o!=null)
                        d=new doubleWrapper();
                }
                kDist=orderedRes.lastKey().value;
            }
        }
        
        private long putTime=0;
        
        public long getPutTime(){
            return putTime;
        }
        
        int savedInsertions;
        int updates;
        int insertions;
        public void put(int o, int inc, int postingListToBeAccessed, int accessedPostingLists)
        {
            intWrapper oo=new intWrapper();
            oo.value=o;
            double currentScore;
            long time=System.currentTimeMillis();
            doubleWrapper oi=tempRes.get(oo);
            if (oi!=null){
                updates++;
                currentScore=oi.value-maxEntries+inc;
                oi.value=currentScore;
            }
            else
            {
                currentScore=maxEntries*(postingListToBeAccessed-1)+inc;
            }
            double minDist=currentScore-maxEntries*(postingListToBeAccessed-accessedPostingLists); // the minimum distance this object can reach
            if(minDist>kDist){
                savedInsertions++;      //the object cannot enter the best k objects we save one insertion
                tempRes.remove(oo);
            }
            else{                       //the object can enter the best k. Let's insert it
                if (oi==null){
                    insertions++;
                    oi=new doubleWrapper();
                    oi.value=currentScore;
                    tempRes.put(oo,oi);              //insert in the full result list
                }
                orderedInsert(o,currentScore);  //insert in the best current k
            }
            putTime+=System.currentTimeMillis()-time;
        }
        
        /*        public void put(Integer o, Integer inc)
        {
            long time=System.currentTimeMillis();
            ObjInfo oi=tempRes.get(o);
            if (oi!=null){
                oi.setCurrentScore(oi.getCurrentScore()-maxEntries+inc);
            }
            else
            {
                oi = new ObjInfo();
                oi.setCurrentScore(maxEntries*(skp-1)+inc);
                oi.setConsideredEntries(0);
            }
            oi.setConsideredEntries(oi.getConsideredEntries()+1);
            tempRes.put(o,oi);              //insert in the full result list
            orderedInsert(o,oi.getCurrentScore());  //insert in the best current k
            time_counter=time_counter+System.currentTimeMillis()-time;
        }*/
        
        
        public Iterator iterator()
        {
            return tempRes.entrySet().iterator();
        }
        
        public TreeMap getOrderedRes(){
//            System.err.println("Insertions: "+insertions+" Updates: " +updates+" Ordered insertions: "+orderedInsertions+" Saved insertions: "+savedInsertions+" ");
         TreeMap res=new TreeMap();
         Iterator temp_iter=orderedRes.entrySet().iterator();
         while(temp_iter.hasNext())
         {
             Map.Entry e =(Map.Entry)temp_iter.next();
             res.put(((doubleWrapper)e.getKey()).value,((intWrapper)e.getValue()).value);      
         }
            return res;
        }
        
        public int numberOfObjectsFound(){
            return tempRes.size();
        }
        
    }
