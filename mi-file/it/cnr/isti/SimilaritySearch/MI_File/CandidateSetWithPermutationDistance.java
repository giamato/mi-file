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

class CandidateSetWithPermutationDistance extends CandidateSet{

       private int maxEntries;  // it is equal to k_i
        
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
        
        private TreeMap<doubleWrapper,intWrapper> orderedRes=null;
        private TIntObjectHashMap<doubleWrapper> objectsOfOrderedRes=null;
        private double kDist=Double.MAX_VALUE;

        
        public CandidateSetWithPermutationDistance(int maxEntries, int k){
            super(k);
            this.maxEntries=maxEntries;
            orderedRes=new TreeMap<doubleWrapper,intWrapper>(new DoubleWrapperComparator());       //here we maintain the k current best using the distance as entry
            objectsOfOrderedRes=new TIntObjectHashMap<doubleWrapper>(); //here we maintain the k current best using the oid as entry
        }
        
        //if object o is already one of the k current best, we eliminate it
        private intWrapper eliminateDuplicateDistances(TreeMap<doubleWrapper,intWrapper> orderedRes, doubleWrapper d){
           intWrapper o =orderedRes.remove(d);
           return o;
        }
        
        private doubleWrapper eliminateDuplicateObjects(TIntObjectHashMap<doubleWrapper> objectsOfOrderedRes, int o){
            doubleWrapper d=objectsOfOrderedRes.remove(o);
            return d;
        }
        
        int orderedInsertions;
        private void orderedInsert(int oid, double dist){
            double rdist=0;
            if(orderedRes.size()<=getK()){
                intWrapper o=new intWrapper();
                o.value=oid;
                doubleWrapper d=eliminateDuplicateObjects(objectsOfOrderedRes,oid);  // if it is already there we remove it
                if (d!=null)
                    eliminateDuplicateDistances(orderedRes,d);
                else
                    d=new doubleWrapper();
                while(o!=null)
                {
                    orderedInsertions++;
                    dist+=rdist;
                    d.value=dist;
                    objectsOfOrderedRes.put(o.value,d);
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
                doubleWrapper d=eliminateDuplicateObjects(objectsOfOrderedRes,oid);  // if it is already there we remove it
                if (d!=null)
                    eliminateDuplicateDistances(orderedRes,d);
                else
                    d=new doubleWrapper();
                if(orderedRes.size()>=getK()-1){
                    intWrapper to=orderedRes.remove(orderedRes.lastKey());  
                    objectsOfOrderedRes.remove(to.value);
                }
                while(o!=null)
                {
                    orderedInsertions++;
                    dist+=rdist;
                    d.value=dist;
                    objectsOfOrderedRes.put(o.value,d);
                    o=orderedRes.put(d,o);        //there could be another object with the same distance: we have to reinsrt it
                    rdist+=0.0001*Math.random();
                    if(o!=null)
                        d=new doubleWrapper();
                }
                kDist=orderedRes.lastKey().value;
            }
        }
        
        int savedInsertions;
        int updates;
        int insertions;
        public void put(int o, int score, int position, int pivot, int postingListToBeAccessed, int accessedPostingLists)
        {
            int increment=Math.abs(position-score);//the "sperman footrule" distance is the sum of the position differences
            //int increment=0;
            //increment=(int)(ki-(ki-increment)*pl_idf);  //tests using idf
            //int increment=(position-s)*(position-score);    //the "sperman rho" distance is the sum of the position differences power 2
            double currentScore;
            long time=System.currentTimeMillis();
            doubleWrapper i=(doubleWrapper)getTempRes().get(o);
            if (i!=null){
                updates++;
                currentScore=i.value-maxEntries+increment;
                i.value=currentScore;
            }
            else
            {
                currentScore=maxEntries*(postingListToBeAccessed-1)+increment;
            }
            double minDist=currentScore-maxEntries*(postingListToBeAccessed-accessedPostingLists); // the minimum distance this object can reach
            if(minDist>kDist){
                savedInsertions++;      //the object cannot enter the best k objects we save one insertion
                getTempRes().remove(o);
            }
            else{                       //the object can enter the best k. Let's insert it
                if (i==null){
                    insertions++;
                    i=new doubleWrapper();
                    i.value=currentScore;
                    getTempRes().put(o,i);              //insert in the full result list
                }
                orderedInsert(o,currentScore);  //insert in the best current k
            }
            setPutTime(getPutTime()+System.currentTimeMillis()-time);
        }
        
        
        
        public TreeMap getSortedCandidateSet(){
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
        
    }
