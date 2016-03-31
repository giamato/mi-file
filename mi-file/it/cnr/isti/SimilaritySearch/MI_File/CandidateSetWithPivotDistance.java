/*
 * CandidateSet.java
 *
 * Created on 12 dicembre 2008, 9.59
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package it.cnr.isti.SimilaritySearch.MI_File;

import it.cnr.isti.Dataset.*;

import java.util.*;

import gnu.trove.map.hash.TIntObjectHashMap;

class CandidateSetWithPivotDistance extends CandidateSet{
        
        private class MaxDistanceWrapperComparator implements Comparator{
            public int compare(Object o1, Object o2){
                double d1= ((DistanceWrapper)o1).maxDistance;
                double d2= ((DistanceWrapper)o2).maxDistance;
                if (d1==d2)
                    return 0;
                if(d1>d2)
                    return 1;
                else
                    return -1;
            }
        }

        private class MinDistanceWrapperComparator implements Comparator{
            public int compare(Object o1, Object o2){
                double d1= ((DistanceWrapper)o1).minDistance;
                double d2= ((DistanceWrapper)o2).minDistance;
                if (d1==d2)
                    return 0;
                if(d1>d2)
                    return 1;
                else
                    return -1;
            }
        }
    
        private class DistanceWrapper{
            double minDistance=0;
            double maxDistance=Double.MAX_VALUE;
            
        @Override
            public boolean equals(Object obj) {
                return ((DistanceWrapper)obj).maxDistance==maxDistance;
            }
        }

        private class ObjWrapper{
            int oid=0;

        @Override
            public int hashCode() {
                return oid;
            }
            
        @Override
            public boolean equals(Object obj) {
                return ((ObjWrapper)obj).oid==oid;
            }
        }
        
        private TreeMap<DistanceWrapper,Integer> sortedBestK=null;
        private TreeMap<DistanceWrapper,Integer> sortedTempRes=null;
        private TIntObjectHashMap<DistanceWrapper> currentBestK=null;
        private double maxDistance=Double.MAX_VALUE;
        private SimilarityDatasetObject q;
        private ReferenceObjects ros;
        private SimilarityDataset dataset;


        
        public CandidateSetWithPivotDistance(ReferenceObjects ros, SimilarityDataset dataset, SimilarityDatasetObject q, int k)
        {
            super(k);
            this.ros=ros;
            this.dataset=dataset;
            this.q=q;
            sortedBestK=new TreeMap<DistanceWrapper,Integer>(new MaxDistanceWrapperComparator());       //here we maintain the k current best using the distance as entry
            sortedTempRes=new TreeMap<DistanceWrapper,Integer>(new MinDistanceWrapperComparator());      //here we maintain the tem res using the distance as entry
            currentBestK=new TIntObjectHashMap<DistanceWrapper>(); //here we maintain the k current best using the oid as entry
        }
        
        //if object o is already one of the k current best, we eliminate it
        private ObjWrapper eliminateDuplicateDistances(TreeMap<DistanceWrapper,ObjWrapper> orderedRes, DistanceWrapper d){
           ObjWrapper o =orderedRes.remove(d);
           return o;
        }
        
        private DistanceWrapper eliminateDuplicateObjects(TIntObjectHashMap<DistanceWrapper> objectsOfOrderedRes, int o){
            DistanceWrapper d=objectsOfOrderedRes.remove(o);
            return d;
        }
        
        int sortedInsertions;
        private void sortedInsert(int oid, DistanceWrapper dw){
            try{
                if(currentBestK.size()<getK()&&!currentBestK.contains(oid)){
                    Integer obj=oid;
                    sortedTempRes.remove(dw);
                    dw.maxDistance=dw.minDistance=dataset. getObject(oid).distance(q);
                    while(obj!=null){
                        obj=sortedTempRes.get(dw);
                        if(obj!=null){
                            dw.minDistance=dw.minDistance+0.000001*Math.random();
                        }
                    }
                    sortedTempRes.put(dw, oid);/**/
                    currentBestK.put(oid, dw);
                    obj=oid;
                    while(obj!=null){
                        obj=sortedBestK.get(dw);
                        if(obj!=null){
                            dw.maxDistance=dw.maxDistance+0.000001*Math.random();
                        }
                    }
                    sortedBestK.put(dw,oid);//<<---Conrollare che non si inserisca più volte la stessa distanza
                    maxDistance = sortedBestK.lastKey().maxDistance;
                    System.out.println("maxDistance: "+maxDistance+"; size: "+sortedBestK.size()+" "+currentBestK.size());
                }
                if(dw.maxDistance<maxDistance&&!currentBestK.contains(oid)){//we have a potentially better object
                    Integer obj=oid;
                    sortedTempRes.remove(dw);
                    dw.maxDistance=dw.minDistance=dataset.getObject(oid).distance(q);
                    while(obj!=null){
                        obj=sortedTempRes.get(dw);
                        if(obj!=null){
                            dw.minDistance=dw.minDistance+0.000001*Math.random();
                        }
                    }
                    sortedTempRes.put(dw, oid);/**/
                    currentBestK.put(oid, dw);
                    obj=oid;
                    while(obj!=null){
                        obj=sortedBestK.get(dw);
                        if(obj!=null){
                            dw.maxDistance=dw.maxDistance+0.000001*Math.random();
                        }
                    }
                    sortedBestK.put(dw,oid);//<<---Conrollare che non si inserisca più volte la stessa distanza
                    DistanceWrapper lastDistance=sortedBestK.lastKey();
                    int last=sortedBestK.lastEntry().getValue();
                    currentBestK.remove(last);
                    sortedBestK.remove(lastDistance);
                    maxDistance = sortedBestK.lastKey().maxDistance;
                    System.out.println("-------> updated with better object "+oid+"; size: "+sortedBestK.size()+" "+currentBestK.size()+" "+getTempRes().size()+" "+sortedTempRes.size());
                    System.out.println("maxDistance: "+maxDistance);
                }
            }catch(Exception e){
                e.printStackTrace();
            }
        }
        
        int updates;
        public void put(int o, int score, int position, int pivot, int postingListToBeAccessed, int accessedPostingLists)
        {
            try{
                long time=System.currentTimeMillis();
                DistanceWrapper dw=(DistanceWrapper)getTempRes().get(o);
                double q_p_dist=q.distance(ros.getObject(pivot));
                double o_p_dist=dataset.getObject(o).distance(ros.getObject(pivot));
                if (dw==null){//if it is not in the candidate set
                    dw=new DistanceWrapper();
                    dw.minDistance=Math.abs(q_p_dist-o_p_dist);
                    dw.maxDistance=q_p_dist+o_p_dist;
                    if(dw.minDistance<=maxDistance){//it is a candidate
                        getTempRes().put(o,dw);
                        Integer obj=o;
                        while(obj!=null){
                            obj=sortedTempRes.get(dw);
                            if(obj!=null){
                                dw.minDistance=dw.minDistance+0.000001*Math.random();
                            }
                        }
                        sortedTempRes.put(dw,o);
                    }
                }else{
                    sortedTempRes.remove(dw);
                    boolean reinsert=false;
                    if(this.sortedBestK.containsKey(dw)){
                        sortedBestK.remove(dw);
                        reinsert=true;
                    }
                    updates++;
                    dw.minDistance=Math.max(dw.minDistance,Math.abs(q_p_dist-o_p_dist));
                    dw.maxDistance=Math.min(dw.maxDistance,q_p_dist+o_p_dist);
                    Integer obj=o;
                    while(obj!=null){
                        obj=sortedTempRes.get(dw);
                        if(obj!=null){
                            dw.minDistance=dw.minDistance+0.000001*Math.random();
                        }
                    }
                    sortedTempRes.put(dw, o);
                    if(reinsert){
                        obj=o;
                        while(obj!=null){
                            obj=sortedBestK.get(dw);
                            if(obj!=null){
                                dw.maxDistance=dw.maxDistance+0.000001*Math.random();
                            }
                        }
                        sortedBestK.put(dw,o);//<<---Conrollare che non si inserisca più volte la stessa distanza
                    }
                }
                sortedInsert(o,dw);
                DistanceWrapper mdw=new DistanceWrapper();
                mdw.minDistance=maxDistance; //we want to eliminate those whose mindist is greater than maxdist
                SortedMap<DistanceWrapper,Integer> toBeRemoved=sortedTempRes.tailMap(mdw,false);
                for(java.util.Map.Entry<DistanceWrapper,Integer> e:toBeRemoved.entrySet()){
                    //sortedTempRes.remove(e.getKey());
                    int remove=e.getValue();
                    Object ooo=getTempRes().remove(remove);
                    if(ooo==null){
                        System.out.println("not found");
                    }
                }
                toBeRemoved.clear();
                //if(sortedTempRes.size()!=getTempRes().size())
                //    System.out.println("azzz");
                setPutTime(getPutTime()+System.currentTimeMillis()-time);
            }catch(Exception e){
                e.printStackTrace();
            }
        }
        
        
        
        public TreeMap<Double,Integer> getSortedCandidateSet(){
            System.out.print("Candidate set size: "+getTempRes().size());
//            System.err.println("Insertions: "+insertions+" Updates: " +updates+" Ordered insertions: "+orderedInsertions+" Saved insertions: "+savedInsertions+" ");
            TreeMap<Double,Integer> res=new TreeMap();
            try{
                for(int o:getTempRes().keys()){
                    //DistanceWrapper dw=((DistanceWrapper)getTempRes().get(o));
                    //res.put(dw.minDistance, o);
                    res.put(dataset.getObject(o).distance(q), o);
                }
                /*int num=50;
                for(int o:res.values()){
                    System.out.print("<"+o+","+((DistanceWrapper)getTempRes().get(o)).minDistance+","+dataset.getObject(o).distance(q)+">");
                    num--;
                    if(num==0)
                        return res;
                }*/
            }catch(Exception e){
                e.printStackTrace();
            }
            return res;
        }
        
    }
