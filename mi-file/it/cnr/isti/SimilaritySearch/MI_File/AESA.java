/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package it.cnr.isti.SimilaritySearch.MI_File;
import it.cnr.isti.Dataset.*;

/**
 *
 * @author Amato
 */
public class AESA {
    private int size;
    private SimilarityDatasetObject[] objects;
    private double[][] distances;
    private int last=-1;
    private boolean[] discarded;
    private boolean[] pivots;
    private double minDists[];
    private double D[];
    private int computedDistances=0;

    public AESA(int size){
        this.size=size;
        objects=new SimilarityDatasetObject[size];
        discarded=new boolean[size];
        pivots=new boolean[size];
        minDists=new double[size];
        distances=new double[size][size];
        for(int i=0;i<size;i++){
            discarded[i]=false;
            pivots[i]=false;
            minDists[i]=0;
        }
        D=new double[size];
    }

    public void add(SimilarityDatasetObject obj, int id){
        last++;
        obj.setInternalId(id);
        objects[last]=obj;
        for(int i=0;i<last;i++){
            double dist=obj.distance(objects[i]);
            distances[last][i]=dist;
        }
    }
    
    private int nextPivot(int p,double dist, int lastPivot,double r){
        int stop=-1;
        for(int u=lastPivot+1;u<=last;u++){
            double pivDist=(u<=p)?distances[p][u]:distances[u][p];
            double queryDist=dist;
            double minDist=Math.abs(pivDist-queryDist);
            if(minDist<r)
                return u;
        }
        return stop;
        /*if(p<last)
            return p+1;
        else
            return -1;*/
        /*double minD=Double.MAX_VALUE;
        int bestPivot=-1;
//        pivots[p]=true;
        double queryDist=dist;
        //for(int u=0;u<=last;u++){
        for(int u=p+1;u<=last;u++){  //This oly works with trivial strategy
            //if(!pivots[u]&&!discarded[u]){
            if(!discarded[u]){  //This oly works with trivial strategy
                double pivDist=(u<=p)?distances[p][u]:distances[u][p];
                double minDist=Math.abs(pivDist-queryDist);
                if(minDist>minDists[u])
                    minDists[u]=minDist;
                D[u]+=minDist;
                if(minDists[u]>r)
                    discarded[u]=true;
//                if (D[u]<minD){  //minSumOfDistances Strategy
//                    minD=D[u];
//                    bestPivot=u;
//                }
                if(bestPivot==-1) //trivial strategy: pivots are take sequentially
                   bestPivot=u;
            }
        }
        return bestPivot;*/
    }

    public java.util.TreeMap<Double,Integer> kNN(SimilarityDatasetObject q,int k){
        double r=Double.MAX_VALUE;
        boolean stop=false;
        int nextPivot=0;
        computedDistances=0;

        java.util.TreeMap<Double,Integer> res= new java.util.TreeMap<Double,Integer>();

        while(!stop){
            Integer o=nextPivot;
            double dist=q.distance(objects[nextPivot]);
            computedDistances++;
            if((res.size()<k)||(dist<(Double)res.lastKey())){
                while(o!=null)
                {
                    o=res.put(dist,o);
                    dist+=0.0001;
                }
                if (res.size()>k)
                    res.remove(res.lastKey());
                if (res.size()==k)
                    r=Math.min(r, res.lastKey());
            }
            //nextPivot=nextPivot(nextPivot,dist,r);
            nextPivot=nextPivot(res.firstEntry().getValue(),res.firstKey(),nextPivot,r);
            //nextPivot=nextPivot(res.lastEntry().getValue(),res.lastKey(),nextPivot,r);
            stop=nextPivot==-1;
        }
        for(int i=0;i<size;i++){
            discarded[i]=false;
            pivots[i]=false;
            minDists[i]=0;
        }
        for(java.util.Map.Entry<Double,Integer> e:res.entrySet())
            res.put(e.getKey(), objects[e.getValue()].getInternalId());
        return res;
    }

    /**
     * @return the computedDistances
     */
    public int getComputedDistances() {
        return computedDistances;
    }
}
