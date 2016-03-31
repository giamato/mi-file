/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package it.cnr.isti.PerformanceMeasures;

import java.util.HashSet;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 *
 * @author Amato
 */
public class PerformanceMeasure<T> {

    public double precision(SortedSet<T> retrieved, SortedSet<T> relevant){
        SortedSet<T> test=relevant;
        HashSet<T> s=new HashSet<T>();
        for(T elem:test)
            s.add(elem);
        int count=0;
        for(T elem:retrieved)
            if(s.contains(elem))
                count++;
        return (double)count/(double)retrieved.size();
    }


    public double recall(SortedSet<T> retrieved, SortedSet<T> relevant){
        SortedSet<T> test=relevant;
        HashSet<T> s=new HashSet<T>();
        for(T elem:test)
            s.add(elem);
        int count=0;
        for(T elem:retrieved)
            if(s.contains(elem))
                count++;
        return (double)count/(double)relevant.size();
    }

    public double f1(SortedSet<T> retrieved, SortedSet<T> relevant){
        if(retrieved==null)
            return 0;
        double p=precision(retrieved, relevant);
        double r=recall(retrieved, relevant);
        if(r+p==0)
            return 0;
        double f1=2*((r*p)/(r+p));
        return f1;
    }
    
    public double averagePrecision(SortedSet<T> retrieved, SortedSet<T> relevant){
        if(retrieved==null)
                return 0;
        SortedSet<T> test=retrieved;
        HashSet<T> s=new HashSet<T>();
        for(T elem:relevant)
            s.add(elem);
        double precisionSum=0;
        TreeSet<T> retrievedAtK=new TreeSet<T>(retrieved.comparator());
        for(T elem:retrieved){
            retrievedAtK.add(elem);
            if(s.contains(elem))
                precisionSum+=precision(retrievedAtK,relevant);
        }
        return (double)precisionSum/(double)relevant.size();
    }

    /*public SortedSet<T> intersection(SortedSet<T> s1, SortedSet<T> s2){
        TreeSet<T> intersection=null;
        if(s1!=null&&s2!=null){
            intersection=new TreeSet<T>(s1.comparator());
            HashSet<T> s=new HashSet<T>();
            for(T elem:s1)
                s.add(elem);
            for(T elem:s2)
                if(s.contains(elem))
                    intersection.add(elem);
        }
        return intersection;
    }*/

    public SortedSet<T> intersection(SortedSet<T> s1, SortedSet<T> s2){
        TreeSet<T> intersection=null;
        if(s1!=null&&s2!=null){
            if(s1.size()<s2.size()){
                SortedSet<T> tmp=s2;
                s2=s1;
                s1=tmp;
            }
            intersection=new TreeSet<T>(s1.comparator());
            for(T elem:s2)
                if(s1.contains(elem))
                    intersection.add(elem);
        }
        return intersection;
    }

    public boolean isIn(T val, SortedSet<T> s1){
        return s1.contains(val);
    }

    public SortedSet<T> difference(SortedSet<T> s1, SortedSet<T> s2){
        TreeSet<T> difference=null;
        if(s2!=null&&s1!=null){
            difference=new TreeSet<T>(s2.comparator());
            HashSet<T> s=new HashSet<T>();
            for(T elem:s2)
                s.add(elem);
            for(T elem:s1)
                if(!s.contains(elem))
                    difference.add(elem);
        }
        return difference;
    }

    public SortedSet<T> union(SortedSet<T> s1, SortedSet<T> s2){
        TreeSet<T> union=null;
        if(s1!=null&&s2!=null){
            union=new TreeSet<T>(s1.comparator());
            HashSet<T> s=new HashSet<T>();
            for(T elem:s1)
                s.add(elem);
            for(T elem:s2)
                s.add(elem);
            for(T elem:s)
                union.add(elem);
        }
        return union;
    }



}
