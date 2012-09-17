/*
 * Object32D.java
 *
 * Created on 21 aprile 2008, 16.56
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package Examples;
import it.cnr.isti.SimilaritySearch.Dataset.DatasetObject;

/**
 * Example of a data object consisting in a vector of 32 doubles
 * @author Giuseppe Amato
 * ISTI-CNR, Pisa, Italy
 * giuseppe.amato@isti.cnr.it
 */

public class Vector32D extends DatasetObject{
    static public int dim=32;
    private double objects[];
    private int id;

    private static final long serialVersionUID=2283784013781082621L;
    
    
    /** Creates a new instance of Object32D
     * @param id_p the external object identifier
     * @param v the vector of double
     */
    public Vector32D(int id_p,double[] v) {
        id=id_p;
        objects=v;
    }
    
    public int getInternalId(){
        return id;
    }
    
    public void setInternalId(int pid){
        id=pid;
    }
    
    /**Eucledian distance**/
    private double eucledian_distance(Vector32D o){
        double dist=0;
        for (int j=0;j<objects.length;j++){
            double diff=objects[j]-o.objects[j];
            dist+=diff*diff;
        }
        return Math.sqrt(dist);
    }
    
    public double distance(DatasetObject o){
        return eucledian_distance((Vector32D)o);
    }

    /**
     * Initialize the vector with random values. This is just to generate objects somehow, in reality you never do that.
     */
    public void initializeRandom(){
        objects=new double[dim];
        for (int i=0;i<dim;i++)
            objects[i]=Math.random();
    }
}
