/*
 * PersistentDataset.java
 *
 * Created on 15 maggio 2008, 22.12
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package it.cnr.isti.PersistentDataset;

import java.io.*;
import java.nio.ByteBuffer;
import it.cnr.isti.Dataset.*;

/**
 * This class implements the Dataset interfaces and represents/manages a persistent database of objects.
 * Each object is associated with an internal identifier, which should not be confused with the
 * (external) object identifier, that can be obtained by object.getInternalId(). Specifically, the getObject(id)
 * method of this class retrieves the object associated with the internal identifier id, which migh
 * be different than the external object identifier. In other words, id == getObject(id).getInternalId().
 *
 *
 * @author Giuseppe Amato
 * ISTI-CNR, Pisa, Italy
 * giuseppe.amato@isti.cnr.it
 */
public class PersistentDataset<DO extends PersistentDatasetObject> implements Dataset{
    
    protected String databaseDirectory;
    protected RandomAccessFile offsetFile=null;
    protected RandomAccessFile databaseFile=null;
    protected String mode="r"; //read or read-write; read-only by default
//    private long offsets[];
//    MappedByteBuffer offsets=null;
//    MappedByteBuffer database=null;
    
    
    /** Creates a new instance of PersistentDataset.
     * @param databaseDirectory_p the directory where the database is stored
     * @param mode "r","rw","rws",rwd"
     * @throws java.io.IOException there were problems in opening the needed files
     */
    public PersistentDataset(String databaseDirectory_p,String mode) throws java.io.IOException{
        this.mode=mode;
        set(databaseDirectory_p);
    }

    /** Creates a new instance of PersistentDataset in the specified directory. The database is open in read-only.
     * @param directory_p directory where the database is created.
     */
    public PersistentDataset(String databaseDirectory_p) throws java.io.IOException{
        this.mode="r";
        set(databaseDirectory_p);
    }

    public void set(String databaseDirectory_p) throws java.io.IOException{
        databaseDirectory = databaseDirectory_p;
        openFiles(databaseDirectory);
    }

   
    public void close() throws java.io.IOException{
        if(offsetFile!=null)
            offsetFile.close();
        if(databaseFile!=null)
            databaseFile.close();
            offsetFile=null;
            databaseFile=null;
    }
    
    @Override
    protected void finalize () throws Throwable{
        try {
            close();
        } finally {
            super.finalize();
        }
    }
    
    protected void openFiles(String databaseDirectory_p) throws java.io.IOException{
        File offset_f=new File(databaseDirectory+"/offsets.dat");
        File database_f=new File(databaseDirectory+"/database.dat");
        File databaseDirectory_f=new File(databaseDirectory);
        if(!databaseDirectory_f.exists())
            databaseDirectory_f.mkdirs();
        
        if(offsetFile==null)
            offsetFile=new RandomAccessFile(offset_f,mode);
        if(databaseFile==null)
            databaseFile=new RandomAccessFile(database_f,mode);/**/
    }

    
    public synchronized DO getObject(int id) throws Exception{
        
       if(offsetFile==null || databaseFile==null)
            openFiles(databaseDirectory);
        
       long position=((long)id)*8;
       long l=offsetFile.length();
       if(position>=offsetFile.length())
           //throw new Exception("Object with internal identifier "+id+" does not exist!");
           return null;
        
        offsetFile.seek(position);
        
        //original
        long startOffset=offsetFile.readLong();/**/
        //alternate:
        /*byte[] longBuff=new byte[8];
        offsetFile.read(longBuff);
        long startOffset = ByteBuffer.wrap(longBuff).getLong();*/
        //end_alternate/**/
                        
        return getObjectFromOffset(startOffset);
        
    }

    synchronized protected DO getObjectFromOffset(long startOffset)throws Exception{
        /*databaseFile.seek(startOffset);
        databaseFile.read(buff);
        ByteArrayInputStream bais=new ByteArrayInputStream(buff);
        ObjectInputStream baiso=new ObjectInputStream(bais);
        DO obj=(DO)baiso.readObject();
        return obj;*/

        if(startOffset==-1)
            return null;
        InputStream is = java.nio.channels.Channels.newInputStream(databaseFile.getChannel().position(startOffset));
        ObjectInputStream baiso=new ObjectInputStream(is);
        DO obj=(DO)baiso.readObject();
        return obj;
    }
    
    /**
     * Insert an object
     * @param o object to be inserted
     * @return internal id of the object
     * @throws java.io.IOException
     */
    public int insert(DatasetObject o) throws java.io.IOException{
        DO obj=(DO)o;
        openFiles(databaseDirectory);
        
        int currentId=(int)offsetFile.length()/8;
        long currentOffset=databaseFile.length();

        obj.setInternalId(currentId);  //Here we make the assoication betwen objects and internal identifiers
        
        offsetFile.seek(offsetFile.length());
        offsetFile.writeLong(currentOffset);

        writeObject(obj, currentOffset);
        
        return currentId;
    }

    /**
     * update an object in the database
     * @param id internal id of the object
     * @param obj new object
     * @return the internal id of the object
     * @throws java.io.IOException
     */
    public int update(int id,DO obj) throws java.io.IOException{
        openFiles(databaseDirectory);

        int maxId=(int)(offsetFile.length()/8)-1;
        if(id>maxId)
            throw new java.io.IOException("Internal id "+id+" does not exist and cannot be updated. Maximum allowable id is "+maxId);
        long currentOffset=databaseFile.length();

        obj.setInternalId(id);  //Here we make the assoication betwen objects and internal identifiers

        offsetFile.seek(((long)id)*8);
        offsetFile.writeLong(currentOffset);

        writeObject(obj, currentOffset);

        return id;
    }

    protected void writeObject(PersistentDatasetObject pobj, long currentOffset) throws java.io.IOException{
        DO obj =(DO)pobj;
        ByteArrayOutputStream baos=new ByteArrayOutputStream();
        ObjectOutputStream baoso=new ObjectOutputStream(baos);
        baoso.writeObject(obj);
        byte[] buff=baos.toByteArray();
        databaseFile.seek(currentOffset);
        databaseFile.write(buff);
    }
    
    public int size() throws Exception{
        return (int) offsetFile.length()/8;
//        return (int) offsets.capacity()/8;
    }
    
    int bufferSize=100000;
    byte buffer[]=new byte[bufferSize*8];
    
    public void cacheOffsets() throws Exception{

        long total=0;
        long taken=0;
        long count=0;
        offsetFile.seek(0);
        do{
            count = offsetFile.read(buffer);
            if(count >0){
                ByteBuffer bb=ByteBuffer.wrap(buffer);
                for(int j=0;j<count/8;j++){
                    total++;
                    if(bb.getLong()!=-1)
                        taken++;
                }
            }
            System.out.println(count +" "+total+" "+taken+" "+(double)total/(double)taken);
        }while(count >0);
    }
}
