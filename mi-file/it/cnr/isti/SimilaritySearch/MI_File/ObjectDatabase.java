/*
 * ObjectDatabase.java
 *
 * Created on 15 maggio 2008, 22.12
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package it.cnr.isti.SimilaritySearch.MI_File;

import it.cnr.isti.SimilaritySearch.Dataset.DatasetObject;
import java.io.*;

/**
 *
 * @author amato
 */
public class ObjectDatabase {
    
    protected String indexDirectory;
    protected RandomAccessFile offsetFile=null;
    protected RandomAccessFile databaseFile=null;
//    private long offsets[];
//    MappedByteBuffer offsets=null;
//    MappedByteBuffer database=null;
    
    
    /** Creates a new instance of ObjectDatabase */
    public ObjectDatabase(String indexDirectory_p) throws Exception{
        indexDirectory = indexDirectory_p;
        openFiles(indexDirectory);
    }
    
    public void close(){
        try{
            offsetFile.close();
            databaseFile.close();
        }catch (Exception e){
            System.err.println("Cannot close database files");
            e.printStackTrace();
        }
    }
    
    protected void finalize () throws Throwable{
        close();
    }
    
    protected void openFiles(String indexDirectory_p) throws Exception{
//        RandomAccessFile offsetFile=null;
//        RandomAccessFile databaseFile=null;
        //With persistent buffers
        File offset_f=new File(indexDirectory+"/offsets.dat");
        File database_f=new File(indexDirectory+"/database.dat");
        File indexDirectory_f=new File(indexDirectory);
        if(!indexDirectory_f.exists())
            indexDirectory_f.mkdir();
        
        if(offsetFile==null)
            offsetFile=new RandomAccessFile(offset_f,"rw");
        if(databaseFile==null)
            databaseFile=new RandomAccessFile(database_f,"rw");/**/


        
        // with MemoryMappedBuffers
/*        if(offsets==null){
            offsetFile=new RandomAccessFile(offset_f,"rw");
            offsets=offsetFile.getChannel().map(java.nio.channels.FileChannel.MapMode.READ_WRITE,0,offsetFile.length());
        }
        if(database==null){
            databaseFile=new RandomAccessFile(database_f,"rw");
            database=databaseFile.getChannel().map(java.nio.channels.FileChannel.MapMode.READ_WRITE,0,databaseFile.length());
        };/**/
        
        
        
    }
    
    public synchronized DatasetObject getObject(int id) throws Exception{
        
//        System.out.println("ObjectDatbase: Executed getObject!!!!!");
       if(offsetFile==null || databaseFile==null)
            openFiles(indexDirectory);
        
      //With persistent offsets
       long position=id*8;
       long l=offsetFile.length();
       if(position>=offsetFile.length())
           throw new Exception("Object with internal identifier "+id+" does not exist!");
        
        offsetFile.seek(position);
        
        long startOffset=offsetFile.readLong();
        long endOffset;
        if(position+8!=offsetFile.length())
            endOffset=offsetFile.readLong();
        else
            endOffset=databaseFile.length();/**/
     
       //With in memory offsets;
/*       if(offsets==null){
           offsets=new long[(int)(offsetFile.length()/8)];
           for(int i=0;i<offsetFile.length();i+=8)
               offsets[i/8]=offsetFile.readLong();
       }
       if(id>=offsets.length)
           throw new Exception("Object with internal identifier "+id+" does not exist!");
   
        long startOffset=offsets[id];
        long endOffset;
        if(id+1!=offsets.length)
            endOffset=offsets[id+1];
        else
            endOffset=databaseFile.length(); /**/
       
        //With persistent database
        byte[] buff=new byte[(int)(endOffset-startOffset)];
        databaseFile.seek(startOffset);
        databaseFile.read(buff);/**/
       
       //START NEW with MemoryMappedBuffers
/*       int position=id*8;
       if(position>=offsets.capacity())
           throw new Exception("Object with internal identifier "+id+" does not exist!");
        
        offsets.position(position);
        
        long startOffset=offsets.getLong();
        long endOffset;
        if(position+8!=offsets.capacity())
            endOffset=offsets.getLong();
        else
            endOffset=database.capacity();
        
        byte[] buff=new byte[(int)(endOffset-startOffset)];
        database.position((int)startOffset);
        database.get(buff);
       //END NEW*/
                
        ByteArrayInputStream bais=new ByteArrayInputStream(buff);
        ObjectInputStream baiso=new ObjectInputStream(bais);
        DatasetObject obj=(DatasetObject)baiso.readObject();
        
        return obj;
        
    }
    
    public int insert(DatasetObject obj) throws Exception{
        openFiles(indexDirectory);
        
        int currentId=(int)offsetFile.length()/8;
        long currentOffset=databaseFile.length();

        obj.setInternalId(currentId);  //Here we make the assoication betwen objects and internal identifiers
        
        offsetFile.seek(offsetFile.length());
        offsetFile.writeLong(currentOffset);
        
/*        //with in-memory offsets
        offsets=null;/**/
        
        ByteArrayOutputStream baos=new ByteArrayOutputStream();
        ObjectOutputStream baoso=new ObjectOutputStream(baos);
        baoso.writeObject(obj);
        byte[] buff=baos.toByteArray();
        databaseFile.seek(currentOffset);
        databaseFile.write(buff);
        
/*        //START NEW with MemoryMappedBuffers
        int currentId=(int)offsets.capacity()/8;
        long currentOffset=database.capacity();
        
        offsets.position(offsets.capacity());
        offsets.putLong(currentOffset);
        
        ByteArrayOutputStream baos=new ByteArrayOutputStream();
        ObjectOutputStream baoso=new ObjectOutputStream(baos);
        baoso.writeObject(obj);
        byte[] buff=baos.toByteArray();
        database.position((int)currentOffset);
        database.put(buff);
        //END NEW*/
        
        return currentId;
    }
    
    public int size() throws Exception{
        return (int) offsetFile.length()/8;
//        return (int) offsets.capacity()/8;
    }
 
    
}
