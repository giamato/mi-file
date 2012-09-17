/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package it.cnr.isti.SimilaritySearch.MI_File;

import it.cnr.isti.SimilaritySearch.Dataset.*;
import java.io.*;
import java.util.zip.GZIPOutputStream;
import java.util.zip.GZIPInputStream;

/**
 *
 * @author Amato
 */
public class ObjectDatabaseCompressed extends ObjectDatabase{

    /** Creates a new instance of ObjectDatabase */
    public ObjectDatabaseCompressed(String indexDirectory_p) throws Exception{
        super(indexDirectory_p);
    }

    public synchronized DatasetObject getObject(int id) throws Exception{

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
        //System.out.println(startOffset+" "+endOffset);
        byte[] buff=new byte[(int)(endOffset-startOffset)];
        databaseFile.seek(startOffset);
        databaseFile.read(buff);/**/


        ByteArrayInputStream bais=new ByteArrayInputStream(buff);
        GZIPInputStream gz=new GZIPInputStream(bais);
        ObjectInputStream baiso=new ObjectInputStream(gz);
        DatasetObject obj=(DatasetObject)baiso.readObject();

        return obj;

    }

    public int insert(DatasetObject obj) throws Exception{
        openFiles(indexDirectory);
        //System.out.println("Compressed!");

        int currentId=(int)offsetFile.length()/8;
        long currentOffset=databaseFile.length();
        //System.out.println(currentOffset);

        obj.setInternalId(currentId);  //Here we make the assoication betwen objects and internal identifiers

        offsetFile.seek(offsetFile.length());
        offsetFile.writeLong(currentOffset);

/*        //with in-memory offsets
        offsets=null;/**/

/*        ByteArrayOutputStream baos=new ByteArrayOutputStream();
        ObjectOutputStream baoso=new ObjectOutputStream(baos);
        baoso.writeObject(obj);*/
        
        ByteArrayOutputStream baos=new ByteArrayOutputStream();
        GZIPOutputStream gz=new GZIPOutputStream(baos);
        ObjectOutputStream baoso=new ObjectOutputStream(gz);
        baoso.writeObject(obj);
        baoso.close();


        byte[] buff=baos.toByteArray();
        databaseFile.seek(currentOffset);
        databaseFile.write(buff);


        return currentId;
    }

}
