/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package it.cnr.isti.PersistentDataset;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel.MapMode;
import java.util.zip.GZIPOutputStream;
import java.util.zip.GZIPInputStream;

/**
 *
 * @author Amato
 */
public class PersistentDatasetCompressed<DO extends PersistentDatasetObject> extends PersistentDataset{

    /*class ByteBufferBackedInputStream extends InputStream{

      ByteBuffer buf;
      ByteBufferBackedInputStream( ByteBuffer buf){
        this.buf = buf;
      }
      public synchronized int read() throws IOException {
        if (!buf.hasRemaining()) {
          return -1;
        }
        return buf.get();
      }
      public synchronized int read(byte[] bytes, int off, int len) throws IOException {
        len = Math.min(len, buf.remaining());
        buf.get(bytes, off, len);
        return len;
      }
    }*/

    MappedByteBuffer mbb;

     /** Creates a new instance of PersistentDataset.
     * @param indexDirectory_p the directory where the index is stored
     * @param mode "r","rw","rws",rwd"
     * @throws java.io.IOException there were problems in opening the needed files
     */
    public PersistentDatasetCompressed(String indexDirectory_p,String mode) throws java.io.IOException{
        super(indexDirectory_p,mode);
        /*if(mode.equals("r"))
            mbb = databaseFile.getChannel().map(MapMode.READ_ONLY, 0, databaseFile.length());
        else
            if(mode.equals("rw")||mode.equals("rws")||mode.equals("rwd"))
                mbb = databaseFile.getChannel().map(MapMode.READ_WRITE, 0, databaseFile.length());*/
    }

    public PersistentDatasetCompressed(String indexDirectory_p) throws java.io.IOException{
        super(indexDirectory_p);
        /*mbb = databaseFile.getChannel().map(MapMode.READ_ONLY, 0, databaseFile.length());*/
    }

    /*public synchronized DO getObjectFromOffset(int id) throws Exception{

       if(offsetFile==null || databaseFile==null)
            openFiles(databaseDirectory);

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
            endOffset=databaseFile.length();
       
        byte[] buff=new byte[(int)(endOffset-startOffset)];
        databaseFile.seek(startOffset);
        databaseFile.read(buff);

        return getObjectFromOffset(buff);

    }*/

    @Override
    protected DO getObjectFromOffset(long startOffset)throws Exception{
        /*databaseFile.seek(startOffset);
        databaseFile.read(buff);
        ByteArrayInputStream bais=new ByteArrayInputStream(buff);
        GZIPInputStream gz=new GZIPInputStream(bais);
        ObjectInputStream baiso=new ObjectInputStream(gz);
        DO obj=(DO)baiso.readObject();
        return obj;*/

        if(startOffset==-1)
            return null;
        InputStream is = java.nio.channels.Channels.newInputStream(databaseFile.getChannel().position(startOffset));
        //mbb.position((int)startOffset);
        //InputStream is = new ByteBufferBackedInputStream(mbb);
        GZIPInputStream gz=new GZIPInputStream(is);
        ObjectInputStream baiso=new ObjectInputStream(gz);
        DO obj=(DO)baiso.readObject();
        return obj;

    }

 /*   @Override
    public int insert(DO obj) throws Exception{
        openFiles(databaseDirectory);

        int currentId=(int)offsetFile.length()/8;
        long currentOffset=databaseFile.length();

        obj.setInternalId(currentId);  //Here we make the assoication betwen objects and internal identifiers

        offsetFile.seek(offsetFile.length());
        offsetFile.writeLong(currentOffset);

        return currentId;
    }*/

    
    @Override
    protected void writeObject(PersistentDatasetObject pobj, long currentOffset) throws java.io.IOException{
        DO obj =(DO)pobj;
        ByteArrayOutputStream baos=new ByteArrayOutputStream();
        GZIPOutputStream gz=new GZIPOutputStream(baos);
        ObjectOutputStream baoso=new ObjectOutputStream(gz);
        baoso.writeObject(obj);
        baoso.close();

        byte[] buff=baos.toByteArray();
        databaseFile.seek(currentOffset);
        databaseFile.write(buff);
    }

}
