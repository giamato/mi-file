/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package it.cnr.isti.PersistentHashMap;
import it.cnr.isti.PersistentDataset.*;
import java.io.File;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Map;
import java.util.Map.Entry;


/**
 *
 * @param <K>
 * @param <V>
 * @author Amato
 */
public class PersistentHashMap<K,V> extends PersistentDataset<PersistentEntry> implements Map<K,V>{
    private MappedByteBuffer offsets=null;
    private long capacity;

    public PersistentHashMap(long capacity, String indexDirectory_p,String mode) throws Exception{
        super(indexDirectory_p,mode);
        if(this.offsetFile.length()==0){
            //System.out.println("Start creating hashMap");
            //long startTime=System.currentTimeMillis();
            initializeHashMap(capacity);
            //long endTime=System.currentTimeMillis();
            //System.out.println("HashMap created in "+ (endTime-startTime) +" milliseconds");
        }
        if(offsetFile.length()>=Integer.MAX_VALUE&&mode.contains("w")){
            offsets=offsetFile.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, offsetFile.length());
            offsets.load();
        }
        this.capacity=offsetFile.length()/8;
    }
    
    private void initializeHashMap(long capacity) throws Exception{
        int bufferSize=100000;
        //System.out.println("initializing with buffer size "+bufferSize);
        byte buffer[]=new byte[bufferSize*8];
        ByteBuffer bb=ByteBuffer.wrap(buffer);
        for(int i=0;i<bufferSize;i++)
            bb.putLong(-1);
        long q=capacity/bufferSize;
        int r=(int)capacity % bufferSize;
        for(int i=0;i<q;i++)
            offsetFile.write(buffer);
        offsetFile.write(buffer, 0, r*8);
    }
    
    int bufferSize=100000;
    byte buffer[]=new byte[bufferSize*8];
    public void computeLoadFactor() throws Exception{

        long total=0;
        long taken=0;
        long count=0;
        offsetFile.seek(0);
        do{
            count = offsetFile.read(buffer);
            if(count >0){
                ByteBuffer bb=ByteBuffer.wrap(buffer);
                for(int j=0;j<bufferSize;j++){
                    total++;
                    if(bb.getLong()!=-1)
                        taken++;
                }
            }
            System.out.println(count +" "+total+" "+taken+" "+(double)total/(double)taken);
        }while(count >0);
    }
    
    

    private long hash(K key) throws Exception{
        return Math.abs(key.hashCode())%(capacity);
    }

    private long getPreceedingConflictingEntryOffset(long currentOffset, K key) throws Exception{
        long preceedingOffset=-1;
        PersistentEntry current=getObjectFromOffset(currentOffset);
        while(current!=null && !current.getKey().equals(key)){
            preceedingOffset=currentOffset;
            currentOffset=current.getNext();
            current=getObjectFromOffset(currentOffset);
        }
        if(current==null)
            return -1;
        else
            return preceedingOffset;
    }

    public V put(K key,V value){
        try{
            openFiles(databaseDirectory);
            V oldValue=null;
            long startId=hash(key);
            long startOffset=readOffset((int)startId);
            PersistentEntry newEntry=new PersistentEntry(key,value);
            long preceedingEntryOffset=getPreceedingConflictingEntryOffset(startOffset,key);
            if(preceedingEntryOffset==-1){ //it was not found or it is the first
                if(startOffset!=-1 &&
                        (getObjectFromOffset(startOffset)).getKey().equals(key)){//if the first entry has the same key we bypass it
                    newEntry.setNext((getObjectFromOffset(startOffset)).getNext());
                    oldValue=(V) (getObjectFromOffset(startOffset)).getValue();
                }else{
                    newEntry.setNext(startOffset);
                }
                update((int)startId, newEntry);
            }else{
                PersistentEntry preceedingEntry=getObjectFromOffset(preceedingEntryOffset);
                PersistentEntry oldEntry=getObjectFromOffset(preceedingEntry.getNext());
                long nextEntryOffset=oldEntry.getNext();
                long newEntryOffset=databaseFile.length();
                newEntry.setNext(nextEntryOffset);
                preceedingEntry.setNext(newEntryOffset);
                writeObject(preceedingEntry,preceedingEntryOffset);
                writeObject(newEntry,newEntryOffset);
                oldValue=(V) oldEntry.getValue();
            }
            return oldValue;
        }catch(Exception e){
            e.printStackTrace();
        }
        return null;
    }
    
    public V put_fast(K key,V value){  // Be aware it does not check that the key is already there. It does not return the previous value associated with the key
        try{
            openFiles(databaseDirectory);
            long startId=hash(key);
            long startOffset=readOffset((int)startId);
            PersistentEntry newEntry=new PersistentEntry(key,value);
            newEntry.setNext(startOffset);
            update((int)startId, newEntry);
        }catch(Exception e){
            e.printStackTrace();
        }
        return null;
    }

    public V get(Object k) {
        try{
            K key=(K)k;
            openFiles(databaseDirectory);
            V value=null;
            long startId=hash(key);
            long startOffset=readOffset((int)startId);
            if(startOffset!=-1){
                PersistentEntry current=getObjectFromOffset(startOffset);
                while(current!=null && !current.getKey().equals(key)){
                    current=getObjectFromOffset(current.getNext());
                }
                if(current!=null)
                    value=(V)current.getValue();
            }
            return value;
        }catch(Exception e){
            e.printStackTrace();
        }
        return null;
    }

    synchronized long readOffset(int id) throws Exception{
        long maxId=capacity-1;
        if(id>maxId)
            throw new Exception("Internal id "+id+" does not exist. Maximum allowable id is "+maxId);
        if(offsets==null){
            offsetFile.seek(((long)id)*8);
            return offsetFile.readLong();
        }else{
            offsets.position(id*8);
            return offsets.getLong();
        }
    }
    
    PersistentEntry<K,V> getObjectFromOffset_friend(long offset) throws Exception{
        return super.getObjectFromOffset(offset);
    }

    public int capacity() throws java.io.IOException{
        return (int)(capacity);
    }

    public int usedHashes() throws Exception{
        int databaseSize=super.size();
        int usedHashes=0;
        for(int i=0;i<databaseSize;i++)
            if(readOffset(i)!=-1)
                usedHashes++;
        return usedHashes;
    }

    public void garbageCollection(){
        try{
            String cleanedDir=this.databaseDirectory+"/cleanedPersistentHashMap";
            PersistentHashMap<K,V> newTagFrequencies=new PersistentHashMap<K,V>(super.size(),cleanedDir,"rw");
            int count=3;
            for(Entry<K,V>e:this.entrySet())
                newTagFrequencies.put(e.getKey(), e.getValue());
            File offsets=new File(this.databaseDirectory+"/offsets.dat");
            File database=new File(this.databaseDirectory+"/database.dat");
            this.close();
            if(!offsets.delete())
                throw new Exception("Failed to delete uncleaned offset.dat and database.dat files. The celaned files can be found in the "+cleanedDir);
            if(!database.delete())
                throw new Exception("Failed to delete uncleaned database.dat file. The celaned file can be found in the "+cleanedDir);
            newTagFrequencies.close();
            offsets=new File(cleanedDir+"/offsets.dat");
            database=new File(cleanedDir+"/database.dat");
            if(!offsets.renameTo(new File(databaseDirectory+"/offsets.dat")))
                throw new Exception("Failed to move cleaned offset.dat and database.dat files. The celaned files can be found in the "+cleanedDir);
            if(!database.renameTo(new File(databaseDirectory+"/database.dat")))
                throw new Exception("Failed to move cleaned database.dat files. The celaned file can be found in the "+cleanedDir);
            this.openFiles(databaseDirectory);
        }catch(Exception e){
            e.printStackTrace();
        }
    }

    public boolean containsKey(Object k) {
        try{
            K key=(K)k;
            openFiles(databaseDirectory);
            boolean found=false;
            long startId=hash(key);
            long startOffset=readOffset((int)startId);
            if(startOffset!=-1){
                PersistentEntry current=getObjectFromOffset(startOffset);
                while(current!=null && !current.getKey().equals(key)){
                    current=getObjectFromOffset(current.getNext());
                }
                if(current!=null)
                    found=true;
            }
            return found;
        }catch(Exception e){
            e.printStackTrace();
        }
        return false;
    }

    public void putAll(Map<? extends K, ? extends V> m) {
        for(Entry<? extends K,? extends V> e:m.entrySet()){
            put(e.getKey(),e.getValue());
        }
    }

    public it.cnr.isti.PersistentHashMap.EntrySet<K,V> entrySet() {
        return new it.cnr.isti.PersistentHashMap.EntrySet<K,V>(this);
    }

    public it.cnr.isti.PersistentHashMap.KeySet<K> keySet() {
        return new it.cnr.isti.PersistentHashMap.KeySet<K>(this);
    }

    public it.cnr.isti.PersistentHashMap.ValueSet<V> values() {
        return new it.cnr.isti.PersistentHashMap.ValueSet<V>(this);
    }

    public boolean isEmpty() {
       //return  !new it.cnr.isti.PersistentHashMap.EntrySet<K,V>(this).iterator().hasNext();
        return  new it.cnr.isti.PersistentHashMap.EntrySet<K,V>(this).isEmpty();
    }

    @Override
    public int size(){
        return new it.cnr.isti.PersistentHashMap.EntrySet<K,V>(this).size();
    }

    public boolean containsValue(Object value) {
        V v=(V) value;
        for(V val:values())
            if(val.equals(v))
                return true;
        return false;
    }

    public V remove(Object key) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void clear() {
        throw new UnsupportedOperationException("Not supported yet.");
    }
    
    @Override
    protected void finalize() throws Throwable{
        try {
            offsets.force();
            offsets=null;
            System.gc();
        } finally {
            super.finalize();
        }
    }
}
