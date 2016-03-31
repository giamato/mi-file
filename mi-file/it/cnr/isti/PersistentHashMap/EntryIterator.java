/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package it.cnr.isti.PersistentHashMap;
import java.util.Iterator;
import java.util.Map.Entry;

/**
 *
 * @author Amato
 */
class EntryIterator<K,S> implements Iterator<Entry<K,S>>{


    private int currentEntryHash=-1;
    private long currentEntryOffset=-1;
    private PersistentHashMap<K,S> hashMap;

    public EntryIterator(PersistentHashMap<K, S> hashMap) {
        this.hashMap = hashMap;
    }

    public boolean hasNext() {
        try{
            if(currentEntryHash==-1||currentEntryOffset==-1)
                seekNextEntry();
            if(currentEntryHash==Integer.MAX_VALUE)
                return false;
            else
                return true;
        }catch(Exception e){
            e.printStackTrace();
        }
        return false;
    }

    public Entry<K,S> next() {
        try{
            if(currentEntryHash==Integer.MAX_VALUE)
                return null;
            else{
                if(currentEntryHash==-1||currentEntryOffset==-1){
                    seekNextEntry();
                    return next();
                }
                PersistentEntry<K,S> e= hashMap.getObjectFromOffset_friend(currentEntryOffset);
                currentEntryOffset=e.getNext();
                return e;
            }
        }catch(Exception e){
            e.printStackTrace();
        }
        return null;
    }

    private void seekNextEntry() throws Exception{
        if(currentEntryHash==Integer.MAX_VALUE)
            return;
        else{
            for(currentEntryHash=currentEntryHash+1;currentEntryHash<hashMap.capacity();currentEntryHash++){
                currentEntryOffset=hashMap.readOffset(currentEntryHash);
                if(currentEntryOffset!=-1){
                    return;
                }
            }
            currentEntryHash=Integer.MAX_VALUE;
        }
    }


    public void remove() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

}
