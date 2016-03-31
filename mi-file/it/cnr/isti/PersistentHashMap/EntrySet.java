/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package it.cnr.isti.PersistentHashMap;
import java.util.Map.Entry;

/**
 *
 * @author Amato
 */
public class EntrySet<K,S> extends java.util.AbstractSet<Entry<K,S>>{

    private PersistentHashMap<K,S> hashMap;

    public EntrySet(PersistentHashMap<K, S> hashMap) {
        this.hashMap = hashMap;
    }

    @Override
    public EntryIterator<K,S> iterator() {
        return new EntryIterator<K,S>(hashMap);
    }

    @Override
    public int size() {
        int count =0;
        for(Entry<K,S> e:this )
            count++;
        return count;
    }

}
