/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package it.cnr.isti.PersistentHashMap;

import java.util.Map.Entry;
import it.cnr.isti.PersistentDataset.*;

/**
 *
 * @author Amato
 */
public class PersistentEntry<K,V> implements PersistentDatasetObject,java.util.Map.Entry<K,V>{

    private static final long serialVersionUID =1234984763098734874L;

    K key;
    V value;
    long next;

    public long getNext() {
        return next;
    }

    public void setNext(long next) {
        this.next = next;
    }
    int internalId;

    public PersistentEntry(K key, V value) {
        this.key=key;
        this.value=value;
    }

    @Override
    public int getInternalId() {
        return internalId;
    }

    @Override
    public void setInternalId(int id) {
        internalId=id;
    }

    public K getKey() {
        return key;
    }

    public V getValue() {
        return value;
    }

    public V setValue(V value) {
        this.value=value;
        return value;
    }

}
