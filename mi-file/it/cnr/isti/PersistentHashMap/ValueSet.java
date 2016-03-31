/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package it.cnr.isti.PersistentHashMap;

/**
 *
 * @author Amato
 */
public class ValueSet<V> extends java.util.AbstractSet<V>{

    private PersistentHashMap<?,V> hashMap;

    public ValueSet(PersistentHashMap<?,V> hashMap) {
        this.hashMap = hashMap;
    }

    @Override
    public ValueIterator<V> iterator() {
        return new ValueIterator(hashMap);
    }

    @Override
    public int size() {
        return hashMap.entrySet().size();
    }

}
