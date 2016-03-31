/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package it.cnr.isti.PersistentHashMap;


/**
 *
 * @author Amato
 */
public class KeySet<K> extends java.util.AbstractSet<K>{

    private PersistentHashMap<K,?> hashMap;

    public KeySet(PersistentHashMap<K,?> hashMap) {
        this.hashMap = hashMap;
    }

    @Override
    public KeyIterator<K> iterator() {
        return new KeyIterator(hashMap);
    }

    @Override
    public int size() {
        return hashMap.entrySet().size();
    }

}
