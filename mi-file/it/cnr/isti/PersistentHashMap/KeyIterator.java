/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package it.cnr.isti.PersistentHashMap;
import java.util.Iterator;

/**
 *
 * @author Amato
 */
class KeyIterator<K> implements Iterator<K>{
    PersistentHashMap hp;
    EntryIterator ei;

     public KeyIterator(PersistentHashMap hp) {
        this.hp = hp;
        ei=hp.entrySet().iterator();
    }

    public boolean hasNext() {
        return ei.hasNext();
    }

    public K next() {
        return (K)ei.next().getKey();
    }

    public void remove() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

}
