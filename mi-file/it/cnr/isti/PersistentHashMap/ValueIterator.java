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
class ValueIterator<V> implements Iterator<V>{
    PersistentHashMap hp;
    EntryIterator ei;

     public ValueIterator(PersistentHashMap hp) {
        this.hp = hp;
        ei=hp.entrySet().iterator();
    }

    public boolean hasNext() {
        return ei.hasNext();
    }

    public V next() {
        return (V)ei.next().getValue();
    }

    public void remove() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

}

