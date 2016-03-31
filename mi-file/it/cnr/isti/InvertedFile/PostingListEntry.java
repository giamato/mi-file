/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package it.cnr.isti.InvertedFile;

/**
 *
 * @author Amato
 */
public class PostingListEntry
    {
        private int object;
        private int score;

        public PostingListEntry(int o,int s)
        {
            object=o;
            score=s;
        }

        public int getObject()
        {
            return object;
        }

        public int getScore()
        {
            return score;
        }

        public String toString()
        {
            return "object: " + object + " score: " + score;
        }

    }
