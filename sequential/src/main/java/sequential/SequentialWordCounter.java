package sequential;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.Serializable;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Scanner;
 
public class SequentialWordCounter {

    // Entry

    public static class Entry implements Serializable {

        public static final long serialVersionUID = 202103050952L;

        String key;
        Integer value;

        public Entry(String key, Integer value) {
            this.key = key;
            this.value = value;
        }
    }

    // SortMethod

    public static enum SortMethod {
        COUNT, WORD;
    }

    // SortByCount

    public static class SortByCount implements Comparator<Entry> { 
        
        public int compare(Entry a, Entry b) { 
            if (a.value != b.value) {
                return b.value - a.value;
            }
            return a.key.compareToIgnoreCase(b.key);
        } 
    }

    // SortByWord

    public static class SortByWord implements Comparator<Entry> { 
        
        public int compare(Entry a, Entry b) {
            if (!a.key.equals(b.key)) {
                a.key.compareToIgnoreCase(b.key);
            }
            return b.value - a.value;
        } 
    } 

    public ArrayList<Entry> countWordsInFile(String filename, SortMethod sortMethod) {
        HashMap<String, Integer> wordCount = new HashMap<String, Integer>();

        File file = new File(filename);
        Scanner input = null;
        try {
            input = new Scanner(file);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            System.exit(-1);
        } 

        while (input.hasNext()) {
            String word  = input.next();
            wordCount.putIfAbsent(word, 0);
            wordCount.put(word, wordCount.get(word) + 1);
        }

        input.close();

        ArrayList<Entry> elements = new ArrayList<Entry>(wordCount.size());
        for (String key : wordCount.keySet()) {
            elements.add(new Entry(key, wordCount.get(key)));
        }

        switch (sortMethod) {
            case COUNT:
                elements.sort(new SortByCount());
                break;
            case WORD:
                elements.sort(new SortByWord());
                break;
        }

        return elements;
  }

}
