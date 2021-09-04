import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.io.IOException;
import java.io.File;
import java.io.FileReader;
import java.io.BufferedReader;
import java.util.Scanner;

public class MapReduceFiles {

  public static void main(String[] args) {
long time = System.currentTimeMillis();

      
    if (args.length < 4) {
      System.err.println("usage: java MapReduceFiles file1.txt file2.txt file3.txt");

    }

    Map<String, String> input = new HashMap<String, String>();
     Map<String, String> input2 = new HashMap<String, String>();
    try {
      input.put(args[0], readFile(args[0]));
      input.put(args[1], readFile(args[1]));
      input.put(args[2], readFile(args[2]));
      
    }
    catch (IOException ex)
    {
        System.err.println("Error reading files...\n" + ex.getMessage());
        ex.printStackTrace();
        System.exit(0);
    }

    // APPROACH #3: Distributed MapReduce
    {
       int tr=0;
       int tr2=0;
      final int size = Integer.parseInt(args[3]);
      final Map<String, Map<String, Integer>> output = new HashMap<String, Map<String, Integer>>();

      // MAP: value, file output (key/value pairs)

      final List<MappedItem> mappedItems = new LinkedList<MappedItem>();

      final MapCallback<String, MappedItem> mapCallback = new MapCallback<String, MappedItem>() {
        @Override
        public synchronized void mapDone(String file, List<MappedItem> results) {
          mappedItems.addAll(results);
        }
      };
      
      

      List<Thread> mapCluster = new ArrayList<Thread>(input.size());
      
      Iterator<Map.Entry<String, String>> inputIter = input.entrySet().iterator();
      while(inputIter.hasNext()) {
        Map.Entry<String, String> entry = inputIter.next();
        final String file = entry.getKey();
        final String contents = entry.getValue();
        
        
          final  int length = contents.length();
         final List<String> parts = new ArrayList<>(size);
          
           for (int i = 0; i < length; i += size) {
            parts.add(contents.substring(i, Math.min(length, i + size)));
        }
          
          for(int x=0;x<parts.size();x++){
              //if i mod size == 0 start new thread
              //start thread
              //while i mod size not = 0
              tr++;
              final String c = parts.get(x);
        Thread t = new Thread(new Runnable() {
          @Override
          public void run() {
            map(file,c, mapCallback);
          }
        });
        mapCluster.add(t);
        t.start();
      }
      }
      // wait for mapping phase to be over:
      for(Thread t : mapCluster) {
        try {
          t.join();
        } catch(InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
      
     long mphase = System.currentTimeMillis()-time;
         
      // GROUP: map that contains key(word) and value(files containing word)

      Map<String, List<String>> groupedItems = new HashMap<String, List<String>>();

      Iterator<MappedItem> mappedIter = mappedItems.iterator();
      while(mappedIter.hasNext()) {
        MappedItem item = mappedIter.next();
        String word = item.getWord();
        String file = item.getFile();
        List<String> list = groupedItems.get(word);
        if (list == null) {
          list = new LinkedList<String>();
          groupedItems.put(word, list);
        }
        list.add(file);
      }
      
      long gphase = System.currentTimeMillis()-time + mphase;

      // REDUCE: get count reduce to final format word file and index

      final ReduceCallback<String, String, Integer> reduceCallback = new ReduceCallback<String, String, Integer>() {
        @Override
        public synchronized void reduceDone(String k, Map<String, Integer> v) {
          output.put(k, v);
        }
      };
      final ArrayList<String> words = new ArrayList();
      final ArrayList<String> words2 = new ArrayList();
      final List<List<String>>list2 = new ArrayList();
      final List<List<String>>list3 = new ArrayList();
      final int size2 = Integer.parseInt(args[4]);
      
      List<Thread> reduceCluster = new ArrayList<Thread>(groupedItems.size());
     

      Iterator<Map.Entry<String, List<String>>> groupedIter = groupedItems.entrySet().iterator();
      while(groupedIter.hasNext()) {
        Map.Entry<String, List<String>> entry = groupedIter.next();
        final String word = entry.getKey();
        words.add(word);
        final List<String> list = entry.getValue();
        list2.add(list);
        
      }
       for(int j=0;j<words.size();j++){
            //final String w = words.get(i);
            //final List<String> li = list2.get(i);
            words2.add(words.get(j));
            list3.add(list2.get(j));
     
            
        if(j%size2 == 0){
            tr2++;
        Thread t = new Thread(new Runnable() {
          @Override
          public void run() {
              for(int i = 0; i<words2.size();i++){
            
                   final String w = words2.get(i);
                   final List<String> li = list3.get(i);
            reduce(w, li, reduceCallback);
              }
              words2.clear();
              list3.clear();
              }
          
        });
        reduceCluster.add(t);
        t.start();
      
        }
      }
      // wait for reducing phase to be over:
      for(Thread t : reduceCluster) {
        try {
          t.join();
        } catch(InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
      

      System.out.println(output);
      
      long rphase = System.currentTimeMillis()-time + gphase + mphase;
            System.out.println("\n");
            System.out.println("Mapping phase took: " + mphase+ " m/s");
            System.out.println("Grouping phase took: " + gphase+ " m/s");
            System.out.println("Reducing phase took: " + rphase+ " m/s");
            System.out.println("number of mapping threads: " + tr);
            System.out.println("number of reducing threads: " + tr2);
            System.out.println("\n");
    }
  }

  public static void map(String file, String contents, List<MappedItem> mappedItems) {
    String[] words = contents.trim().split("\\s+");
    for(String word: words) {
        if(word.contains(";")){
            word = word.replace(";","");
        }
        if(word.contains(",")){
            word = word.replace(",","");
        }
        if(word.contains(".")){
            word = word.replace(".","");
        }
        
        if(word.contains("/?")){
            word = word.replace("/?","");
        }
        /*
       if(word.contains("_")){
            word = word.replaceAll("_","");
        }*/
        if(word.contains("!")){
            word = word.replace("!","");
        }
         if(word.contains("\"")){
            word = word.replace("\"","");
        }
        if(word.contains(":")){
            word = word.replace(":","");
        }        
        
      mappedItems.add(new MappedItem(word, file));
    }
  }

  public static void reduce(String word, List<String> list, Map<String, Map<String, Integer>> output) {
    Map<String, Integer> reducedList = new HashMap<String, Integer>();
    for(String file: list) {
      Integer occurrences = reducedList.get(file);
      if (occurrences == null) {
        reducedList.put(file, 1);
      } else {
        reducedList.put(file, occurrences.intValue() + 1);
      }
    }
    output.put(word, reducedList);
  }

  public static interface MapCallback<E, V> {

    public void mapDone(E key, List<V> values);
  }

  public static void map(String file, String contents, MapCallback<String, MappedItem> callback) {
    String[] words = contents.trim().split("\\s+");
    List<MappedItem> results = new ArrayList<MappedItem>(words.length);
    for(String word: words) {
        if(word.contains(";")){
            word = word.replace(";","");
        }
        if(word.contains(",")){
            word = word.replace(",","");
        }
        if(word.contains(".")){
            word = word.replace(".","");
        }
        
       if(word.contains("/?")){
            word = word.replace("/?","");
        }
        if(word.contains("â??")){
            word = word.replaceAll("â??","");
        }
        /*if(word.contains("_")){
            word = word.replaceAll("_","");
        }*/
                if(word.contains("!")){
            word = word.replace("!","");
        }
        if(word.contains(":")){
            word = word.replace(":","");
        }
      results.add(new MappedItem(word, file));
    }
    callback.mapDone(file, results);
  }

  public static interface ReduceCallback<E, K, V> {

    public void reduceDone(E e, Map<K,V> results);
  }

  public static void reduce(String word, List<String> list, ReduceCallback<String, String, Integer> callback) {

    Map<String, Integer> reducedList = new HashMap<String, Integer>();
    for(String file: list) {
      Integer occurrences = reducedList.get(file);
      if (occurrences == null) {
        reducedList.put(file, 1);
      } else {
        reducedList.put(file, occurrences.intValue() + 1);
      }
    }
    callback.reduceDone(word, reducedList);
  }

  private static class MappedItem {

    private final String word;
    private final String file;

    public MappedItem(String word, String file) {
      this.word = word;
      this.file = file;
    }

    public String getWord() {
      return word;
    }

    public String getFile() {
      return file;
    }

    @Override
    public String toString() {
      return "[\"" + word + "\",\"" + file + "\"]";
    }
  }

  private static String readFile(String pathname) throws IOException {
    File file = new File(pathname);
    StringBuilder fileContents = new StringBuilder((int) file.length());
    Scanner scanner = new Scanner(new BufferedReader(new FileReader(file)));
    String lineSeparator = System.getProperty("line.separator");

    try {
      if (scanner.hasNextLine()) {
        fileContents.append(scanner.nextLine());
      }
      while (scanner.hasNextLine()) {
        fileContents.append(lineSeparator + scanner.nextLine());
      }
      return fileContents.toString();
    } finally {
      scanner.close();
    }
  }

}