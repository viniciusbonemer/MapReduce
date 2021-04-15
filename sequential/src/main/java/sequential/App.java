package sequential;

import java.util.ArrayList;

public class App {

    public static void main(String[] args) {
        if (args.length != 1) {
            System.err.println("Usage: java App <filename>");
            System.exit(1);
        }

        String filename = args[0];
        SequentialWordCounter counter = new SequentialWordCounter();

        long startTime = System.currentTimeMillis();

        ArrayList<SequentialWordCounter.Entry> count =  counter.countWordsInFile(
            filename, SequentialWordCounter.SortMethod.COUNT
        );

        long endTime = System.currentTimeMillis();
        long totalTime = endTime - startTime;

        for (int i = 0; i < count.size(); ++i) {
            // if (i == 50) { break; }
            SequentialWordCounter.Entry entry = count.get(i);
            System.out.println(entry.key + ": " + entry.value);
        }

        System.err.println("Total execution time: " + totalTime + "ms");
    }

}