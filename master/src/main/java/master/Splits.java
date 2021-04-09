package master;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.PrintWriter;

public class Splits {

    // Properties

    final private String filePath;

    private int splitsCount;

    // Getters

    public int getSplitsCount() { return this.splitsCount; }

    // Constructors
    
    private Splits(String filePath, int splitCount) {
        this.filePath = filePath;
        this.splitsCount = splitCount;
    }

    public static Splits create(String filePath, int machineCount) {
        Splits splits = new Splits(filePath, machineCount);
        splits.prepare();
        return splits;
    }

    // Methods

    public String getFileForSplit(int i) {
        return Constants.splitDir + "S" + i + ".txt";
    }

    private void createSplitsDirectory() {
        ProcessBuilder pb = new ProcessBuilder("mkdir", "-p", Constants.splitDir);
        try {
            Process p = pb.start();
            p.waitFor();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    private void prepare() {
        createSplitsDirectory();

        FileReader fr = null;
        BufferedReader br = null;

        try {
            // Create file reader
            fr = new FileReader(filePath);
            br = new BufferedReader(fr);

            int i = 0;
            int lineCount = 0;
            String line = br.readLine();
            while (line != null) {
                // Distribute each line for a split
                String splitFile = getFileForSplit(i);
                FileWriter fw = null;
                BufferedWriter bw = null;
                PrintWriter pw = null;

                try {
                    fw = new FileWriter(splitFile, true);
                    bw = new BufferedWriter(fw);
                    pw = new PrintWriter(bw);

                    pw.println(line);
                } catch (Exception e) {
                    e.printStackTrace();
                    System.exit(1);
                } finally {
                    try { pw.close(); } catch (Exception e) { }
                    try { bw.close(); } catch (Exception e) { }
                    try { fw.close(); } catch (Exception e) { }
                }

                line = br.readLine();
                lineCount += 1;
                i = (i + 1) % splitsCount;
            }
            if (lineCount < this.splitsCount) {
                this.splitsCount = lineCount;
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        } finally {
            try { br.close(); } catch (Exception e) { }
            try { fr.close(); } catch (Exception e) { }
        }
    }

}
