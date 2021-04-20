package master;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;

public class Splits {

    // Properties

    final private String filePath;

    final long minSplitSize = 1;

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

        File file = null;
        BufferedReader reader = null;
        Long fileSize = null;
        
        try {
            file = new File(filePath);
            reader = new BufferedReader(new FileReader(file));
            fileSize = file.length();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }

        // Normally, the splits will have similar size `approxSplitSize`. If this generates too small splits \
        // the number of splits is recalculated so that the splits have a size close to `minSplitSize`
        long approxSplitSize = fileSize / splitsCount;
        if (approxSplitSize < minSplitSize) {
            Double count = Math.ceil(fileSize / minSplitSize);
            this.splitsCount = count.intValue();
            approxSplitSize = fileSize / splitsCount;
        }

        for (int i = 0; i < splitsCount; ++i) {
            BufferedWriter writer = null;

            // Create a buffer
            Long minimum = Math.min(50, approxSplitSize);
            int numBytes = minimum.intValue();
            char[] buff = new char[numBytes];
            long bytesRead = 0;
            String splitFile = getFileForSplit(i);

            try {
                writer = new BufferedWriter(new FileWriter(splitFile));
                int lastChar = 0;
                // Read at least `approxSplitSize` using `numBytes` chunks and write to split file
                while (bytesRead < approxSplitSize) {
                    int readCount = reader.read(buff, 0, numBytes);
                    if (readCount == -1) {
                        lastChar = 0;
                        break;
                    }
                    writer.write(buff, 0, readCount);
                    bytesRead += readCount;
                    lastChar = buff[readCount - 1];
                }
                // Read until we find a ' ' or '\n' to avoid spliting words in half
                int extraChars = 0;
                while (lastChar != ' ' && lastChar != '\n' && lastChar != 0) {
                    lastChar = reader.read();
                    buff[extraChars] = (char) lastChar;
                    extraChars++;
                    // Avoid overflowing buff if a file has a huge word
                    if (extraChars == numBytes) {
                        writer.write(buff, 0, extraChars);
                        extraChars = 0;
                    }
                }
                // Writing the extra characters until the whitespace
                writer.write(buff, 0, extraChars);

            } catch (Exception e) {
                e.printStackTrace();
                System.exit(1);
            } finally {
                try { writer.close(); } catch (Exception e) { }
            }
        }
        try { reader.close(); } catch (Exception e) { }
    }

}
