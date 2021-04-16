package slave;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Scanner;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import slave.ProcessRunner.AlreadyRunningException;

public class App {

    //
    // MAIN
    //

    public static void main(String[] args) {

        if (args.length != 1 && args.length != 2) {
            System.err.println("Usage: \n\tjava App <mode>\n\tjava App <mode> <file-name>");
            System.exit(1);
        }

        String modeStr = args[0];

        int mode = 0;
        try {
            mode = Integer.parseInt(modeStr);
        } catch (NumberFormatException e) {
            System.err.println("Illegal argument \"" + modeStr + "\" for mode.");
            System.exit(1);
        }

        switch (mode) {
            case 0: {
                String fileName = args[1];
                App app = new App(fileName);
                app.createMapFromSplit();
                break;
            }
            case 1: {
                String fileName = args[1];
                App app = new App(fileName);
                app.createShuffleFromMap();
                break;
            }
            case 2: {
                App app = new App();
                app.createReduceFromShuffle();
                break;
            }
            default:
                System.err.println("Unexpected mode " + mode);
                System.exit(1);
        }
    }

    //
    // Static Methods
    //

    private static String getOutputMapName(String filePath) {
        String[] components = filePath.split("/");
        String name = components[components.length - 1].split("\\.")[0];
        Scanner in = new Scanner(name);
        in.useDelimiter("[^0-9]+");
        int number = in.nextInt();
        in.close();
        return Constants.mapsDir + "UM" + number + ".txt";
    }

    private static String getOutputShuffleName(String key) {
        String machineName = null;
        try {
            machineName = java.net.InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
        int hash = key.hashCode();
        return Constants.shufflesDir + Integer.toUnsignedString(hash) + "-" + machineName + ".txt";
    }

    private static String getFileForHash(String hash) {
        return Constants.reducesDir + hash + ".txt";
    }

    //
    // Properties
    //

    private String fileName;

    List<String> machines = new ArrayList<>();

    //
    // Constructor
    //

    private App() {
        this.fileName = null;
    }

    private App(String fileName) {
        this.fileName = fileName;
    }

    //
    // MAP
    //

    private void createMapDirectory() {
        ProcessBuilder pb = new ProcessBuilder("mkdir", "-p", Constants.mapsDir);
        try {
            Process p = pb.start();
            p.waitFor();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    private void createMapFromSplit() {
        createMapDirectory();
        File splitFile = new File(fileName);
        File outFile = new File(App.getOutputMapName(fileName));
        try {
            outFile.createNewFile();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
        FileWriter fw = null;
        BufferedWriter bw = null;

        try {
            fw = new FileWriter(outFile);
            bw = new BufferedWriter(fw);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }

        Scanner input = null;
        try {
            input = new Scanner(new BufferedReader(new FileReader(splitFile)));
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        } 

        while (input.hasNext()) {
            String word  = input.next();
            try {
                bw.write(word + " 1\n");
            } catch (Exception e) {
                e.printStackTrace();
                System.exit(1);
            }
        }

        try { input.close(); } catch (Exception e) { }
        try { bw.close(); } catch (Exception e) { }
        try { fw.close(); } catch (Exception e) { }
    }

    //
    // SHUFFLE
    //

    private void createShuffleFromMap() {
        this.machines = this.readReceivedMachinesFile();
        Set<String> shuffleFiles = this.prepareShuffleFiles();
        this.sendShuffleFiles(shuffleFiles);
    }

    private List<String> readReceivedMachinesFile() {
        FileReader fr = null;
        BufferedReader br = null;
        List<String> machines = new ArrayList<>();

        try {
            fr = new FileReader(Constants.machinesFile);
            br = new BufferedReader(fr);

            String machine = null;
            try {
                machine = br.readLine();
            } catch (Exception e) {
                e.printStackTrace();
                System.exit(1);
            }
            while (machine != null) {
                machines.add(machine);

                try {
                    machine = br.readLine();
                } catch (Exception e) {
                    e.printStackTrace();
                    System.exit(1);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        } finally {
            try { br.close(); } catch (Exception e) { }
            try { fr.close(); } catch (Exception e) { }
        }

        return machines;
    }

    private void createShufflesDirectory() {
        ProcessBuilder pb = new ProcessBuilder("mkdir", "-p", Constants.shufflesDir);
        try {
            Process p = pb.start();
            p.waitFor();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    private Set<String> prepareShuffleFiles() {
        Set<String> shuffleFiles = new HashSet<>();
        createShufflesDirectory();
        FileReader fr = null;
        BufferedReader br = null;

        try {
            fr = new FileReader(fileName);
            br = new BufferedReader(fr);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }

        String line = null;
        try {
            line = br.readLine();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
        while (line != null) {
            String word  = line.split(" ")[0];

            String shuffleName = App.getOutputShuffleName(word);
            shuffleFiles.add(shuffleName);

            FileWriter fw = null;
            BufferedWriter bw = null;
            try {
                fw = new FileWriter(shuffleName, true);
                bw = new BufferedWriter(fw);
            } catch (Exception e) {
                e.printStackTrace();
                System.exit(1);
            }

            try {
                bw.write(line + "\n");
            } catch (Exception e) {
                e.printStackTrace();
                System.exit(1);
            }

            try { bw.close(); } catch (Exception e) { }
            try { fw.close(); } catch (Exception e) { }

            try {
                line = br.readLine();
            } catch (Exception e) {
                e.printStackTrace();
                System.exit(1);
            }
        }

        try { br.close(); } catch (Exception e) { }
        try { fr.close(); } catch (Exception e) { }

        return shuffleFiles;
    }

    private Thread sendShuffleFilesToMachine(Set<String> files, String machine) {
        String login = Constants.username + "@" + machine;
        String mkdirCmd = Constants.mkdir + " " + Constants.receivedShufflesDir;
        ProcessBuilder mkdirBuilder = new ProcessBuilder(Constants.ssh, login, mkdirCmd);
        
        ProcessBuilder scpBuilder = null;
        if (files.size() != 0) {
            List<String> commands = new ArrayList<>(files.size() + 2);
            String dest = login + ":" + Constants.receivedShufflesDir;
            commands.add(Constants.scp);
            commands.addAll(files);
            commands.add(dest);
            scpBuilder = new ProcessBuilder(commands);
        }

        ProcessRunner runner = new ProcessRunner();
        try {
            runner.addProcess(mkdirBuilder, 4000);
            if (files.size() != 0) {
                runner.addProcess(scpBuilder, 4000);
            }
        } catch (AlreadyRunningException e) {
            System.err.println("This should never happen");
            System.exit(1);
        }
        Thread thread = new Thread(runner);
        thread.start();
        return thread;
    }

    private void sendShuffleFiles(Set<String> files) {
        List<Thread> threads = new ArrayList<>();

        for (int i = 0; i < machines.size(); ++i) {
            final int index = i;
            String machine = machines.get(i);
            Set<String> filteredFiles = files.stream()
                .filter(filePath -> {
                    // fileName: <dir_path>/<hash>-<machine_name>.txt
                    // escape / because of regex
                    String[] components = filePath.split("\\/");
                    String fileName = components[components.length - 1];
                    try {
                        int hash = Integer.parseInt(fileName.split("-")[0]);
                        return hash % machines.size() == index;
                    } catch (Exception e) {
                        return false;
                    }
                })
                .collect(Collectors.toSet());
            
            Thread t = sendShuffleFilesToMachine(filteredFiles, machine);
            threads.add(t);
        }

        threads.forEach((Thread t) -> {
            try {
                t.join();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

    //
    // REDUCE
    //

    private void createReduceFromShuffle() {
        createReducesDirectory();
        createReduces();
    }

    private void createReducesDirectory() {
        ProcessBuilder pb = new ProcessBuilder("mkdir", "-p", Constants.reducesDir);
        try {
            Process p = pb.start();
            p.waitFor();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    private SimpleEntry<String, Integer> reduceFile(String fileName) {
        int count = 0;
        String word = null;

        FileReader fr = null;
        BufferedReader br = null;

        try {
            fr = new FileReader(fileName);
            br = new BufferedReader(fr);

            for (String line = br.readLine(); line != null; line = br.readLine()) {
                String[] comps = line.split(" ");
                if (comps.length != 2) { continue; }
                if (word == null) {
                    word = comps[0];
                }
                count += 1;
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        } finally {
            try { br.close(); } catch (Exception e) {}
            try { fr.close(); } catch (Exception e) {}
        }

        return new SimpleEntry<String, Integer>(word, count);
    }

    private void createHashFile(String hash, String word, int count) {
        FileWriter fw = null;
        BufferedWriter bw = null;
        String hashFile = App.getFileForHash(hash);

        try {
            fw = new FileWriter(hashFile, true);
            bw = new BufferedWriter(fw);

            String line = word + " " + count + "\n";
            bw.write(line);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        } finally {
            try { bw.close(); } catch (Exception e) { }
            try { fw.close(); } catch (Exception e) { }
        }
    }

    private void createReduces() {

        // Get list of shuffles received
        List<Path> receivedShuffles = null;
        try (Stream<Path> paths = Files.walk(Paths.get(Constants.receivedShufflesDir))) {
            receivedShuffles = paths
                .filter(Files::isRegularFile)
                .collect(Collectors.toUnmodifiableList());
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }

        // Create list of hashes
        List<String> hashes = receivedShuffles.stream()
            .map(path -> path.getFileName().toString())
            .map(name -> name.split("-")[0])
            .distinct()
            .collect(Collectors.toUnmodifiableList());
        
        for (String hash : hashes) {
            List<String> files = receivedShuffles.stream()
                .filter(path -> path.getFileName().toString().startsWith(hash))
                .map(path -> path.toString())
                .collect(Collectors.toUnmodifiableList());
            SimpleEntry<String, Integer> pair = files.stream()
                .map(this::reduceFile)
                .reduce((result, value) -> {
                    if (result == null) {
                        return value;
                    } else {
                        return new SimpleEntry<String,Integer>(
                            value.getKey(), result.getValue() + value.getValue()
                            );
                    }
                }).get();
                
            // Write to file
            createHashFile(hash, pair.getKey(), pair.getValue());
        }

    }


}