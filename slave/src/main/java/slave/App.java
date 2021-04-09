package slave;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Scanner;
import java.util.Set;
import java.util.stream.Collectors;

import slave.ProcessRunner.AlreadyRunningException;

public class App {

    //
    // MAIN
    //

    public static void main(String[] args) {

        if (args.length != 2) {
            System.err.println("Usage: java App <mode> <file-name>");
            System.exit(1);
        }

        String modeStr = args[0];
        String fileName = args[1];

        int mode = 0;
        try {
            mode = Integer.parseInt(modeStr);
        } catch (NumberFormatException e) {
            System.err.println("Illegal argument \"" + modeStr + "\" for mode.");
            System.exit(1);
        }

        App app = new App(fileName);
        switch (mode) {
            case 0:
                app.createMapFromSplit();
                break;
            case 1:
                app.machines = app.readReceivedMachinesFile();
                Set<String> shuffleFiles = app.prepareShuffleFiles();
                app.sendShuffleFiles(shuffleFiles);
                break;
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
        return Constants.shufflesDir + hash + "-" + machineName + ".txt";
    }

    //
    // Properties
    //

    private String fileName;

    List<String> machines = new ArrayList<>();

    //
    // Constructor
    //

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
            input = new Scanner(splitFile);
        } catch (FileNotFoundException e) {
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
                    int hash = Integer.parseInt(fileName.split("-")[0]);
                    return hash % machines.size() == index;
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

}