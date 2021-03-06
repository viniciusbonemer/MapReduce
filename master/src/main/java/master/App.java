package master;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.lang.Math;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import master.ProcessRunner.AlreadyRunningException;

public class App {

    static final class MeasuredTime {

        final long s;
        final long ms;
        final long us;
        final long ns;

        MeasuredTime(long nanoSeconds) {
            long time = nanoSeconds;
            this.s = time / 1000000000;
            time = time % 1000000000;
            this.ms = time / 1000000;
            time = time % 1000000;
            this.us = time / 1000;
            this.ns = time % 1000;
        }
    }

    private final static String jarName = "slave-0.1.jar";

    private static String getMapFileFromSplit(String filePath) {
        String[] components = filePath.split("/");
        String name = components[components.length - 1].split("\\.")[0];
        Scanner in = new Scanner(name);
        in.useDelimiter("[^0-9]+");
        int number = in.nextInt();
        in.close();
        return Constants.mapsDir + "UM" + number + ".txt";
    }

    public static void log(String message, MeasuredTime t) {
        String ts = " in " + t.s + "s " + t.ms + "ms " + t.us + "us " + t.ns + "ns";
        System.err.println(message + ts);
    }

    public static void main(String[] args) {

        if (args.length != 2) {
            System.err.println("Usage: java App <machines-file> <input-file>");
            System.exit(1);
        }

        String machinesFileName = args[0];
        String inputFile = args[1];

        System.out.println("[Starting] Machines File: " + machinesFileName);
        System.out.println("[Starting] Input File: " + inputFile);

        ConnectionTester connectionTester = new ConnectionTester(machinesFileName);
        connectionTester.runTests();
        
        List<String> machines = connectionTester.getAvailableMachines();
        if (machines.size() == 0) {
            System.err.println("Unable to reach remote machines");
            System.exit(1);
        }
        App app = new App(connectionTester.getAvailableMachines(), inputFile);

        // MAP

        System.out.println("[App] <Starting> MAP");
        // System.out.println("[App] <Creating Threads> MAP");
        List<ProcessRunner> runners = app.createMapRunners();

        List<Thread> threads = runners.stream()
            .map(runner -> new Thread(runner))
            .collect(Collectors.toUnmodifiableList());
        
        long startTime = System.nanoTime();

        // System.out.println("[App] <Starting Threads> MAP");
        threads.forEach(thread -> thread.start());
        // System.out.println("[App] <Joining Threads> MAP");
        threads.forEach(thread -> {
            try {
                thread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });

        long endTime = System.nanoTime();
        long duration = (endTime - startTime);
        MeasuredTime mt = new MeasuredTime(duration);
        
        App.log("MAP FINISHED", mt);
        // System.out.println("[App] <Done> MAP");

        // SHUFFLE

        System.out.println("[App] <Starting> SHUFFLE");
        // System.out.println("[App] <Creating Threads> SHUFFLE");

        runners = app.createShuffleRunners();

        threads = runners.stream()
            .map(runner -> new Thread(runner))
            .collect(Collectors.toUnmodifiableList());
        
        startTime = System.nanoTime();

        // System.out.println("[App] <Starting Threads> SHUFFLE");
        threads.forEach(thread -> thread.start());
        // System.out.println("[App] <Joining Threads> SHUFFLE");
        threads.forEach(thread -> {
            try {
                thread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });
        

        endTime = System.nanoTime();
        duration = (endTime - startTime);
        mt = new MeasuredTime(duration);
        
        App.log("SHUFFLE FINISHED", mt);
        // System.out.println("[App] <Done> SHUFFLE");

        // REDUCE

        System.out.println("[App] <Starting> REDUCE");
        // System.out.println("[App] <Creating Threads> REDUCE");
        runners = app.createReduceRunners();

        threads = runners.stream()
            .map(runner -> new Thread(runner))
            .collect(Collectors.toUnmodifiableList());
        
        startTime = System.nanoTime();

        // System.out.println("[App] <Starting Threads> REDUCE");
        threads.forEach(thread -> thread.start());
        // System.out.println("[App] <Joining Threads> REDUCE");
        threads.forEach(thread -> {
            try {
                thread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });

        endTime = System.nanoTime();
        duration = (endTime - startTime);
        mt = new MeasuredTime(duration);
        
        App.log("REDUCE FINISHED", mt);
        // System.out.println("[App] <Done> REDUCE");

        // RETRIEVE

        System.out.println("[App] <Starting> RETRIEVE");
        // System.out.println("[App] <Creating Threads> RETRIEVE");
        runners = app.createRetrieveResultsRunners();

        threads = runners.stream()
            .map(runner -> new Thread(runner))
            .collect(Collectors.toUnmodifiableList());
        
        startTime = System.nanoTime();

        // System.out.println("[App] <Starting Threads> RETRIEVE");
        threads.forEach(thread -> thread.start());
        // System.out.println("[App] <Joining Threads> RETRIEVE");
        threads.forEach(thread -> {
            try {
                thread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });

        System.out.println("[App] <Merging files> RETRIEVE");
        app.createResultsFile();

        endTime = System.nanoTime();
        duration = (endTime - startTime);
        mt = new MeasuredTime(duration);
        
        App.log("RETRIEVE FINISHED", mt);
        // System.out.println("[App] <Done> RETRIEVE");

        // app.printResults();
        System.out.println("[Stopping]");
    }

    private List<String> machines;
    private List<String> usedMachines = new ArrayList<String>();

    private Splits splits;

    private App(List<String> machines, String filePath) {
        this.machines = machines;
        this.splits = Splits.create(filePath, machines.size());
    }

    private String createUsedMachinesFile() {
        String filePath = Constants.basedir + "used_machines.txt";
        File usedMachinesFile = new File(filePath);
        try {
            usedMachinesFile.createNewFile();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
        FileWriter fw = null;
        BufferedWriter bw = null;

        try {
            fw = new FileWriter(usedMachinesFile);
            bw = new BufferedWriter(fw);

            for (String machine : usedMachines) {
                bw.write(machine + "\n");
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        } finally {
            try { bw.close(); } catch (Exception e) {}
            try { fw.close(); } catch (Exception e) {}
        }

        return filePath;
    }

    private List<ProcessRunner> createMapRunners() {
        ArrayList<ProcessRunner> runners = new ArrayList<>(splits.getSplitsCount());
        this.usedMachines = machines.subList(0, Math.min(splits.getSplitsCount(), machines.size()));
        String usedMachinesFileName = createUsedMachinesFile();
        for (int i = 0; i < splits.getSplitsCount(); ++i) {
            final int machineIndex = i % machines.size();
            final String machine = machines.get(machineIndex);
            final String login = Constants.username + "@" + machine;
            final String mkdir = Constants.mkdir + " " + Constants.splitDir;
            final String localFile = splits.getFileForSplit(i);
            final String[] comps = localFile.split("/");
            final String name = comps[comps.length - 1];
            final String splitFile = Constants.splitDir + name;

            ProcessBuilder mkdirBuilder = new ProcessBuilder(Constants.ssh, login, mkdir);
            ProcessBuilder scpSplitBuilder = new ProcessBuilder(
                Constants.scp, localFile, login + ":" + splitFile
            );
            ProcessBuilder scpMachinesBuilder = new ProcessBuilder(
                Constants.scp, usedMachinesFileName, login + ":" + Constants.machinesFile
            );
            
            final String cmd1 = Constants.cd + " " + Constants.basedir;
            final String cmd2 = Constants.runJar + " " + jarName + " 0 " + splitFile;
            final String command = cmd1 + "; " + cmd2;
            ProcessBuilder execBuilder = new ProcessBuilder(Constants.ssh, login, command);
            
            // DEBUG
            String id = "MAP"+ConnectionTester.createNextId();
            mkdirBuilder.environment().put(ConnectionTester.IDKEY, id + " - mkdir");
            scpSplitBuilder.environment().put(ConnectionTester.IDKEY, id + " - scp splits");
            scpMachinesBuilder.environment().put(ConnectionTester.IDKEY, id + " - scp machines");
            execBuilder.environment().put(ConnectionTester.IDKEY, id + " - exec");
            //

            ProcessRunner runner = new ProcessRunner();
            try {
                runner.addProcess(mkdirBuilder, Integer.MAX_VALUE);
                runner.addProcess(scpSplitBuilder, Integer.MAX_VALUE);
                runner.addProcess(scpMachinesBuilder, Integer.MAX_VALUE);
                runner.addProcess(execBuilder, Integer.MAX_VALUE);
            } catch (AlreadyRunningException e) {
                System.err.println("This should never happen");
                System.exit(1);
            }
            runners.add(runner);
        }

        return runners;
    }

    // Shuffle

    private List<ProcessRunner> createShuffleRunners() {
        ArrayList<ProcessRunner> runners = new ArrayList<>(splits.getSplitsCount());
        for (int i = 0; i < usedMachines.size(); ++i) {
            // For each machine
            // Run jar with 1 and map filename
            final String machine = usedMachines.get(i);
            final String login = Constants.username + "@" + machine;

            final String localFile = splits.getFileForSplit(i);
            final String[] comps = localFile.split("/");
            final String name = comps[comps.length - 1];
            final String splitFile = Constants.splitDir + name;
            final String mapFile = App.getMapFileFromSplit(splitFile);
            
            final String cmd1 = Constants.cd + " " + Constants.basedir;
            final String cmd2 = Constants.runJar + " " + jarName + " 1 " + mapFile;
            final String command = cmd1 + "; " + cmd2;
            ProcessBuilder execBuilder = new ProcessBuilder(Constants.ssh, login, command);
            
            String id = "SHUFFLE"+ConnectionTester.createNextId();
            execBuilder.environment().put(ConnectionTester.IDKEY, id + " - exec");

            ProcessRunner runner = new ProcessRunner();
            try {
                runner.addProcess(execBuilder, Integer.MAX_VALUE);
            } catch (AlreadyRunningException e) {
                System.err.println("This should never happen");
                System.exit(1);
            }
            runners.add(runner);
        }

        return runners;
    }

    // Reduce

    private List<ProcessRunner> createReduceRunners() {
        ArrayList<ProcessRunner> runners = new ArrayList<>(splits.getSplitsCount());
        for (int i = 0; i < usedMachines.size(); ++i) {
            // For each machine
            // Run jar with 1 and map filename
            final String machine = usedMachines.get(i);
            final String login = Constants.username + "@" + machine;
            
            final String cmd1 = Constants.cd + " " + Constants.basedir;
            final String cmd2 = Constants.runJar + " " + jarName + " 2";
            final String command = cmd1 + "; " + cmd2;
            ProcessBuilder execBuilder = new ProcessBuilder(Constants.ssh, login, command);
            
            String id = "REDUCE"+ConnectionTester.createNextId();
            execBuilder.environment().put(ConnectionTester.IDKEY, id + " - exec");
            
            ProcessRunner runner = new ProcessRunner();
            try {
                runner.addProcess(execBuilder, Integer.MAX_VALUE);
            } catch (AlreadyRunningException e) {
                System.err.println("This should never happen");
                System.exit(1);
            }
            runners.add(runner);
        }

        return runners;
    }

    // Retrieve results

    private List<ProcessRunner> createRetrieveResultsRunners() {
        ArrayList<ProcessRunner> runners = new ArrayList<>(splits.getSplitsCount());
        for (int i = 0; i < usedMachines.size(); ++i) {
            final String machine = usedMachines.get(i);
            final String login = Constants.username + "@" + machine;
            
            ProcessBuilder mkdirResultsBuilder = new ProcessBuilder("mkdir", "-p", Constants.resultsDir);
            
            final String machineResultDir = Constants.resultsDir + machine + "/";
            final String origin = login + ":" + Constants.reducesDir;
            final String dest = machineResultDir;
            ProcessBuilder scpBuilder = new ProcessBuilder(Constants.scp, "-r", origin, dest);

            String id = "RETIEVE"+ConnectionTester.createNextId();
            mkdirResultsBuilder.environment().put(ConnectionTester.IDKEY, id + " - mkdir");
            scpBuilder.environment().put(ConnectionTester.IDKEY, id + " - scp");
            
            ProcessRunner runner = new ProcessRunner();
            try {
                runner.addProcess(mkdirResultsBuilder, Integer.MAX_VALUE);
                runner.addProcess(scpBuilder, Integer.MAX_VALUE);
            } catch (AlreadyRunningException e) {
                System.err.println("This should never happen");
                System.exit(1);
            }
            runners.add(runner);
        }

        return runners;
    }

    private void appendReultsFromFile(String origin, String dest) {
        FileReader fr = null;
        BufferedReader br = null;

        FileWriter fw = null;
        BufferedWriter bw = null;
        PrintWriter pw = null;

        try {
            fr = new FileReader(origin);
            br = new BufferedReader(fr);

            fw = new FileWriter(dest, true);
            bw = new BufferedWriter(fw);
            pw = new PrintWriter(bw);

            String result = br.readLine();
            if (result == null) { return; }

            pw.println(result);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        } finally {
            try { pw.close(); } catch (Exception e) { }
            try { bw.close(); } catch (Exception e) { }
            try { fw.close(); } catch (Exception e) { }

            try { br.close(); } catch (Exception e) { }
            try { fr.close(); } catch (Exception e) { }
        }
    }

    private void createResultsFile() {
        for (int i = 0; i < usedMachines.size(); ++i) {
            final String resultsFile = Constants.resultsDir + "results.txt";
            final String machine = usedMachines.get(i);
            final String resultsDir = Constants.resultsDir + machine;

            List<Path> machineResults = null;
            try (Stream<Path> paths = Files.walk(Paths.get(resultsDir))) {
                machineResults = paths
                    .filter(Files::isRegularFile)
                    .collect(Collectors.toUnmodifiableList());
            } catch (Exception e) {
                e.printStackTrace();
                System.exit(1);
            }

            for (Path path : machineResults) {
                appendReultsFromFile(path.toString(), resultsFile);
            }
        }
    }

    private void printResults() {
        final String resultsFile = Constants.resultsDir + "results.txt";
        
        FileReader fr = null;
        BufferedReader br = null;

        try {
            fr = new FileReader(resultsFile);
            br = new BufferedReader(fr);

            String line = null;
            while ((line = br.readLine()) != null) {
                System.out.println(line);
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
