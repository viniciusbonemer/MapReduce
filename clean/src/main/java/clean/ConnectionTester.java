package clean;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.InputStreamReader;

public class ConnectionTester {

    // Properties

    public final String fileName;

    private static final int timeout = 5000;

    private List<String> testedMachines = new ArrayList<>();

    private List<String> availableMachines = new ArrayList<>();

    // Initializers

    public ConnectionTester(String fileName) {
        this.fileName = fileName;
    }

    // Getters

    public List<String> getAllMachines() {
        return Collections.unmodifiableList(this.testedMachines);
    }

    public List<String> getAvailableMachines() {
        return Collections.unmodifiableList(this.availableMachines);
    }

    // Methods

    public void runTests() {
        runTests(false);
    }

    public void runTests(boolean verbose) {
        ExecutorService executor = Executors.newFixedThreadPool(10);

        this.testedMachines = readFile();
        // Map each machine to a boolean indicating whether it's available
        List<Boolean> results = this.testedMachines.stream()
            .map(machine -> createProcessRunner(machine, ConnectionTester.timeout))
            .map(runner -> executor.submit(runner))
            .map(future -> {
                try {
                    return future.get();
                } catch (Exception e) {
                    return false;
                }
            })
            .collect(Collectors.toList());
        
        // All threads will have finished due to future.get() above
        executor.shutdownNow();

        // Zip results
        Map<String, Boolean> resultsMap = IntStream.range(0, testedMachines.size())
            .boxed()
            .collect(Collectors.toMap(testedMachines::get, results::get));
        if (verbose) {
            // Print each machine's status
            resultsMap.forEach((machine, isAvailable) -> {
                String status = isAvailable ? "AVAILABLE" : "UNAVAILABLE";
                System.out.println("Machine " + machine + " " + status);
            });
        } else {
            resultsMap.entrySet().stream()
                .filter(entry -> !entry.getValue())
                .forEach(entry -> System.err.println("Error connecting to machine " + entry.getKey()));
        }
        // Filter only available machines
        this.availableMachines = resultsMap.entrySet().stream()
            .filter(entry -> entry.getValue())
            .map(entry -> entry.getKey())
            .collect(Collectors.toList());
    }

    /**
     * Reads the file `fileName` and return the a list of machines
     */
    private ArrayList<String> readFile() {
        FileReader fReader = null;
        BufferedReader reader = null;
        String line = null;
        ArrayList<String> machineList = new ArrayList<>(10);

        try {
            fReader = new FileReader(fileName);
        } catch (FileNotFoundException e) {
            System.err.println("File " + fileName + " could not be opened.");
            e.printStackTrace();
            System.exit(1);
        }

        reader = new BufferedReader(fReader);

        try {
            while ((line = reader.readLine()) != null) {
                machineList.add(line);
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        } finally {
            try { reader.close(); } catch (Exception e) { e.printStackTrace(); }
            try { fReader.close(); } catch (Exception e) { e.printStackTrace(); }
        }

        return machineList;
    }

    private ProcessRunner createProcessRunner(String machine, long timeout) {
        ProcessBuilder pb = new ProcessBuilder(Constants.ssh, machine, Constants.hostname);
        ProcessRunner runner = new ProcessRunner(pb, timeout);
        
        runner.onComplete = process -> {
            InputStream is = process.getInputStream();
            BufferedReader br = new BufferedReader(new InputStreamReader(is));
            String line;

            try {
                // Output should have only one line
                line = br.readLine();
                return line.equals(machine);
            } catch (IOException e) {
                e.printStackTrace();
                return false;
            }
        };

        return runner;
    }

    
}
