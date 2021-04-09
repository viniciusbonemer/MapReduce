package deploy;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
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
        this.testedMachines = readFile();

        Map<String, Boolean> results = IntStream.range(0, testedMachines.size()).boxed()
            .collect(Collectors.toMap(
                i -> testedMachines.get(i),
                i -> false
            ));
        List<Thread> threads = results.entrySet().stream()
            .map(entry -> new AbstractMap.SimpleEntry<>(entry.getKey(), createTestMachine(entry.getKey(), 4000)))
            .map(entry -> {
                ProcessRunner runner = new ProcessRunner();
                try {
                    runner.addProcess(entry.getValue(), 4000);
                } catch (Exception e) {
                    System.err.println("This should never happen");
                    System.exit(1);
                }

                final String key = entry.getKey();
                runner.onComplete = p -> results.replace(key, verifyProcessOutput(p, key));

                return runner;
            })
            .map(runner -> new Thread(runner))
            .collect(Collectors.toList());
        
        threads.forEach(t -> t.start());
        threads.forEach(t -> {
            try {
                t.join();
            } catch (Exception e) {
                e.printStackTrace();
                System.exit(1);
            }
        });

        if (verbose) {
            // Print each machine's status
            results.forEach((machine, isAvailable) -> {
                String status = isAvailable ? "AVAILABLE" : "UNAVAILABLE";
                System.out.println("Machine " + machine + " " + status);
            });
        }
        // Filter only available machines
        this.availableMachines = results.entrySet().stream()
            .filter(entry -> entry.getValue())
            .map(entry -> entry.getKey())
            .collect(Collectors.toList());
    }

    private boolean verifyProcessOutput(Process p, String machineName) {
        InputStream is = p.getInputStream();
        BufferedReader br = new BufferedReader(new InputStreamReader(is));
        String line;

        try {
            // Output should have only one line
            line = br.readLine();
            return line.equals(machineName);
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

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
        }

        return machineList;
    }

    private ProcessBuilder createTestMachine(String name, long timeout) {
        return new ProcessBuilder(Constants.ssh, name, Constants.hostname);
    }

    
}
