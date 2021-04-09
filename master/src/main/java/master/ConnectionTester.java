package master;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
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

        // Map each machine to a boolean indicating whether it's available
        Map<String, Boolean> results = testedMachines.stream()
            .collect(Collectors.toMap(
                machine -> machine,
                machine -> testMachine(machine, 4000))
            );
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

    private boolean testMachine(String name, long timeout) {
        ProcessBuilder pb = new ProcessBuilder(Constants.ssh, name, Constants.hostname);
        
        Process p = null;
        try {
            p = pb.start();
        } catch (IOException e) {
            System.err.println("[ConnectionTester] Error testing machine " + name);
            return false;
        }

        InputStream is = p.getInputStream();

        try {
            boolean completed = p.waitFor(timeout, TimeUnit.MILLISECONDS);
            if (!completed) {
                System.out.println("[ConnectionTester] Timeout reaching machine " + name);
                return false;
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
            p.destroy();
            return false;
        }

        BufferedReader br = new BufferedReader(new InputStreamReader(is));
        String line;

        try {
            // Output should have only one line
            line = br.readLine();
            return line.equals(name);
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        } finally {
            p.destroy();
        }
    }

    
}
