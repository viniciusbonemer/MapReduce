package clean;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class App {

    public static void main(String[] args) {
        if (args.length != 1) {
            System.err.println("Usage: java App <machines-file>");
            System.exit(1);
        }

        String fileName = args[0];

        ConnectionTester connectionTester = new ConnectionTester(fileName);
        connectionTester.runTests(false);
        System.out.println("Done testing");

        App app = new App(connectionTester.getAvailableMachines());
        app.cleanAllMachines();

        System.out.println("Finished cleaning");
    }

    List<String> machines;

    private App(List<String> machines) {
        this.machines = machines;
    }

    private void cleanAllMachines() {
        ExecutorService executor = Executors.newFixedThreadPool(10);
        machines.stream()
            .map(this::createCleanBuilderForMachine)
            .map(pb -> new ProcessRunner(pb, 4000))
            .map(runner -> executor.submit(runner))
            .forEach(future -> {
                try {
                    future.get();
                } catch (Exception e) {
                    return;
                }
            });
        // All futures completed
        executor.shutdownNow();
    }

    private ProcessBuilder createCleanBuilderForMachine(String machine) {
        String login = Constants.username + "@" + machine;
        return new ProcessBuilder(Constants.ssh, login, Constants.rm + " " + Constants.basedir);
    }
    
}
