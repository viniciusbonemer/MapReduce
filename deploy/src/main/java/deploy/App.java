package deploy;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.stream.Collectors;

import deploy.ProcessRunner.AlreadyRunningException;

public class App {

    public static void main(String[] args) {
        
        if (args.length != 2) {
            System.err.println("Usage: java App <machines-file> <jar-file>");
            System.exit(1);
        }

        String fileName = args[0];
        String jarFile = args[1];

        ConnectionTester connectionTester = new ConnectionTester(fileName);
        connectionTester.runTests(false);

        App app = new App(connectionTester.getAvailableMachines());
        app.deployJarToAll(jarFile);

        System.out.println("Finished deploying");
    }

    List<String> machines;

    private App(List<String> machines) {
        this.machines = machines;
    }

    public void deployJarToAll(String jarFile) {
        List<Thread> threads = machines.stream()
            .map(machine -> createProcessRunner(jarFile, machine))
            .map(runner -> new Thread(runner))
            .collect(Collectors.toUnmodifiableList());
        
        threads.forEach(thread -> thread.start());
        threads.forEach(thread -> {
            try {
                thread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });
    }

    private ProcessRunner createProcessRunner(String jarFile, String machine) {
        ProcessRunner runner = new ProcessRunner();
        runner.onComplete = (p) -> {
            InputStream is = p.getInputStream();
            BufferedReader br = new BufferedReader(new InputStreamReader(is));
            String line;

            try {
                while ((line = br.readLine()) != null) {
                    System.out.println("Standard line: " + line);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        };

        String login = Constants.username + "@" + machine;
        ProcessBuilder mkdirProcessBuilder = new ProcessBuilder(
            Constants.ssh, login, Constants.mkdir + " " + Constants.basedir
        );
        ProcessBuilder scpProcessBuilder = new ProcessBuilder(
            Constants.scp, jarFile, login + ":" + Constants.basedir
        );
        try {
            runner.addProcess(mkdirProcessBuilder, 10000);
            runner.addProcess(scpProcessBuilder, 10000);
        } catch (AlreadyRunningException e) {
            // This should never actually happen
            e.printStackTrace();
            System.exit(1);
        }

        return runner;
    }

}
