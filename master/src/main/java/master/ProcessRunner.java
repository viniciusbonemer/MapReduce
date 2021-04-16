package master;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

/**
 * Run the process created by process builders waiting the specified timeout.
 * Processes run sequentially in the order they are added. 
 */
public class ProcessRunner implements Runnable {

    public interface ProcessCompletion {
        public void onComplete(Process process);
    }

    public static class AlreadyRunningException extends Exception {
        private static final long serialVersionUID = 202103261401L;
    }

    private boolean didStartRunning = false;

    // <DEBUG>
    private static int nextId = 0;
    private static synchronized int createNextId() {
        return nextId++;
    }

    public final int id;
    // </DEBUG>

    private ArrayList<ProcessBuilder> processBuilders = new ArrayList<>();
    private ArrayList<Long> timeouts = new ArrayList<>();

    public ProcessCompletion onComplete = null;

    public ProcessRunner() {this.id = ProcessRunner.createNextId();} // DEBUG

    public void addProcess(ProcessBuilder pb, long timeout) throws AlreadyRunningException {
        if (didStartRunning) {
            throw new AlreadyRunningException();
        }
        processBuilders.add(pb);
        timeouts.add(timeout);
    }
    
    @Override
    public void run() {
        synchronized (System.out) {
            System.out.println("[ProcessRunner" + id + "] Start"); // DEBUG
        }
        didStartRunning = true;
        if (processBuilders.size() == 0) { return; }
        IntStream.range(0, processBuilders.size())
            .forEach(i -> {
                runFromProcessBuilder(processBuilders.get(i), timeouts.get(i));
                System.out.println("[ProcessRunner" + id + "] Done " + ConnectionTester.getIdForProcessBuilder(processBuilders.get(i))); // DEBUG
            });
    }

    private void runFromProcessBuilder(ProcessBuilder pb, long timeout) {
        pb.redirectErrorStream(true);
        
        Process p = null;
        try {
            p = pb.start();
        } catch (IOException e) {
            System.err.println("Error starting process");
            System.exit(1);
        }

        boolean b;
        try {
            b = p.waitFor(timeout, TimeUnit.MILLISECONDS);
            if (!b) {
                System.out.println("[ProcessRunner" + id + "] Timeout " + ConnectionTester.getIdForProcessBuilder(pb)); // DEBUG
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        if (onComplete == null) { return; }

        onComplete.onComplete(p);

        p.destroy();
    }

}
