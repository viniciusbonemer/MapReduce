package deploy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

/**
 * Run the process created by process builders waiting the specified timeout.
 */
public class ProcessRunner implements Runnable {

    public interface ProcessCompletion {
        public void onComplete(Process process);
    }

    public static class AlreadyRunningException extends Exception {
        private static final long serialVersionUID = 202103261401L;
    }

    private boolean didStartRunning = false;

    private ArrayList<ProcessBuilder> processBuilders = new ArrayList<>();
    private ArrayList<Long> timeouts = new ArrayList<>();

    public ProcessCompletion onComplete = null;

    public ProcessRunner() {}

    public void addProcess(ProcessBuilder pb, long timeout) throws AlreadyRunningException {
        if (didStartRunning) {
            throw new AlreadyRunningException();
        }
        processBuilders.add(pb);
        timeouts.add(timeout);
    }
    
    @Override
    public void run() {
        didStartRunning = true;
        if (processBuilders.size() == 0) { return; }
        IntStream.range(0, processBuilders.size())
            .forEach(i -> runFromProcessBuilder(processBuilders.get(i), timeouts.get(i)));
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
                System.out.println("Timeout!");
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
            p.destroy();
            return;
        }

        if (onComplete == null) { return; }

        onComplete.onComplete(p);

        p.destroy();
    }

}
