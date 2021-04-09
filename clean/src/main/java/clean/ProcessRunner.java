package clean;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * Run the process created by process builders waiting the specified timeout.
 */
public class ProcessRunner implements Callable<Boolean> {

    public interface ProcessCompletion {
        public boolean onComplete(Process process);
    }

    private ProcessBuilder processBuilder;

    private long timeout;

    public ProcessCompletion onComplete = null;

    public ProcessRunner(ProcessBuilder processBuilder, long timeout) {
        this.processBuilder = processBuilder;
        this.timeout = timeout;
    }

    @Override
    public Boolean call() throws Exception {
        processBuilder.redirectErrorStream(true);
        
        Process p = null;
        try {
            p = processBuilder.start();
        } catch (IOException e) {
            System.err.println("Error starting process");
            System.exit(1);
            return false;
        }

        boolean b;
        try {
            b = p.waitFor(timeout, TimeUnit.MILLISECONDS);
            if (!b) {
                System.out.println("Timeout!");
                return false;
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
            p.destroy();
            return false;
        }

        if (onComplete == null) { return false; }

        boolean res = onComplete.onComplete(p);
        p.destroy();
        return res;
    }

}

