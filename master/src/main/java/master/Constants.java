package master;

public class Constants {

    static final String username = "vbonemer";
    static final String basedir = "/tmp/" + username + "/";
    static final String ssh = "ssh";
    static final String scp = "scp";
    static final String mkdir = "mkdir -p";
    static final String cd = "cd";
    static final String hostname = "hostname";
    static final String runJar = "java -jar";
    static final String splitDir = basedir + "splits/";
    static final String mapsDir = basedir + "maps/";
    static final String machinesFile = basedir + "machines.txt";
    static final String reducesDir = basedir + "reduces/";
    static final String resultsDir = basedir + "results/";

    private Constants() { }
}
