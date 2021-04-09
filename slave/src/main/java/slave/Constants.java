package slave;

public class Constants {

    static final String username = "vbonemer";
    static final String basedir = "/tmp/" + username + "/";
    static final String ssh = "ssh";
    static final String scp = "scp";
    static final String mkdir = "mkdir -p";
    static final String hostname = "hostname";
    static final String mapsDir = basedir + "maps/";
    static final String shufflesDir = basedir + "shuffles/";
    static final String machinesFile = basedir + "machines.txt";
    static final String receivedShufflesDir = basedir + "shufflesreceived/";

    private Constants() { }
}
