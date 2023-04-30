package uet.PeerCatcher.config;

public class PeerCatcherConfigure {
    public static String ROOT_LOCATION = "data/";
    public static int BYTE_PER_PACKET_THRESHOLD = 10;
    public static int P2P_HOST_DETECTION_THRESHOLD_DEFAULT = 30;
    public static int FREQUENCY_THRESHOLD = 4;
    public static double LOUVAIN_COMMUNITY_DETECTION_RESOLUTION = 1.0;
    public static double BOTNET_DETECTION_THRESHOLD_BGP = 0.3;
    public static double BOTNET_DETECTION_THRESHOLD_MCS = 0.12;
    public static double BOTNET_DETECTION_THRESHOLD_INTERNAL_DEGREE = 0.8;
    public static double BOTNET_DETECTION_THRESHOLD_LOCAL_ASSOCIATIVITY = 0.8;
    public static double BOTNET_DETECTION_THRESHOLD_COEFFICIENT_VARIATION = 0.08;

}
