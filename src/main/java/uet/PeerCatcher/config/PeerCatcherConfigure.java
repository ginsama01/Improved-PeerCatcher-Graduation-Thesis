package uet.PeerCatcher.config;

public class PeerCatcherConfigure {
    public static int P2P_HOST_DETECTION_THRESHOLD_DEFAULT = 30;
    public static int P2P_HOST_DETECTION_THRESHOLD_NumberOfIPs = 0;
    public static int FREQUENCY_THRESHOLD = 4;
    public static double MUTUAL_CONTACT_SCORE_THRESHOLD = 0;
    public static double LOUVAIN_COMMUNITY_DETECTION_RESOLUTION = 1.0;
    public static String ROOT_LOCATION = "data/";
    public static double MMK_ATT_RATIO = 0.0;
    public static double[] BOTNET_DETECTION_THRESHOLD_BGP_SET = { 0.3 };
    public static double[] BOTNET_DETECTION_THRESHOLD_MCS_SET = { 0.12 };
    public static double INTERNAL_DEGREE_THRESHOLD = 0.8;
    public static double LOCAL_ASSORTATIVITY_THRESHOLD = 0.8;
    public static double COEFFICIENT_VARIATION_THRESHOLD = 0.08;
    public static int BYTE_PER_PACKET_THRESHOLD = 10;
}
