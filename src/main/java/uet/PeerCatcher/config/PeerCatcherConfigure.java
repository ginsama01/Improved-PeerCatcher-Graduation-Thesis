package uet.PeerCatcher.config;

public class PeerCatcherConfigure {
    public static int P2P_HOST_DETECTION_THRESHOLD_DEFAULT = 30;
    public static int P2P_HOST_DETECTION_THRESHOLD_NumberOfIPs = 0;
    public static int P2P_HOST_MERGING_THRESHOLD = 10;
    public static int FREQUENCY_THRESHOLD = 4;
    public static double MUTUAL_CONTACT_SCORE_THRESHOLD = 0;
    public static double LOUVAIN_COMMUNITY_DETECTION_RESOLUTION = 1.0;
    public static String ROOT_LOCATION = "0804/";
    public static double MMK_ATT_RATIO = 0.0;
    public static double[] BOTNET_DETECTION_THRESHOLD_BGP_SET = { 0.6 };
    public static double[] BOTNET_DETECTION_THRESHOLD_MCS_SET = { 0.13 };
    public static double INTERNAL_DEGREE_THRESHOLD = 1.5;
    public static double LOCAL_ASSORTATIVITY_THRESHOLD = 0.5;
}
