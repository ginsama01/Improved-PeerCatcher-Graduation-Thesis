package uet.PeerCatcher.mcg;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import uet.PeerCatcher.config.PeerCatcherConfigure;
import uet.PeerCatcher.main.FileModifier;
import net.andreinc.mockneat.MockNeat;
import net.andreinc.mockneat.types.enums.IPv4Type;

public class CalculateMutualContactScore {
    public static HashMap<String, Integer> map_p2p = new HashMap<String, Integer>();
    public static HashMap<Integer, String> map_Id2Ip = new HashMap<Integer, String>();

    private static double mutual_contact_score_threshold = PeerCatcherConfigure.MUTUAL_CONTACT_SCORE_THRESHOLD;
    private static double mmk_att_ratio = PeerCatcherConfigure.MMK_ATT_RATIO;

    public static int Generate_Mutual_Contact_Graph(String Graph) throws IllegalArgumentException, IOException {
        String InputFolder = PeerCatcherConfigure.ROOT_LOCATION + Graph + "/INPUT/P2P_Legi_Map";
        File folder = new File(InputFolder + "/");
        File[] listOfFiles = folder.listFiles();
        HashMap<String, String> IP_MAP = new HashMap<String, String>();

        for (File file : listOfFiles) {
            if (file.isFile() && !file.getName().substring(0, 1).equals(".")) {
                BufferedReader br = new BufferedReader(new FileReader(InputFolder + "/" + file.getName()));
                String line = "";
                while ((line = br.readLine()) != null && line.contains(".")) {
                    String[] lines = line.split("\t");
                    IP_MAP.put(lines[1], lines[2]);
                }
                br.close();
            }
        }

        ArrayList<String> bot_ip_sets = new ArrayList<String>();
        HashSet<String> app_sets = new HashSet<String>();
        ArrayList<String> app_ip_sets = new ArrayList<String>();
        app_sets.add("uTorrent");
        app_sets.add("Vuze");
        app_sets.add("eMule");
        app_sets.add("FrostWire");

        app_sets.add("P2P_1");
        app_sets.add("P2P_14");
        app_sets.add("P2P_2");
        app_sets.add("P2P_20");
        app_sets.add("P2P_32");
        app_sets.add("P2P_4");
        app_sets.add("P2P_48");
        app_sets.add("P2P_8");
        app_sets.add("P2P_96");

        Iterator it = IP_MAP.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry pair = (Map.Entry) it.next();
            String key = (String) pair.getKey();
            String val = (String) pair.getValue();
            if (app_sets.contains(val)) {
                app_ip_sets.add(key);
            } else {
                bot_ip_sets.add(key);
            }
        }

        InputFolder = PeerCatcherConfigure.ROOT_LOCATION + Graph + "/mutual_contact_sets";

        FileModifier.deleteDir(new File(PeerCatcherConfigure.ROOT_LOCATION + Graph + "/mutual_contact_graph"));
        File f = new File(PeerCatcherConfigure.ROOT_LOCATION + Graph + "/mutual_contact_graph");
        f.mkdir();

        String OutputFolder = PeerCatcherConfigure.ROOT_LOCATION + Graph + "/mutual_contact_graph/";
        folder = new File(InputFolder + "/");
        listOfFiles = folder.listFiles();

        PrintWriter writer_IDtoIP = new PrintWriter(OutputFolder + "IDtoIP.txt", "UTF-8");

        int ID_NUM = 0;

        String line = "";
        for (File file : listOfFiles) {
            if (file.isFile() && !file.getName().substring(0, 1).equals(".")) {
                BufferedReader br = new BufferedReader(new FileReader(InputFolder + "/" + file.getName()));

                while ((line = br.readLine()) != null) {
                    ID_NUM++;
                }
                br.close();
            }
        }

        int ID_NUM_indx = 0;
        int i = 0;
        String[] IP = new String[ID_NUM];
        String[] InSet = new String[ID_NUM];

        line = "";
        MockNeat mock = MockNeat.threadLocal();


        for (File file : listOfFiles) {

            if (file.isFile() && !file.getName().substring(0, 1).equals(".")) {
                BufferedReader br = new BufferedReader(new FileReader(InputFolder + "/" + file.getName()));


                while ((line = br.readLine()) != null) {

                    String[] parts = line.split("\t");
                    String set = parts[1].replace("[", "");
                    set = set.replace("]", "");

                    String ck_ip = parts[0].split(",")[0];
                    //no run
                    if (bot_ip_sets.contains(ck_ip) && mmk_att_ratio != 0.0) {
                        int new_add_ip = (int) (set.split(", ").length * mmk_att_ratio);
                        for (int ijk = 0; ijk < new_add_ip; ijk++) {
                            set = set + ", " + mock.ipv4s().type(IPv4Type.CLASS_B).val();
                        }
                    }

                    IP[i] = parts[0];
                    InSet[i] = set;

                    i++;

                    Set<String> Prefix16 = new HashSet<String>();
                    Set<String> Prefix24 = new HashSet<String>();
                    Set<String> Prefix32 = new HashSet<String>();

                    for (String IPANDProto : set.split(", ")) {
                        String IPP = IPANDProto;
                        String[] str = IPP.split("\\.");

                        String P24 = str[0] + "." + str[1] + "." + str[2];
                        String P16 = str[0] + "." + str[1];
                        Prefix16.add(P16);
                        Prefix24.add(P24);
                        Prefix32.add(IPP);
                    }
                    ;
                    if (IP_MAP.containsKey(parts[0].split(",")[0])) {
                        writer_IDtoIP.println(ID_NUM_indx + "\t" + parts[0] + "\t" + IP_MAP.get(parts[0].split(",")[0])
                                + "\t" + Prefix16.size() + "\t" + Prefix24.size() + "\t" + Prefix32.size());
                    } else {
                        writer_IDtoIP.println(ID_NUM_indx + "\t" + parts[0] + "\t" + "Normal" + "\t" + Prefix16.size()
                                + "\t" + Prefix24.size() + "\t" + Prefix32.size());
                    }
                    ID_NUM_indx++;
                    Prefix16.clear();
                    Prefix24.clear();
                    Prefix32.clear();
                }
                br.close();
            }
        }
        writer_IDtoIP.close();

        System.out.println("Started1...");
        Generate_HashMap();
        System.out.println("Ended1...");

        ExecutorService executor = Executors.newFixedThreadPool(256);

        for (int j = 0; j < IP.length; j++) {
            for (int k = j + 1; k < IP.length; k++) {
                String[] sts1 = IP[j].split(",");
                String[] sts2 = IP[k].split(",");

                String st1 = sts1[1] + "," + sts1[2] + "," + sts1[3];
                String st2 = sts2[1] + "," + sts2[2] + "," + sts2[3];

                if (st1.equals(st2)) {

                    GenerateMutualContactGraph R = new GenerateMutualContactGraph(OutputFolder,
                            InSet[j], InSet[k], j, k, mutual_contact_score_threshold);
                    executor.execute(R);
                }
            }
        }
        executor.shutdown();
        while (!executor.isTerminated()) {
        }
        System.out.println("Finished all threads!");

        return 1;
    }



    public static void run(String ID) throws IllegalArgumentException, IOException {
        String Graph = "Graph_" + ID;
        Generate_Mutual_Contact_Graph(Graph);
    }


    public static void Generate_HashMap() throws IllegalArgumentException, IOException {
        BufferedReader br_Freq = null;
        try {
            br_Freq = new BufferedReader(new FileReader(
                    PeerCatcherConfigure.ROOT_LOCATION + "Graph_1/p2p_host_frequency2/p2pFrequency2.txt"
            ));
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }

        String lineFreq = "";
        String[] temp;

        while ((lineFreq = br_Freq.readLine()) != null) {
            temp = lineFreq.split("\t");
            map_p2p.put(temp[0] + "\t" + temp[1], Integer.valueOf(temp[2]));
        }
        br_Freq.close();

        BufferedReader br_Id2Ip = null;
        try {
            br_Id2Ip = new BufferedReader(new FileReader(
                    PeerCatcherConfigure.ROOT_LOCATION + "Graph_1/mutual_contact_graph/IDtoIP.txt"
            ));
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }

        String lineId2Ip = "";

        while ((lineId2Ip = br_Id2Ip.readLine()) != null) {
            temp = lineId2Ip.split("\t");
            map_Id2Ip.put(Integer.valueOf(temp[0]), temp[1]);
//			System.out.println(Integer.valueOf(temp[0]) + " --map_Id2Ip-- " + temp[1]);
        }
        br_Id2Ip.close();
    }
}
