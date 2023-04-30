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
    public static HashMap<String, Integer> mapP2P = new HashMap<String, Integer>();
    public static HashMap<Integer, String> mapId2Ip = new HashMap<Integer, String>();

    private static double mutualContactScoreThreshold = PeerCatcherConfigure.MUTUAL_CONTACT_SCORE_THRESHOLD;
    private static double mmkAttRatio = PeerCatcherConfigure.MMK_ATT_RATIO;

    public static int GenerateMutualContactGraph() throws IllegalArgumentException, IOException {
        String inputFolder = PeerCatcherConfigure.ROOT_LOCATION + "/INPUT/P2P_Legi_Map";
        File folder = new File(inputFolder + "/");
        File[] listOfFiles = folder.listFiles();
        HashMap<String, String> ipMap = new HashMap<String, String>();

        for (File file : listOfFiles) {
            if (file.isFile() && !file.getName().substring(0, 1).equals(".")) {
                BufferedReader br = new BufferedReader(new FileReader(inputFolder + "/" + file.getName()));
                String line = "";
                while ((line = br.readLine()) != null && line.contains(".")) {
                    String[] lines = line.split("\t");
                    ipMap.put(lines[1], lines[2]);
                }
                br.close();
            }
        }

        ArrayList<String> botIpSets = new ArrayList<String>();
        HashSet<String> appSets = new HashSet<String>();
        ArrayList<String> appIpSets = new ArrayList<String>();
        appSets.add("uTorrent");
        appSets.add("Vuze");
        appSets.add("eMule");
        appSets.add("FrostWire");

        appSets.add("P2P_1");
        appSets.add("P2P_14");
        appSets.add("P2P_2");
        appSets.add("P2P_20");
        appSets.add("P2P_32");
        appSets.add("P2P_4");
        appSets.add("P2P_48");
        appSets.add("P2P_8");
        appSets.add("P2P_96");

        Iterator it = ipMap.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry pair = (Map.Entry) it.next();
            String key = (String) pair.getKey();
            String val = (String) pair.getValue();
            if (appSets.contains(val)) {
                appIpSets.add(key);
            } else {
                botIpSets.add(key);
            }
        }

        inputFolder = PeerCatcherConfigure.ROOT_LOCATION + "/mutual_contact_sets";

        FileModifier.deleteDir(new File(PeerCatcherConfigure.ROOT_LOCATION + "/mutual_contact_graph"));
        File f = new File(PeerCatcherConfigure.ROOT_LOCATION + "/mutual_contact_graph");
        f.mkdir();

        String outputFolder = PeerCatcherConfigure.ROOT_LOCATION + "/mutual_contact_graph/";
        folder = new File(inputFolder + "/");
        listOfFiles = folder.listFiles();

        PrintWriter writer_IDtoIP = new PrintWriter(outputFolder + "IDtoIP.txt", "UTF-8");

        int idNum = 0;

        String line = "";
        for (File file : listOfFiles) {
            if (file.isFile() && !file.getName().substring(0, 1).equals(".")) {
                BufferedReader br = new BufferedReader(new FileReader(inputFolder + "/" + file.getName()));

                while ((line = br.readLine()) != null) {
                    idNum++;
                }
                br.close();
            }
        }

        int idNumIndx = 0;
        int i = 0;
        String[] IP = new String[idNum];
        String[] inSet = new String[idNum];

        line = "";
        MockNeat mock = MockNeat.threadLocal();


        for (File file : listOfFiles) {

            if (file.isFile() && !file.getName().substring(0, 1).equals(".")) {
                BufferedReader br = new BufferedReader(new FileReader(inputFolder + "/" + file.getName()));


                while ((line = br.readLine()) != null) {

                    String[] parts = line.split("\t");
                    String set = parts[1].replace("[", "");
                    set = set.replace("]", "");

                    String ckIp = parts[0].split(",")[0];
                    //no run
                    if (botIpSets.contains(ckIp) && mmkAttRatio != 0.0) {
                        int newAddIp = (int) (set.split(", ").length * mmkAttRatio);
                        for (int ijk = 0; ijk < newAddIp; ijk++) {
                            set = set + ", " + mock.ipv4s().type(IPv4Type.CLASS_B).val();
                        }
                    }

                    IP[i] = parts[0];
                    inSet[i] = set;

                    i++;

                    Set<String> prefix16 = new HashSet<String>();
                    Set<String> prefix24 = new HashSet<String>();
                    Set<String> prefix32 = new HashSet<String>();

                    for (String IPANDProto : set.split(", ")) {
                        String IPP = IPANDProto;
                        String[] str = IPP.split("\\.");

                        String P24 = str[0] + "." + str[1] + "." + str[2];
                        String P16 = str[0] + "." + str[1];
                        prefix16.add(P16);
                        prefix24.add(P24);
                        prefix32.add(IPP);
                    }
                    ;
                    if (ipMap.containsKey(parts[0].split(",")[0])) {
                        writer_IDtoIP.println(idNumIndx + "\t" + parts[0] + "\t" + ipMap.get(parts[0].split(",")[0])
                                + "\t" + prefix16.size() + "\t" + prefix24.size() + "\t" + prefix32.size());
                    } else {
                        writer_IDtoIP.println(idNumIndx + "\t" + parts[0] + "\t" + "Normal" + "\t" + prefix16.size()
                                + "\t" + prefix24.size() + "\t" + prefix32.size());
                    }
                    idNumIndx++;
                    prefix16.clear();
                    prefix24.clear();
                    prefix32.clear();
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

                    GenerateMutualContactGraph R = new GenerateMutualContactGraph(outputFolder,
                            inSet[j], inSet[k], j, k, mutualContactScoreThreshold);
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



    public static void run() throws IllegalArgumentException, IOException {
        GenerateMutualContactGraph();
    }


    public static void Generate_HashMap() throws IllegalArgumentException, IOException {
        File folder = new File(PeerCatcherConfigure.ROOT_LOCATION + "p2p_host_frequency2/");
        File[] listOfFiles = folder.listFiles();

        for (File file : listOfFiles) {
            if (file.isFile() && !file.getName().substring(0, 1).equals(".")) {
                BufferedReader br_Freq = new BufferedReader(new FileReader(PeerCatcherConfigure.ROOT_LOCATION + "p2p_host_frequency2/" + file.getName()));
                String lineFreq = "";
                String[] temp;
                while ((lineFreq = br_Freq.readLine()) != null) {
                    temp = lineFreq.split("\t");
                    mapP2P.put(temp[0] + "\t" + temp[1], Integer.valueOf(temp[2]));
                }
                BufferedReader br_Id2Ip = null;
                try {
                    br_Id2Ip = new BufferedReader(new FileReader(
                            PeerCatcherConfigure.ROOT_LOCATION + "mutual_contact_graph/IDtoIP.txt"
                    ));
                } catch (FileNotFoundException e) {
                    throw new RuntimeException(e);
                }

                String lineId2Ip = "";

                while ((lineId2Ip = br_Id2Ip.readLine()) != null) {
                    temp = lineId2Ip.split("\t");
                    mapId2Ip.put(Integer.valueOf(temp[0]), temp[1]);
                }
                br_Id2Ip.close();
                br_Freq.close();
            }
        }


    }
}
