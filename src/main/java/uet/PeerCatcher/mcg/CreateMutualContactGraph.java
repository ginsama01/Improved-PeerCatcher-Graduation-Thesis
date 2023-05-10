package uet.PeerCatcher.mcg;

import java.io.*;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import uet.PeerCatcher.config.PeerCatcherConfigure;
import uet.PeerCatcher.main.FileModifier;

public class CreateMutualContactGraph {
    protected static HashMap<String, Integer> flowFrequency = new HashMap<String, Integer>();
    protected static HashMap<Integer, String> vertexId2Ip = new HashMap<Integer, String>();

    public static int GenerateMutualContactGraph() throws IllegalArgumentException, IOException {

        //Get label of IP from P2P_Legi_Map
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


        //Create mutual contact graph
        inputFolder = PeerCatcherConfigure.ROOT_LOCATION + "/mutual_contact_sets";

        FileModifier.deleteDir(new File(PeerCatcherConfigure.ROOT_LOCATION + "/mutual_contact_graph"));
        File f = new File(PeerCatcherConfigure.ROOT_LOCATION + "/mutual_contact_graph");
        f.mkdir();

        String outputFolder = PeerCatcherConfigure.ROOT_LOCATION + "/mutual_contact_graph/";
        folder = new File(inputFolder + "/");
        listOfFiles = folder.listFiles();

        //Vertices file
        PrintWriter printWriter = new PrintWriter(outputFolder + "vertices.txt", "UTF-8");

        //Count number of vertices
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

        int idNumIndex = 0;
        String[] vertexIp = new String[idNum];
        String[] contactSet = new String[idNum];

        //Create vertices file with flow bundle, label type, number of distinct /16, /24 and /32.
        int i = 0;
        for (File file : listOfFiles) {
            if (file.isFile() && !file.getName().substring(0, 1).equals(".")) {
                BufferedReader br = new BufferedReader(new FileReader(inputFolder + "/" + file.getName()));

                while ((line = br.readLine()) != null) {
                    //Sample line input "100.156.51.138,tcp,11,11	[124.225.110.104, 79.16.39.243, 114.246.152.89, 114.91.192.55,
                    // 86.152.179.229, 14.113.26.210, 116.228.2.154, 46.27.163.65, 79.177.16.226, 219.140.214.236,
                    // 93.29.163.76, 59.172.198.14, 112.115.188.139]
                    String[] parts = line.split("\t");
                    String set = parts[1].replace("[", "");
                    set = set.replace("]", "");

                    vertexIp[i] = parts[0];
                    contactSet[i] = set;

                    i++;

                    Set<String> prefix16 = new HashSet<String>();
                    Set<String> prefix24 = new HashSet<String>();
                    Set<String> prefix32 = new HashSet<String>();

                    for (String destIp : set.split(", ")) {
                        String[] str = destIp.split("\\.");

                        String P24 = str[0] + "." + str[1] + "." + str[2];
                        String P16 = str[0] + "." + str[1];
                        prefix16.add(P16);
                        prefix24.add(P24);
                        prefix32.add(destIp);
                    }

                    if (ipMap.containsKey(parts[0].split(",")[0])) {
                        printWriter.println(idNumIndex + "\t" + parts[0] + "\t" + ipMap.get(parts[0].split(",")[0])
                                + "\t" + prefix16.size() + "\t" + prefix24.size() + "\t" + prefix32.size());
                    } else {
                        printWriter.println(idNumIndex + "\t" + parts[0] + "\t" + "Normal" + "\t" + prefix16.size()
                                + "\t" + prefix24.size() + "\t" + prefix32.size());
                    }
                    vertexId2Ip.put(idNumIndex, parts[0]);
                    idNumIndex++;
                    prefix16.clear();
                    prefix24.clear();
                    prefix32.clear();
                }
                br.close();
            }
        }
        printWriter.close();

        //Read flow frequency into map
        folder = new File(PeerCatcherConfigure.ROOT_LOCATION + "p2p_host_frequency/");
        listOfFiles = folder.listFiles();

        for (File file : listOfFiles) {
            if (file.isFile() && !file.getName().substring(0, 1).equals(".")) {
                BufferedReader br = new BufferedReader(new FileReader(PeerCatcherConfigure.ROOT_LOCATION + "p2p_host_frequency/" + file.getName()));
                String[] sets;
                while ((line = br.readLine()) != null) {
                    sets = line.split("\t");
                    flowFrequency.put(sets[0] + "\t" + sets[1], Integer.valueOf(sets[2]));
                }
                br.close();


            }
        }

        //ExecutorService is used to manage a group of threads operating in the background, managed by the thread pool.
        //Help perform asynchronous tasks
        ExecutorService executor = Executors.newFixedThreadPool(256);
        System.out.println(vertexIp.length);
        for (int j = 0; j < vertexIp.length; j++) {
            for (int k = j + 1; k < vertexIp.length; k++) {
                String[] sts1 = vertexIp[j].split(",");
                String[] sts2 = vertexIp[k].split(",");

                String st1 = sts1[1] + "," + sts1[2] + "," + sts1[3];
                String st2 = sts2[1] + "," + sts2[2] + "," + sts2[3];

                //If two flow bundle has the same protocol, bppin and out
                if (st1.equals(st2)) {

                    CreateEdge R = new CreateEdge(outputFolder,
                            contactSet[j], contactSet[k], j, k);
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

}
