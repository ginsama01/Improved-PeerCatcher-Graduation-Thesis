package uet.PeerCatcher.botnet;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;
import java.util.Map.Entry;

import uet.PeerCatcher.config.PeerCatcherConfigure;
import uet.PeerCatcher.main.FileModifier;

public class BotnetIdentify {

    public static Set<String> PotentialIP = new HashSet<String>();

    public static double botnet_detection_threshold_bgp;
    public static double botnet_detection_threshold_mcs;
    public static double botnet_detection_threshold_internal_degree = PeerCatcherConfigure.INTERNAL_DEGREE_THRESHOLD;
    public static double botnet_detection_threshold_local_assor = PeerCatcherConfigure.LOCAL_ASSORTATIVITY_THRESHOLD;
    public static double coefficient_threshold = PeerCatcherConfigure.COEFFICIENT_VARIATION_THRESHOLD;
    public static void Botnet_Detection() throws IOException {
        FileModifier.deleteDir(new File(PeerCatcherConfigure.ROOT_LOCATION + "/botnet_detection"));
        File f = new File(PeerCatcherConfigure.ROOT_LOCATION + "/botnet_detection");

        if (!f.exists()) {
            if (f.mkdir()) {
                System.out.println(
                        "Directory " + PeerCatcherConfigure.ROOT_LOCATION + "/botnet_detection" + " is created!");
            } else {
                System.out.println("Failed to create directory!");
            }
        }

        //Calculate neighbors set and local assortativity
        File graph_file =  new File(PeerCatcherConfigure.ROOT_LOCATION + "/mutual_contact_graph/LouvainInput.txt");
        File ip_file = new File(PeerCatcherConfigure.ROOT_LOCATION + "/mutual_contact_graph/IDtoIP.txt");
        int number_of_node = 0;
        if (ip_file.isFile()) {
            String line = "";
            BufferedReader br = new BufferedReader((new FileReader(ip_file.getPath())));
            while (br.readLine() != null) {
                ++number_of_node;
            }
        }
        Vector[] neighbors = new Vector[number_of_node + 5];
        for (int i = 0; i < number_of_node; ++i) {
            neighbors[i] = new Vector<Integer>();
        }
        // Neighbors
        if (graph_file.isFile()) {
            String line = "";
            BufferedReader br = new BufferedReader((new FileReader(graph_file.getPath())));
            while ((line = br.readLine()) != null) {
                String[] str = line.split("\t");
                Integer vertex1 = Integer.parseInt(str[0]);
                Integer vertex2 = Integer.parseInt(str[1]);
                neighbors[vertex1].add(vertex2);
                neighbors[vertex2].add(vertex1);
            }
        }

        //Calculate local assortativity
        double[] local_assortativity = new double[number_of_node];
        Integer[] component = new Integer[number_of_node];
        int number_of_component = 0;
        for (int i = 0; i < number_of_node; ++i) {
            local_assortativity[i] = 0.0;
            component[i] = 0;
        }

        // Tinh theo cac thanh phan lien thong
        for (int i = 0; i < number_of_node; ++i) {
            if (component[i] == 0) {
                Queue<Integer> q = new LinkedList<Integer>();
                q.add(i);
                component[i] = ++number_of_component;
                while (!q.isEmpty()) {
                    int u = q.peek();
                    q.remove();
                    for (int j = 0; j < neighbors[u].size(); ++j) {
                        int v = (int) neighbors[u].get(j);
                        if (component[v] == 0) {
                            component[v] = number_of_component;
                            q.add(v);
                        }
                    }
                }
            }
        }

        //Calculate cua tung tplt
        for (int i = 1; i <= number_of_component; ++i) {
            Vector<Integer> tmp = new Vector<Integer>();
            for (int j = 0; j < number_of_node; ++j) {
                if (component[j] == i) {
                    tmp.add(j);
                }

            }
            int n = tmp.size();
            if (n < 2) continue;
            int m = 0; //total edge
            double expectation = 0;
            double standard_deviation = 0;
            int[] degree_distribution = new int[number_of_node];
            for (int j = 0; j < number_of_node; ++j) {
                degree_distribution[j] = 0;
            }
            for (Integer t :tmp) {
                degree_distribution[neighbors[t].size()]++;
                m += neighbors[t].size();
            }
            double avg_degree = (double)m / n;
            m /= 2;
            for (int j = 0; j < number_of_node - 1; ++j) {
                double excess_degree_distribution = (double) (j + 1) * (double) degree_distribution[j + 1] / (avg_degree * n);
                expectation += excess_degree_distribution * j;
            }
            for (int j = 0; j < number_of_node - 1; ++j) {
                double excess_degree_distribution = (double) (j + 1) * (double) degree_distribution[j + 1] / (avg_degree * n);
                standard_deviation += (expectation - j) * (expectation - j) * excess_degree_distribution;
            }
//            if (i == 1) System.out.println("Expectation" + expectation);
//            if (i == 1) System.out.println("Standard deviation" + standard_deviation);
            for (Integer t : tmp) {
                if (standard_deviation == 0) {
                    local_assortativity[t] = 1.0 / n;
                } else {
                    int degree = neighbors[t].size() - 1;
                    double neighbor_degree = 0;
                    for (Object neighbor : neighbors[t]) {
                        neighbor_degree += (neighbors[(Integer)neighbor].size() - 1);
                    }
                    neighbor_degree /= neighbors[t].size();
                    local_assortativity[t] = (double)degree * (degree + 1) * (neighbor_degree - expectation) / (2.0 * m * standard_deviation);
                }
            }
        }


        FileModifier.deleteDir(new File(PeerCatcherConfigure.ROOT_LOCATION + "/communities_scores_calculate"));
        f = new File(PeerCatcherConfigure.ROOT_LOCATION + "/communities_scores_calculate");

        if (!f.exists()) {
            if (f.mkdir()) {
                System.out.println("Directory " + PeerCatcherConfigure.ROOT_LOCATION
                        + "/communities_scores_calculate" + " is created!");
            } else {
                System.out.println("Failed to create directory!");
            }
        }

        PotentialIP.clear();
        File folder = new File(PeerCatcherConfigure.ROOT_LOCATION + "/louvain_communities_detection");
        File[] listOfFiles = folder.listFiles();

        for (File file : listOfFiles) {
            if (file.isFile() && !file.getName().substring(0, 1).equals(".")) {

                String line = "";
                BufferedReader br = new BufferedReader(new FileReader(file.getPath())); // Read the Community Detection
                // Result

                HashMap<String, ArrayList<String>> louvain_results = new HashMap<String, ArrayList<String>>();

                while ((line = br.readLine()) != null) {

                    String[] str = line.split(",");
                    if (louvain_results.containsKey(str[1])) {
                        louvain_results.get(str[1]).add(str[0]);
                    } else {
                        ArrayList<String> temp = new ArrayList<String>();
                        temp.add(str[0]);
                        louvain_results.put(str[1], temp);
                    }
                }
                br.close();

                for (Entry<String, ArrayList<String>> entry : louvain_results.entrySet()) {
                    String com_id = entry.getKey();
                    ArrayList<String> nodes = entry.getValue();

                    HashSet<String> nodes_ips = new HashSet<String>();

                    if (nodes.size() > 2) {
                        double resolution = Double.parseDouble(file.getName().split("_")[1].split("\\.")[0]);
                        PrintWriter pw = new PrintWriter(
                                new FileOutputStream(
                                        new File(PeerCatcherConfigure.ROOT_LOCATION
                                                + "/communities_scores_calculate/" + "_" + resolution + ".txt"),
                                        true));
                        PrintWriter pw_2 = new PrintWriter(new FileOutputStream(new File(PeerCatcherConfigure.ROOT_LOCATION
                                + "/communities_scores_calculate/" + "_" + resolution + "_2.txt"),
                                true));

                        double Sum_BGP = 0;
                        double Sum_MCS = 0;
                        double Sum_InternalDegree = 0;
                        double Sum_LocalAssortativity = 0;
                        double m = 0;
                        double n = 0;
                        double mean = 0;
                        double standard_deviation = 0;
                        double[] destination_diversity = new double[nodes.size()];

                        BufferedReader br_BGP = new BufferedReader(new FileReader(
                                PeerCatcherConfigure.ROOT_LOCATION + "/mutual_contact_graph/IDtoIP.txt"));

                        while ((line = br_BGP.readLine()) != null) {
                            if (nodes.contains(line.split("\t")[0])) {
                                destination_diversity[(int)m] = Double.parseDouble(line.split("\t")[3]);
                                mean += destination_diversity[(int)m];
                                m += 1;
                                Sum_LocalAssortativity += local_assortativity[Integer.parseInt(line.split("\t")[0])];
                                Sum_BGP += Double.parseDouble(line.split("\t")[3])
                                        / Double.parseDouble(line.split("\t")[5]);
                                nodes_ips.add(line.split("\t")[1] + "," + line.split("\t")[2] + ","
                                        + line.split("\t")[3] + "," + line.split("\t")[4] + "," + line.split("\t")[5]);

                            }
                        }
                        br_BGP.close();

                        mean /= m;
                        for (int i = 0; i < (int)m; ++i) {
                            standard_deviation += Math.pow(destination_diversity[i] - mean, 2);
                        }

                        standard_deviation /= m;
                        standard_deviation = Math.sqrt((standard_deviation));

                        double coefficient_variation = standard_deviation / mean;

                        BufferedReader br_MCS = new BufferedReader(new FileReader(
                                PeerCatcherConfigure.ROOT_LOCATION + "/mutual_contact_graph/LouvainInput.txt"));
                        while ((line = br_MCS.readLine()) != null) {

                            if (nodes.contains(line.split("\t")[0]) && nodes.contains(line.split("\t")[1])) {
                                Sum_MCS += Double.parseDouble(line.split("\t")[2]);
                                Sum_InternalDegree += 1;
                            }
                        }
                        br_MCS.close();

                        n = m * (m-1) / 2;

                        pw.println(com_id + "," + m + "," + n + "," + Sum_BGP / m + "," + Sum_MCS / n + "," + Sum_InternalDegree / n + "," + Sum_LocalAssortativity + "," + coefficient_variation);

                        if (Sum_BGP / m > botnet_detection_threshold_bgp
                                && Sum_MCS / n > botnet_detection_threshold_mcs
                                    && Sum_InternalDegree > botnet_detection_threshold_internal_degree
                                        && Sum_LocalAssortativity > botnet_detection_threshold_local_assor
                                            && coefficient_variation < coefficient_threshold
                        ) {
                            pw_2.println(com_id + "," + m + "," + n + "," + Sum_BGP / m + "," + Sum_MCS / n + "," + Sum_InternalDegree / n + "," + Sum_LocalAssortativity + "," + coefficient_variation);
                            pw_2.println(nodes);
                            pw_2.println(nodes_ips);
                            PotentialIP.addAll(nodes);

                            PrintWriter pw_ = new PrintWriter(
                                    new FileOutputStream(new File(PeerCatcherConfigure.ROOT_LOCATION
                                            + "/botnet_detection/bot_detection_input.txt"), true));
                            BufferedReader br_ = new BufferedReader(new FileReader(
                                    PeerCatcherConfigure.ROOT_LOCATION + "/mutual_contact_graph/LouvainInput.txt"));
                            Set<String> Edges = new HashSet<String>();
                            while ((line = br_.readLine()) != null) {
                                if (nodes.contains(line.split("\t")[0]) && nodes.contains(line.split("\t")[1])) {
                                    Edges.add(line.split("\t")[0] + "," + line.split("\t")[1]);
                                }
                            }
                            pw_.println(nodes.toString() + "\t" + Edges.toString());
                            br_.close();
                            pw_.close();
                        }
                        pw_2.close();
                        pw.close();
                    }
                }
            }
        }

        PrintWriter pw = new PrintWriter(
                PeerCatcherConfigure.ROOT_LOCATION + "/botnet_detection/botnet_detection.txt");
        BufferedReader br = new BufferedReader(
                new FileReader(PeerCatcherConfigure.ROOT_LOCATION + "/mutual_contact_graph/IDtoIP.txt"));
        String line = "";
        Set<String> PotentialIP_Set = new HashSet<String>();
        while ((line = br.readLine()) != null) {
            if (PotentialIP.contains(line.split("\t")[0])) {
                pw.println(line.split("\t")[0] + "\t" + line.split("\t")[1] + "\t" + line.split("\t")[2]);
                PotentialIP_Set.add(line.split("\t")[1].split(",")[0] + "\t" + line.split("\t")[2]);
            }
        }
        br.close();
        pw.close();

        pw = new PrintWriter(PeerCatcherConfigure.ROOT_LOCATION + "/botnet_detection/botnet_detection_2.txt");
        for (String i : PotentialIP_Set) {
            pw.println(i);
        }
        pw.close();
    }

    public static void run() throws IllegalArgumentException, IOException {
        for (double botnet_detection_threshold_bgp_double : PeerCatcherConfigure.BOTNET_DETECTION_THRESHOLD_BGP_SET) {
            for (double botnet_detection_threshold_mcs_double : PeerCatcherConfigure.BOTNET_DETECTION_THRESHOLD_MCS_SET) {
                botnet_detection_threshold_bgp = botnet_detection_threshold_bgp_double;
                botnet_detection_threshold_mcs = botnet_detection_threshold_mcs_double;

                Botnet_Detection();

            }
        }
    }
}
