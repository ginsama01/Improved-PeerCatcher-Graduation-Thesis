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
    public static double botnetDetectionThresholdBgp = PeerCatcherConfigure.BOTNET_DETECTION_THRESHOLD_BGP;
    public static double botnetDetectionThresholdMcs = PeerCatcherConfigure.BOTNET_DETECTION_THRESHOLD_MCS;
    public static double botnetDetectionThresholdInternalDegree = PeerCatcherConfigure.BOTNET_DETECTION_THRESHOLD_INTERNAL_DEGREE;
    public static double botnetDetectionThresholdLocalAssor = PeerCatcherConfigure.BOTNET_DETECTION_THRESHOLD_LOCAL_ASSOCIATIVITY;
    public static double botnetDetectionCoefficientThreshold = PeerCatcherConfigure.BOTNET_DETECTION_THRESHOLD_COEFFICIENT_VARIATION;

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
        File edgesFile =  new File(PeerCatcherConfigure.ROOT_LOCATION + "/mutual_contact_graph/edges.txt");
        File verticesFile = new File(PeerCatcherConfigure.ROOT_LOCATION + "/mutual_contact_graph/vertices.txt");
        int numberOfNode = 0;
        if (verticesFile.isFile()) {
            String line = "";
            BufferedReader br = new BufferedReader((new FileReader(verticesFile.getPath())));
            while (br.readLine() != null) {
                ++numberOfNode;
            }
        }
        Vector[] neighbors = new Vector[numberOfNode + 5];
        for (int i = 0; i < numberOfNode; ++i) {
            neighbors[i] = new Vector<Integer>();
        }

        //Find neighbors
        if (edgesFile.isFile()) {
            String line = "";
            BufferedReader br = new BufferedReader((new FileReader(edgesFile.getPath())));
            while ((line = br.readLine()) != null) {
                String[] str = line.split("\t");
                Integer vertex1 = Integer.parseInt(str[0]);
                Integer vertex2 = Integer.parseInt(str[1]);
                neighbors[vertex1].add(vertex2);
                neighbors[vertex2].add(vertex1);
            }
        }

        //Calculate local assortativity
        double[] localAssortativity = new double[numberOfNode];
        Integer[] component = new Integer[numberOfNode];
        int numberOfComponent = 0;
        for (int i = 0; i < numberOfNode; ++i) {
            localAssortativity[i] = 0.0;
            component[i] = 0;
        }

        //Identify connected components
        for (int i = 0; i < numberOfNode; ++i) {
            if (component[i] == 0) {
                Queue<Integer> q = new LinkedList<Integer>();
                q.add(i);
                component[i] = ++numberOfComponent;
                while (!q.isEmpty()) {
                    int u = q.peek();
                    q.remove();
                    for (int j = 0; j < neighbors[u].size(); ++j) {
                        int v = (int) neighbors[u].get(j);
                        if (component[v] == 0) {
                            component[v] = numberOfComponent;
                            q.add(v);
                        }
                    }
                }
            }
        }

        //Calculate in terms of connected components
        for (int i = 1; i <= numberOfComponent; ++i) {
            Vector<Integer> tmp = new Vector<Integer>();
            for (int j = 0; j < numberOfNode; ++j) {
                if (component[j] == i) {
                    tmp.add(j);
                }

            }
            int n = tmp.size();
            if (n < 2) continue;
            int m = 0; //total edge
            double expectation = 0;
            double standardDeviation = 0;
            int[] degreeDistribution = new int[numberOfNode];
            for (int j = 0; j < numberOfNode; ++j) {
                degreeDistribution[j] = 0;
            }
            for (Integer t :tmp) {
                degreeDistribution[neighbors[t].size()]++;
                m += neighbors[t].size();
            }
            double avgDegree = (double)m / n;
            m /= 2;
            for (int j = 0; j < numberOfNode - 1; ++j) {
                double excessDegreeDistribution = (double) (j + 1) * (double) degreeDistribution[j + 1] / (avgDegree * n);
                expectation += excessDegreeDistribution * j;
            }
            for (int j = 0; j < numberOfNode - 1; ++j) {
                double excessDegreeDistribution = (double) (j + 1) * (double) degreeDistribution[j + 1] / (avgDegree * n);
                standardDeviation += (expectation - j) * (expectation - j) * excessDegreeDistribution;
            }

            for (Integer t : tmp) {
                if (standardDeviation == 0) {
                    localAssortativity[t] = 1.0 / n;
                } else {
                    int degree = neighbors[t].size() - 1;
                    double neighborDegree = 0;
                    for (Object neighbor : neighbors[t]) {
                        neighborDegree += (neighbors[(Integer)neighbor].size() - 1);
                    }
                    neighborDegree /= neighbors[t].size();
                    localAssortativity[t] = (double)degree * (degree + 1) * (neighborDegree - expectation) / (2.0 * m * standardDeviation);
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

        //Identify botnet communities
        PotentialIP.clear();
        File folder = new File(PeerCatcherConfigure.ROOT_LOCATION + "/louvain_communities_detection");
        File[] listOfFiles = folder.listFiles();

        for (File file : listOfFiles) {
            if (file.isFile() && !file.getName().substring(0, 1).equals(".")) {

                String line = "";
                BufferedReader br = new BufferedReader(new FileReader(file.getPath())); // Read the Community Detection
                // Result

                HashMap<String, ArrayList<String>> louvainResults = new HashMap<String, ArrayList<String>>();

                while ((line = br.readLine()) != null) {

                    String[] str = line.split(",");
                    if (louvainResults.containsKey(str[1])) {
                        louvainResults.get(str[1]).add(str[0]);
                    } else {
                        ArrayList<String> temp = new ArrayList<String>();
                        temp.add(str[0]);
                        louvainResults.put(str[1], temp);
                    }
                }
                br.close();

                for (Entry<String, ArrayList<String>> entry : louvainResults.entrySet()) {
                    String com_id = entry.getKey();
                    ArrayList<String> nodes = entry.getValue();

                    HashSet<String> nodesIps = new HashSet<String>();

                    if (nodes.size() > 2) {
                        double resolution = Double.parseDouble(file.getName().split("_")[1].split("\\.")[0]);
                        PrintWriter pw = new PrintWriter(
                                new FileOutputStream(
                                        new File(PeerCatcherConfigure.ROOT_LOCATION
                                                + "/communities_scores_calculate/" + "community_scores_" + resolution + ".txt"),
                                        true));
                        PrintWriter pw2 = new PrintWriter(new FileOutputStream(new File(PeerCatcherConfigure.ROOT_LOCATION
                                + "/communities_scores_calculate/" + "community_scores__" + resolution + "_2.txt"),
                                true));

                        double sumBGP = 0;
                        double sumMCS = 0;
                        double sumInternalDegree = 0;
                        double sumLocalAssortativity = 0;
                        double n = 0;
                        double m = 0;
                        double mean = 0;
                        double standardDeviation = 0;
                        double[] destinationDiversity = new double[nodes.size()];

                        BufferedReader brBGP = new BufferedReader(new FileReader(
                                PeerCatcherConfigure.ROOT_LOCATION + "/mutual_contact_graph/vertices.txt"));

                        while ((line = brBGP.readLine()) != null) {
                            if (nodes.contains(line.split("\t")[0])) {
                                destinationDiversity[(int)n] = Double.parseDouble(line.split("\t")[3]);
                                mean += destinationDiversity[(int)n];
                                n += 1;
                                sumLocalAssortativity += localAssortativity[Integer.parseInt(line.split("\t")[0])];
                                sumBGP += Double.parseDouble(line.split("\t")[3])
                                        / Double.parseDouble(line.split("\t")[5]);
                                nodesIps.add(line.split("\t")[1] + "," + line.split("\t")[2] + ","
                                        + line.split("\t")[3] + "," + line.split("\t")[4] + "," + line.split("\t")[5]);

                            }
                        }
                        brBGP.close();

                        mean /= n;
                        for (int i = 0; i < (int)n; ++i) {
                            standardDeviation += Math.pow(destinationDiversity[i] - mean, 2);
                        }

                        standardDeviation /= n;
                        standardDeviation = Math.sqrt((standardDeviation));

                        double coefficient_variation = standardDeviation / mean;

                        BufferedReader br_MCS = new BufferedReader(new FileReader(
                                PeerCatcherConfigure.ROOT_LOCATION + "/mutual_contact_graph/edges.txt"));
                        while ((line = br_MCS.readLine()) != null) {

                            if (nodes.contains(line.split("\t")[0]) && nodes.contains(line.split("\t")[1])) {
                                sumMCS += Double.parseDouble(line.split("\t")[2]);
                                sumInternalDegree += 1;
                            }
                        }
                        br_MCS.close();

                        m = n * (n-1) / 2;

                        pw.println(com_id + "," + n + "," + m + "," + sumBGP / n + "," + sumMCS / m + "," + sumInternalDegree / m + "," + sumLocalAssortativity + "," + coefficient_variation);

                        if (sumBGP / n > botnetDetectionThresholdBgp
                                && sumMCS / m > botnetDetectionThresholdMcs
                                    && sumInternalDegree / m > botnetDetectionThresholdInternalDegree
                                        && sumLocalAssortativity > botnetDetectionThresholdLocalAssor
                                            && coefficient_variation < botnetDetectionCoefficientThreshold
                        ) {
                            pw2.println(com_id + "," + n + "," + m + "," + sumBGP / n + "," + sumMCS / m + "," + sumInternalDegree / m + "," + sumLocalAssortativity + "," + coefficient_variation);
                            pw2.println(nodes);
                            pw2.println(nodesIps);
                            PotentialIP.addAll(nodes);

                            PrintWriter pw3 = new PrintWriter(
                                    new FileOutputStream(new File(PeerCatcherConfigure.ROOT_LOCATION
                                            + "/botnet_detection/bot_detection_input.txt"), true));
                            BufferedReader br3 = new BufferedReader(new FileReader(
                                    PeerCatcherConfigure.ROOT_LOCATION + "/mutual_contact_graph/edges.txt"));
                            Set<String> Edges = new HashSet<String>();
                            while ((line = br3.readLine()) != null) {
                                if (nodes.contains(line.split("\t")[0]) && nodes.contains(line.split("\t")[1])) {
                                    Edges.add(line.split("\t")[0] + "," + line.split("\t")[1]);
                                }
                            }
                            pw3.println(nodes.toString() + "\t" + Edges.toString());
                            br3.close();
                            pw3.close();
                        }
                        pw2.close();
                        pw.close();
                    }
                }
            }
        }

        PrintWriter pw = new PrintWriter(
                PeerCatcherConfigure.ROOT_LOCATION + "/botnet_detection/botnet_detection.txt");
        BufferedReader br = new BufferedReader(
                new FileReader(PeerCatcherConfigure.ROOT_LOCATION + "/mutual_contact_graph/vertices.txt"));
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
        Botnet_Detection();
    }
}
