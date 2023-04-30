package uet.PeerCatcher.mcg;

import uet.PeerCatcher.config.PeerCatcherConfigure;

import java.io.*;
import java.util.*;
public class CreateEdge implements Runnable {
    private String destIpSet1;
    private String destIpSet2;
    private String outputFolder;
    private int vertex1;
    private int vertex2;
    private Thread t;

    private int frequencyThreshold = PeerCatcherConfigure.FREQUENCY_THRESHOLD;
    CreateEdge(String OutputFolder_, String destIpSet1_, String destIpSet2_, int vertex1_,
               int vertex2_) {
        destIpSet1 = destIpSet1_;
        destIpSet2 = destIpSet2_;
        vertex1 = vertex1_;
        vertex2 = vertex2_;
        outputFolder = OutputFolder_;
    }

    @Override
    public void run() {
        String[] destIp1 = destIpSet1.split(", ");
        String[] destIp2 = destIpSet2.split(", ");

        Set<String> set1 = new HashSet<String>();
        for (String line : destIp1) {
            set1.add(line);
        }
        Set<String> set2 = new HashSet<String>();
        for (String line : destIp2) {
            set2.add(line);
        }

        //Create union and intersection set of two sets of destination IP.
        Set<String> unionSet = new HashSet<String>();
        unionSet.addAll(set1);
        unionSet.addAll(set2);
        Set<String> intersectionSet = new HashSet<String>();
        intersectionSet.addAll(set1);
        intersectionSet.retainAll(set2);

        //Calculate mcr - weight of edge
        int sameIp = 0;
        for (String s : intersectionSet) {
            String dstIP = s;
            String[] temp1 = CreateMutualContactGraph.vertexId2Ip.get(vertex1).split(",");
            String[] temp2 = CreateMutualContactGraph.vertexId2Ip.get(vertex2).split(",");
            String srcIP1 = temp1[0];
            String srcIP2 = temp2[0];
            String proto = temp1[1];
            String bppout = temp1[2];
            String bppin = temp1[3];

            int tempFreq1 = CreateMutualContactGraph.flowFrequency.get(srcIP1 + "\t" + proto + "," + dstIP + "," + bppout + "," + bppin);
            int tempFreq2 = CreateMutualContactGraph.flowFrequency.get(srcIP2 + "\t" + proto + "," + dstIP + "," + bppout + "," + bppin);
            //If frequency of similar dest IP with each flow bundle is greater than or equal to threshold
            if (tempFreq1 >= frequencyThreshold && tempFreq2 >= frequencyThreshold) {
                sameIp++;
            }
        }

        double mcr = (double) (sameIp) / (double) (unionSet.size());

        if (mcr > 0) {
            try {
                PrintWriter writer = new PrintWriter(
                        new FileOutputStream(new File(outputFolder + "edges.txt"), true));
                writer.println(vertex1 + "\t" + vertex2 + "\t" + mcr);
                writer.close();
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        }

        set1.clear();
        set2.clear();
        intersectionSet.clear();
        unionSet.clear();
    }

    public void start() {
        if (t == null) {
            t = new Thread(this);
            t.start();
        }
    }
}
