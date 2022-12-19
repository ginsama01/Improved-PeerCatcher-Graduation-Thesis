package uet.PeerCatcher.mcg;

import uet.PeerCatcher.config.PeerCatcherConfigure;

import java.io.*;
import java.util.*;
public class GenerateMutualContactGraph implements Runnable {
    private String node1IPSet;
    private String node2IPSet;
    private String OutputFolder;
    private int node1;
    private int node2;
    private Thread t;
    private double mutual_contact_score_threshold;

    private int frequency_threshold = PeerCatcherConfigure.FREQUENCY_THRESHOLD;
    GenerateMutualContactGraph(String OutputFolder_, String node1IPSet_, String node2IPSet_, int node1_,
                                           int node2_, double mutual_contact_score_threshold_) {
        node1IPSet = node1IPSet_;
        node2IPSet = node2IPSet_;
        node1 = node1_;
        node2 = node2_;
        OutputFolder = OutputFolder_;
        mutual_contact_score_threshold = mutual_contact_score_threshold_;
    }

    @Override
    public void run() {
//		System.out.println("Running: node1: " + node1 + " node2: " + node2);
        String[] st1 = node1IPSet.split(", ");
        String[] st2 = node2IPSet.split(", ");
        double score = -1;
        Set<String> set1 = new HashSet<String>();
        for (String line : st1) {
            set1.add(line);
        }
        Set<String> set2 = new HashSet<String>();
        for (String line : st2) {
            set2.add(line);
        }

        Set<String> maxset = set1;
        Set<String> minset = set2;

        if (set1.size() < set2.size()) {
            maxset = set2;
            minset = set1;
        }
        int a = minset.size();
        int b = maxset.size();
        minset.retainAll(maxset);
        int c = minset.size();
        Set<String> s1 = minset;

        int sumIp = 0;
        int tempFreq1 = 0;
        int tempFreq2 = 0;

        String srcIP1 = "";
        String srcIP2 = "";
        String dstIP = "";
        String proto = "";
        String bppout = "";
        String bppin = "";
        String[] temp1;
        String[] temp2;

        for (String s : s1) {
            dstIP = s;
            temp1 = CalculateMutualContactScore.map_Id2Ip.get(node1).split(",");
            temp2 = CalculateMutualContactScore.map_Id2Ip.get(node2).split(",");
            srcIP1 = temp1[0];
            srcIP2 = temp2[0];
            proto = temp1[1];
            bppout = temp1[2];
            bppin = temp1[3];

            try {
                tempFreq1 = CalculateMutualContactScore.map_p2p.get(srcIP1 + "\t" + proto + "," + dstIP + "," + bppout + "," + bppin);
                if(tempFreq1 < frequency_threshold) {
                    continue;
                }
                tempFreq2 = CalculateMutualContactScore.map_p2p.get(srcIP2 + "\t" + proto + "," + dstIP + "," + bppout + "," + bppin);
                if(tempFreq2 < frequency_threshold) {
                    continue;
                }
                sumIp++;
            } catch (NullPointerException e) {
                System.out.println("1null " + srcIP1 + "\t" + proto + "," + dstIP + "," + bppout + "," + bppin);
                System.out.println("2null " + srcIP2 + "\t" + proto + "," + dstIP + "," + bppout + "," + bppin);
            }
        }

        set1.clear();
        set2.clear();
        minset.clear();
        maxset.clear();
        set1 = null;
        set2 = null;
        minset = null;
        maxset = null;
        score = (double) (sumIp) / (double) (a + b - c);

//		System.out.println(node1 + " - " + node2 + " - score: " + score + " sumIp: " + sumIp + " totalIP: " + (a + b - c) + " totalInnerIP: " + c);

        if (c > 0 && score > mutual_contact_score_threshold) {
            try {
                PrintWriter writer = new PrintWriter(
                        new FileOutputStream(new File(OutputFolder + "LouvainInput.txt"), true));
                writer.println(node1 + "\t" + node2 + "\t" + score);
                writer.close();
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        }
    }

    public void start() {
        if (t == null) {
            t = new Thread(this);
            t.start();
        }
    }
}
