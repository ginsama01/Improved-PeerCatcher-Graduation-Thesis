package uet.PeerCatcher.main;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;

import uet.PeerCatcher.louvain.LouvainMain;
import uet.PeerCatcher.mcg.CalculateFrequency;
import uet.PeerCatcher.p2p.P2PHostIdentify;
import uet.PeerCatcher.mcg.CreateMutualContactGraph;
import uet.PeerCatcher.botnet.BotnetIdentify;
public class MAIN {
    public static void Experiment(int ID) throws IllegalArgumentException, IOException, InterruptedException {
        PrintWriter pw = new PrintWriter(new FileOutputStream(new File("ExTime"), true));

        long st_time_start = System.currentTimeMillis();

        P2PHostIdentify.run();

        CalculateFrequency.run();

        CreateMutualContactGraph.run();

        LouvainMain.run();

        BotnetIdentify.run();

        long st_time_end = System.currentTimeMillis();
        pw.println(st_time_end - st_time_start);
        pw.close();
    }

    public static void main(String[] args) throws Exception {
        Experiment(1);
    }
}
