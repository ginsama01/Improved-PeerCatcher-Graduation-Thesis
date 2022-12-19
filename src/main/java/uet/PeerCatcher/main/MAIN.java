package uet.PeerCatcher.main;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;

import uet.PeerCatcher.louvain.LouvainMain;
import uet.PeerCatcher.p2p.P2PHostIdentify;
import uet.PeerCatcher.mcg.CalculateMutualContactScore;
import uet.PeerCatcher.botnet.BotnetIdentify;
public class MAIN {
    public static void Experiment(int ID) throws IllegalArgumentException, IOException, InterruptedException {
        PrintWriter pw = new PrintWriter(new FileOutputStream(new File("ExTime"), true));

        long st_time = System.currentTimeMillis();

        long st_time_1 = System.currentTimeMillis();
        P2PHostIdentify.run(ID + "");
        long end_time_1 = System.currentTimeMillis();

        long st_time_2 = System.currentTimeMillis();
        CalculateMutualContactScore.run(ID + "");
        long end_time_2 = System.currentTimeMillis();

        long st_time_3 = System.currentTimeMillis();
        LouvainMain.run(ID + "");
        long end_time_3 = System.currentTimeMillis();

        long st_time_4 = System.currentTimeMillis();
        BotnetIdentify.run(ID + "");
        long end_time_4 = System.currentTimeMillis();

        long end_time = System.currentTimeMillis();

		pw.println(ID + "\t" + (end_time_1 - st_time_1) + "\t" + (end_time_2 - st_time_2) + "\t"
				+ (end_time_3 - st_time_3) + "\t" + (end_time_4 - st_time_4) + "\t" + (end_time - st_time));
        pw.close();
    }

    public static void main(String[] args) throws Exception {
        Experiment(1);
    }
}
