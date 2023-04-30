package uet.PeerCatcher.p2p;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

import uet.PeerCatcher.config.PeerCatcherConfigure;
import uet.PeerCatcher.main.FileModifier;

public class P2PHostIdentify {


    //Mapper class for P2P host detection Map-Reduce Module
    public static class P2PHostDetectionMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().split("\t");
            String[] sets = line[1].split(",");
            // Merging through byte-per-packet threshold
            try {
                long bppout = Long.parseLong(sets[2]);
                long bppin = Long.parseLong(sets[3]);
                bppout /= bytePerPacketThreshold;
                bppin /= bytePerPacketThreshold;
                context.write(new Text(line[0] + "," + sets[0] + "," + bppout + "," + bppin), new Text(sets[1]));

            } catch (NumberFormatException e) {
                System.out.println("Cannot convert string to long");
                System.exit(0);
            }


        }
    }

    //Reducer class for P2P host detection Map-Reduce Module
    public static class P2PHostDetectionReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            Set<String> distinct16Set = new HashSet<String>();
            List<String> cache = new ArrayList<String>();
            int flag = -1;
            for (Text i : values) {
                String[] sets = i.toString().split("\\.");
                distinct16Set.add(sets[0] + "." + sets[1]);
                cache.add(i.toString());
                if (distinct16Set.size() >= p2PHostDetectionThreshold) {
                    flag = 1;
                    break;
                }
            }
            if (flag == 1) {
                String[] sets = key.toString().split(",");
                for (String i : cache) {
                    context.write(new Text(sets[0]), new Text(sets[1] + "," + i + "," + sets[2] + "," + sets[3]));
                }
                for (Text i : values) {
                    context.write(new Text(sets[0]),
                            new Text(sets[1] + "," + i.toString() + "," + sets[2] + "," + sets[3]));
                }
            } else {
                cache.clear();
            }

        }
    }

    //P2P host detection threshold for detecting P2P flows
    private static int p2PHostDetectionThreshold = PeerCatcherConfigure.P2P_HOST_DETECTION_THRESHOLD_DEFAULT;
    //Byte per packet threshold for merging flows
    private static int bytePerPacketThreshold = PeerCatcherConfigure.BYTE_PER_PACKET_THRESHOLD;

    public static void run() throws IllegalArgumentException, IOException {

        //Create MAP-REDUCE job for detecting P2P flows.
        BasicConfigurator.configure();
        JobConf conf = new JobConf(P2PHostIdentify.class);

        FileModifier.deleteDir(new File(PeerCatcherConfigure.ROOT_LOCATION  + "/p2p_host_detection"));

        Job jobP2PHostDetection = Job.getInstance();
        jobP2PHostDetection.setJobName("Job_p2p_host_detection_");
        jobP2PHostDetection.setJarByClass(P2PHostIdentify.class);
        jobP2PHostDetection.setMapperClass(P2PHostDetectionMapper.class);
        jobP2PHostDetection.setReducerClass(P2PHostDetectionReducer.class);
        jobP2PHostDetection.setOutputKeyClass(Text.class);
        jobP2PHostDetection.setOutputValueClass(Text.class);
        ControlledJob ctrlJobP2PHostDetection = new ControlledJob(conf);
        ctrlJobP2PHostDetection.setJob(jobP2PHostDetection);

        FileInputFormat.addInputPath(jobP2PHostDetection,
                new Path(PeerCatcherConfigure.ROOT_LOCATION + "/INPUT/Legi"));
        FileInputFormat.addInputPath(jobP2PHostDetection,
                new Path(PeerCatcherConfigure.ROOT_LOCATION + "/INPUT/P2P"));
        FileInputFormat.addInputPath(jobP2PHostDetection,
                new Path(PeerCatcherConfigure.ROOT_LOCATION + "/INPUT/Wild_P2P"));
        FileOutputFormat.setOutputPath(jobP2PHostDetection,
                new Path(PeerCatcherConfigure.ROOT_LOCATION + "/p2p_host_detection"));
        jobP2PHostDetection.setNumReduceTasks(18);

        // Run job
        JobControl jobCtrl = new JobControl("ctrl_p2p_host_detection");
        jobCtrl.addJob(ctrlJobP2PHostDetection);
        Thread t = new Thread(jobCtrl);
        t.start();

        while (true) {
            if (jobCtrl.allFinished()) {
                System.out.println(jobCtrl.getSuccessfulJobList());
                jobCtrl.stop();
                break;
            }
        }

    }

}
