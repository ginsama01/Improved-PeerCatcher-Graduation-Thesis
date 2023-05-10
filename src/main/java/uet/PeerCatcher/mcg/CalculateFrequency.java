package uet.PeerCatcher.mcg;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;


public class CalculateFrequency {
    //Mapper class for frequency calculation Map-Reduce Module
    public static class CalculateFrequencyMapper extends Mapper<Object, Text, Text, IntWritable> {
        public final static IntWritable count = new IntWritable(1);
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            context.write(value, count);
        }
    }

    //Reducer class for frequency calculation Map-Reduce Module
    public static class CalculateFrequencyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    //Mapper class for generating mutual contact sets Map-Reduce Module
    public static class GenerateMutualContactSetsMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] sets = value.toString().split("\t");
            String[] set_1 = sets[1].split(",");
            context.write(new Text(sets[0] + "," + set_1[0] + "," + set_1[2] + "," + set_1[3]), new Text(set_1[1]));

        }
    }

    //Reducer class for generating mutual contact sets Map-Reduce Module
    public static class GenerateMutualContactSetsReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            Set<String> nodeSet = new HashSet<String>();
            for (Text i : values) {
                nodeSet.add(i.toString());
            }
            if (nodeSet.size() > 0) {
                context.write(new Text(key.toString()), new Text(nodeSet.toString()));
            }
            nodeSet.clear();
        }
    }

    public static void run() throws IllegalArgumentException, IOException {
        BasicConfigurator.configure();

        //Create and run MAP-REDUCE job for frequency calculation of flows.
        JobConf conf = new JobConf(CalculateFrequency.class);

        FileModifier.deleteDir(new File(PeerCatcherConfigure.ROOT_LOCATION + "/p2p_host_frequency"));

        Job jobCalculateFrequency = Job.getInstance();
        jobCalculateFrequency.setJobName("Job_calculate_frequency");
        jobCalculateFrequency.setJarByClass(CalculateFrequency.class);
        jobCalculateFrequency.setMapperClass(CalculateFrequencyMapper.class);
        jobCalculateFrequency.setReducerClass(CalculateFrequencyReducer.class);
        jobCalculateFrequency.setOutputKeyClass(Text.class);
        jobCalculateFrequency.setOutputValueClass(IntWritable.class);
        ControlledJob ctrlCalculateFrequency = new ControlledJob(conf);
        ctrlCalculateFrequency.setJob(jobCalculateFrequency);

        FileInputFormat.addInputPath(jobCalculateFrequency, new Path(PeerCatcherConfigure.ROOT_LOCATION + "/p2p_host_detection"));
        FileOutputFormat.setOutputPath(jobCalculateFrequency, new Path(PeerCatcherConfigure.ROOT_LOCATION + "/p2p_host_frequency"));
        jobCalculateFrequency.setNumReduceTasks(5);

        JobControl jobCtrl = new JobControl("ctrl_calculate_frequency");
        jobCtrl.addJob(ctrlCalculateFrequency);
        Thread t = new Thread(jobCtrl);
        t.start();

        while (true) {
            if (jobCtrl.allFinished()) {
                System.out.println(jobCtrl.getSuccessfulJobList());
                jobCtrl.stop();
                break;
            }
        }

        //Create and run MAP-REDUCE job for generating mutual contact sets
        FileModifier.deleteDir(new File(PeerCatcherConfigure.ROOT_LOCATION + "/mutual_contact_sets"));
        Job jobGenerateMutualContactSets = Job.getInstance();
        jobGenerateMutualContactSets.setJobName("Job_Generate_Mutual_Contact_Sets_");
        jobGenerateMutualContactSets.setJarByClass(CalculateFrequency.class);
        jobGenerateMutualContactSets.setMapperClass(GenerateMutualContactSetsMapper.class);
        jobGenerateMutualContactSets.setReducerClass(GenerateMutualContactSetsReducer.class);
        jobGenerateMutualContactSets.setOutputKeyClass(Text.class);
        jobGenerateMutualContactSets.setOutputValueClass(Text.class);
        ControlledJob ctrlJobGenerateMutualContactSets = new ControlledJob(conf);
        ctrlJobGenerateMutualContactSets.setJob(jobGenerateMutualContactSets);
        FileInputFormat.addInputPath(jobGenerateMutualContactSets,
                new Path(PeerCatcherConfigure.ROOT_LOCATION + "/p2p_host_frequency"));
        FileOutputFormat.setOutputPath(jobGenerateMutualContactSets,
                new Path(PeerCatcherConfigure.ROOT_LOCATION + "/mutual_contact_sets"));
        ctrlJobGenerateMutualContactSets.addDependingJob(ctrlCalculateFrequency);   //Set job is only available after other job done.
        jobGenerateMutualContactSets.setNumReduceTasks(10);

        JobControl jobCtrl1 = new JobControl("map");
        jobCtrl1.addJob(ctrlJobGenerateMutualContactSets);
        t = new Thread(jobCtrl1);
        t.start();

        while (true) {

            if (jobCtrl1.allFinished()) {
                System.out.println(jobCtrl1.getSuccessfulJobList());
                jobCtrl1.stop();
                break;
            }
        }
    }
}
